# PXC-5201 — Low Level Design

This document describes the concrete implementation of the Cluster-Aware
Asynchronous Replication Failover Coordinator. File paths are relative to the
PXC source root.

---

## 1. Component map

```
sql/wsrep_async_failover.h          new  — public API + state types
sql/wsrep_async_failover.cc         new  — coordinator, worker thread, election, GTID gate
sql/wsrep_var.{h,cc}                edit — system-variable check/update hooks
sql/sys_vars.cc                     edit — Sys_var_* registration (WITH_WSREP block)
sql/wsrep_mysqld.{h,cc}             edit — global var storage + status export
sql/wsrep_server_service.cc         edit — call coordinator from log_view()
sql/wsrep_sst.cc / wsrep_server_state.cc  (no change required)
sql/mysqld.cc                       edit — init/deinit coordinator, status vars, PSI keys
sql/rpl_async_conn_failover_add_managed_udf.cc  edit — accept "GaleraCluster"
storage/perfschema/table_wsrep_async_failover_status.{h,cc}  new — P_S table
storage/perfschema/pfs_engine_table.cc          edit — register P_S table
sql/CMakeLists.txt                  edit — add wsrep_async_failover.cc
storage/perfschema/CMakeLists.txt   edit — add the new P_S table source
share/messages_to_*.txt / errmsg    edit — new error/warning messages (optional)
```

The coordinator deliberately lives in the **`sql/wsrep_*` layer**, where PXC
already mediates between Galera and the server, and reuses the existing
replication service interfaces rather than touching `rpl_io_monitor.cc` /
`rpl_replica.cc`.

---

## 2. System variables

Defined as globals in `sql/wsrep_mysqld.cc`, declared in `sql/wsrep_mysqld.h`,
registered in `sql/sys_vars.cc` inside the `#ifdef WITH_WSREP` block, with
check/update callbacks in `sql/wsrep_var.cc` (declared in `sql/wsrep_var.h`).

```cpp
// wsrep_mysqld.h
extern bool          wsrep_async_failover;            // master switch
extern ulong         wsrep_async_failover_mode;       // enum OFF/RECEIVER/SOURCE/BOTH
extern char*         wsrep_async_failover_channel;     // managed channel name
extern ulong         wsrep_async_failover_gtid_check; // enum OFF/WARN/ENFORCE
extern bool          wsrep_async_failover_read_only;   // manage super_read_only
extern uint          wsrep_async_failover_check_interval; // seconds
```

`enum_wsrep_acf_mode  { ACF_OFF, ACF_RECEIVER, ACF_SOURCE, ACF_BOTH }`
`enum_wsrep_acf_gtid  { GTID_CHECK_OFF, GTID_CHECK_WARN, GTID_CHECK_ENFORCE }`

Registration pattern (mirrors `Sys_wsrep_desync` / `Sys_wsrep_cluster_address`):

```cpp
static Sys_var_bool Sys_wsrep_async_failover(
    "wsrep_async_failover",
    "Enable PXC cluster-aware asynchronous replication failover coordinator.",
    GLOBAL_VAR(wsrep_async_failover), CMD_LINE(OPT_ARG), DEFAULT(false),
    NO_MUTEX_GUARD, NOT_IN_BINLOG, ON_CHECK(nullptr),
    ON_UPDATE(wsrep_async_failover_update));
```

`wsrep_async_failover_update()` calls
`Wsrep_async_failover::instance().refresh_enabled()` which starts or stops the
worker thread. The other variables' `ON_UPDATE` callbacks simply signal the
worker's condition variable so changes take effect on the next iteration.

---

## 3. Coordinator object & worker thread

```cpp
// wsrep_async_failover.h
class Wsrep_async_failover {
 public:
  static Wsrep_async_failover &instance();

  void init();                 // called once from mysqld init
  void deinit();               // called once from mysqld shutdown (joins thread)

  // Called from Wsrep_server_service::log_view() in total order.
  // Lightweight: snapshots the view and signals the worker.
  void on_view(const wsrep::view &view);

  // Called from sys-var ON_UPDATE hooks.
  void refresh_enabled();      // start/stop worker to match wsrep_async_failover
  void wakeup();               // signal the worker condition variable

  // Observability snapshot for the P_S table & status vars.
  struct Status {
    bool        enabled;
    const char *mode;          // OFF/RECEIVER/SOURCE/BOTH
    const char *channel;
    bool        is_active_replica;
    long        elected_index;  // own_index of elected replica, -1 if none
    const char *gtid_verdict;   // OK / GAP / ERRANT / XID_MISMATCH / SKIPPED
    const char *last_action;    // human readable
    ulonglong   last_action_time;
  };
  Status snapshot();

 private:
  void run();                  // worker loop
  void process_receiver(const wsrep::view &view);
  void process_source(const wsrep::view &view);
  bool elect_is_self(const wsrep::view &view) const;  // own_index == 0
  // ...
  mysql_mutex_t m_mutex;
  mysql_cond_t  m_cond;
  my_thread_handle m_thread;
  bool m_thread_running{false};
  bool m_abort{false};
  bool m_view_pending{false};
  wsrep::view m_last_view;     // copy under mutex
  Status m_status;             // protected by m_mutex
};
```

* The view callback (`on_view`) **only** copies the view and sets
  `m_view_pending`, then signals `m_cond` — satisfying **NFR-1** (no SQL in
  total order).
* `run()` loops: wait on `m_cond` with a timeout of
  `wsrep_async_failover_check_interval` seconds, then, if enabled and in the
  primary component, call `process_receiver()` and/or `process_source()`
  according to `wsrep_async_failover_mode`.
* A dedicated `THD` is created for SQL execution (see §5), in the style of
  `wsrep_create_rollbacker()` / GR's internal threads.
* PSI thread key `key_THREAD_wsrep_async_failover` is registered in
  `sql/mysqld.cc` so the thread shows up in `performance_schema.threads`.

---

## 4. Election (Scenario A)

The current Galera view is the input. `wsrep::view` provides:

* `members()` — ordered vector of `{id (UUID), name, incoming (host:port)}`.
* `own_index()` — this node's position, mirrored to `wsrep_local_index`.
* `status()` — `primary` / `non_primary` / `disconnected`.

Election rule (**FR-A2**, deterministic, no extra messaging):

```cpp
bool Wsrep_async_failover::elect_is_self(const wsrep::view &v) const {
  if (v.status() != wsrep::view::primary) return false;
  if (v.members().empty()) return false;
  // Candidate = member with the smallest stable ordering key.
  // Galera orders members identically on every node, so index 0 is a
  // cluster-wide agreed choice. Members with empty incoming address
  // (e.g. arbitrator/garbd) are skipped.
  size_t elected = SIZE_MAX;
  for (size_t i = 0; i < v.members().size(); ++i)
    if (!v.members()[i].incoming().empty()) { elected = i; break; }
  return elected != SIZE_MAX && elected == (size_t)v.own_index();
}
```

`process_receiver()`:

1. If not primary component → return (FR-C5).
2. Compute `self_elected = elect_is_self(view)`.
3. Manage `super_read_only` (see §6) when `wsrep_async_failover_read_only`.
4. If `self_elected`:
   * run the GTID gate (§7). If it fails in `ENFORCE` → log, set verdict, **do
     not** start; retry next iteration.
   * if the managed channel is not running → `START REPLICA` via the channel
     service interface (§5).
   * update status `is_active_replica=true`.
5. Else (not elected):
   * if the managed channel is running locally → `STOP REPLICA` (FR-A5/FR-A6).
   * update status `is_active_replica=false`.

Because election only depends on the totally ordered view, when the elected
node disappears the very next view gives a new index-0 member, which then runs
step 4. No leader lease / heartbeat is needed beyond Galera's own membership.

---

## 5. Driving replication without a SQL client

Start/stop of the channel uses the in-server **replication channel service
interface** (`sql/rpl_channel_service_interface.h`), the same API Group
Replication uses:

```cpp
#include "sql/rpl_channel_service_interface.h"

Channel_connection_info info;
initialize_channel_connection_info(&info);
// start only the threads we need; AUTO_POSITION already on the channel
int err = channel_start(channel, &info,
                        CHANNEL_RECEIVER_THREAD | CHANNEL_APPLIER_THREAD,
                        /*wait_for_connection=*/true);
...
channel_stop(channel, CHANNEL_RECEIVER_THREAD | CHANNEL_APPLIER_THREAD,
             /*timeout=*/...);
bool running = channel_is_active(channel, CHANNEL_NO_THD);
```

These functions create/attach their own THD internally and are safe to call
from the coordinator worker thread.

`super_read_only` is toggled through a small internal helper that runs
`SET GLOBAL super_read_only = ON/OFF` on the coordinator's THD (mirroring GR's
`Set_system_variable`), so the normal `fix_super_read_only` path (global read
lock semantics) executes correctly rather than poking `opt_super_readonly`
directly.

---

## 6. super_read_only management (Scenario split-brain protection)

On a RECEIVER/BOTH node with `wsrep_async_failover_read_only=ON`:

* Every coordinator iteration ensures `super_read_only=ON`. The coordinator
  records that *it* set the bit (`m_set_super_read_only=true`).
* Replication appliers are unaffected by `super_read_only`, so the elected node
  still applies the relay log (FR-S2).
* If the operator turns `wsrep_async_failover_read_only=OFF`, the coordinator
  clears only the bit it set (FR-S3); it never clears a bit set independently.

This matches InnoDB-Cluster semantics: the whole DR cluster is read-only to
applications, removing any window where traffic mistakenly directed at DR could
diverge from the primary.

---

## 7. GTID consistency gate

Implemented in `gtid_gate()` and evaluated before every `START REPLICA` on the
elected node. Policy from `wsrep_async_failover_gtid_check`.

Checks (FR-G2):

1. **Gap detection.** Read `@@GLOBAL.gtid_executed`; for each UUID interval set,
   detect missing intervals relative to `gtid_purged` continuity. A
   discontinuity within the cluster's own UUID set is a gap.
2. **Errant transactions.** Compute
   `errant = gtid_executed − (cluster_uuid_set ∪ received_via_channel)`.
   Any GTID originating from neither this cluster nor the managed source is
   errant. The received set is taken from
   `performance_schema.replication_connection_status.RECEIVED_TRANSACTION_SET`
   for the managed channel.
3. **Xid ↔ GTID_NEXT agreement.** Validate that the last committed InnoDB `Xid`
   maps to the expected `GTID_NEXT` (the historical PXC inconsistency class).
   In practice this is checked by comparing the binlog/engine recovery GTID with
   `gtid_executed` at startup; the gate re-verifies it has not regressed.

Verdicts: `OK`, `GAP`, `ERRANT`, `XID_MISMATCH`, `SKIPPED`.

* `ENFORCE`: non-`OK` verdict ⇒ do not start/relocate; log
  `[wsrep-acf] GTID consistency check failed (<verdict>); replica not started`;
  retry on next iteration (so a transient state self-heals once Galera catches
  up).
* `WARN`: log the warning, proceed.
* `OFF`: verdict `SKIPPED`, proceed.

The verdict is stored in `m_status.gtid_verdict` and surfaced via the status
variable `wsrep_async_failover_gtid_consistent` and the P_S table.

The gate uses lightweight SQL (`SELECT @@GLOBAL.gtid_executed`,
`SELECT GTID_SUBTRACT(...)`, a read of
`performance_schema.replication_connection_status`) executed on the
coordinator's THD; the heavy lifting reuses server GTID-set primitives
(`Gtid_set`, `gtid_state`) where a direct in-process path is cheaper than SQL.

---

## 8. Source-side auto-population (Scenario B)

Active on the elected replica node when `wsrep_async_failover_mode` is
`SOURCE`/`BOTH` on the *primary* cluster and `RECEIVER`/`BOTH` on the DR side
(typically the symmetric case is configured per cluster: primary=SOURCE,
DR=RECEIVER, and the *DR* coordinator does the population).

`process_source()` / the receiver's source-refresh step:

1. Read the managed group rows for the channel from
   `performance_schema.replication_asynchronous_connection_failover_managed`
   where `Managed_type='GaleraCluster'`.
2. For the managed primary cluster, obtain the live healthy membership. Two
   supported discovery paths:
   * **Pull**: query the currently-connected source for
     `SHOW STATUS LIKE 'wsrep_incoming_addresses'` (a comma-separated list of
     `host:port` of all synced members) over the existing replication
     credentials. This requires no schema on the source beyond standard PXC.
   * The list is parsed into `(host, port)` candidates.
3. Reconcile against
   `performance_schema.replication_asynchronous_connection_failover` for that
   `(channel, managed_name)`:
   * add rows for new members
     (`Rpl_async_conn_failover_table_operations::add_source_skip_send`),
   * delete rows for departed members
     (`...::delete_source`),
   * keep the currently-connected source at the highest weight so a stable
     membership does not cause connection flapping (**FR-B5**).
4. The existing ACF IO thread / `Source_IO_monitor` performs the actual
   reconnect to a surviving candidate when the current source dies (**FR-B3**).

This keeps PXC-5201's new code confined to the coordinator while the proven ACF
reconnect path does the mechanics. The `GaleraCluster` managed type is what
tells PXC to use Galera membership discovery instead of GR's
`performance_schema.replication_group_members`.

### 8.1 UDF change

`sql/rpl_async_conn_failover_add_managed_udf.cc` currently rejects any
`Managed_type` other than the 16-char string `"GroupReplication"`. The check is
relaxed to also accept the 13-char `"GaleraCluster"`:

```cpp
const bool is_gr     = (len == 16 && !strcmp(t, "GroupReplication"));
const bool is_galera = (len == 13 && !strcmp(t, "GaleraCluster"));
if (!is_gr && !is_galera) {
  my_stpcpy(message,
            "Wrong value: Managed type must be GroupReplication or GaleraCluster.");
  return true;
}
```

No storage change is needed — `Managed_type` is already a free-text column in
`mysql.replication_asynchronous_connection_failover_managed`.

---

## 9. Hook into the view-change path

`Wsrep_server_service::log_view()` (`sql/wsrep_server_service.cc`) already runs
on every Galera view and updates `wsrep_cluster_size`, `wsrep_local_index`, …
A single call is appended near the end:

```cpp
#include "sql/wsrep_async_failover.h"
...
void Wsrep_server_service::log_view(...) {
  ...
  // existing status updates ...
  Wsrep_async_failover::instance().on_view(view);
}
```

`on_view` is intentionally trivial (copy + signal); this keeps total-order
processing fast.

---

## 10. Performance Schema table

New read-only table `performance_schema.wsrep_async_failover_status`, following
the exact pattern of `table_replication_asynchronous_connection_failover.{h,cc}`.

| Column | Type | Meaning |
|--------|------|---------|
| `ENABLED` | `ENUM('YES','NO')` | `wsrep_async_failover` on this node |
| `MODE` | `VARCHAR(16)` | OFF / RECEIVER / SOURCE / BOTH |
| `CHANNEL_NAME` | `VARCHAR(64)` | managed channel |
| `IS_ACTIVE_REPLICA` | `ENUM('YES','NO')` | is this node the elected active replica |
| `ELECTED_INDEX` | `BIGINT` | own_index of elected node (-1 = none) |
| `CLUSTER_SIZE` | `BIGINT` | members in current primary view |
| `GTID_CONSISTENCY` | `VARCHAR(16)` | OK / GAP / ERRANT / XID_MISMATCH / SKIPPED |
| `SUPER_READ_ONLY_MANAGED` | `ENUM('YES','NO')` | coordinator is holding super_read_only |
| `LAST_ACTION` | `VARCHAR(512)` | last decision (human readable) |
| `LAST_ACTION_TIMESTAMP` | `TIMESTAMP(6)` | when |

Implementation:

* `table_wsrep_async_failover_status.h/.cc` defines `m_share`, the field list,
  `rnd_init/rnd_next/read_row_values`. It produces a **single row** populated
  from `Wsrep_async_failover::instance().snapshot()`.
* Registered in `storage/perfschema/pfs_engine_table.cc` (the
  `all_shares[]` array) next to the other replication tables.
* Added to `storage/perfschema/CMakeLists.txt`.

### 10.1 Affected enumeration result files

Adding a P_S table changes the canonical table list. The following result files
list every P_S table / its columns and **MUST** be re-recorded
(`mysql-test-run.pl --record`):

```
suite/perfschema/r/all_tests.result
suite/perfschema/r/table_schema.result
suite/perfschema/r/schema.result
suite/perfschema/r/information_schema.result
suite/perfschema/r/dml_handler.result
r/1st.result (if it enumerates P_S)
```

These are deterministic, additive diffs (one new table row / column block).

---

## 11. Status variables

Exported through the existing `SHOW STATUS` mechanism in `sql/mysqld.cc`
(`status_vars[]`) using `SHOW_FUNC` accessors that read
`Wsrep_async_failover::instance().snapshot()`:

* `wsrep_async_failover_role` — string OFF/RECEIVER/SOURCE/BOTH
* `wsrep_async_failover_is_active_replica` — ON/OFF
* `wsrep_async_failover_gtid_consistent` — OK/… verdict

(These are *server* status vars, separate from the provider's `wsrep_*` stats.)

---

## 12. Concurrency, locking, and shutdown

* `m_mutex` guards the pending-view flag, the cached view, and the status
  snapshot. The worker copies what it needs under the lock, then releases it
  before doing SQL.
* The view callback never blocks on SQL; worst case it waits briefly for
  `m_mutex`.
* `deinit()` sets `m_abort`, signals `m_cond`, and `my_thread_join`s the worker.
  Called from the wsrep shutdown sequence in `mysqld.cc` before the provider is
  unloaded.
* All SQL on the worker THD checks `thd->killed` / server-shutdown so a hung
  source connection cannot block shutdown beyond the connect timeout.

---

## 13. Error / log messages

Stable prefix `[wsrep-acf]`. Representative messages:

* `Elected this node (index %d) as active async replica for channel '%s'.`
* `Yielding active async replica role for channel '%s' to cluster index %d.`
* `GTID consistency check failed (%s) for channel '%s'; replica not started.`
* `Refreshed managed source list for channel '%s': %d candidate(s).`
* `Enabled super_read_only on DR cluster (wsrep_async_failover).`

New entries are added to `share/messages_to_error_log.txt` /
`messages_to_clients.txt` where a numbered error is appropriate; informational
lines use `WSREP_INFO/WSREP_WARN`.

---

## 14. Sequence diagrams

### 14.1 Receiver failover

```
DR-N1 (active)   DR-N2            DR-N3            Galera
   x  crash
                  ◀── view: members=[N2,N3], own_index differs ──▶
   (gone)        on_view→worker   on_view→worker
                 elect: index0=N2 elect: index0=N2
                 self_elected=Y   self_elected=N
                 GTID gate OK      STOP REPLICA (noop)
                 super_read_only=ON
                 START REPLICA ───────────────────────────▶ primary source
                 status: IS_ACTIVE_REPLICA=YES
```

### 14.2 Source failover

```
DR active replica            Primary cluster (D1)
  process_source():            N1(connected) N2 N3
    SHOW wsrep_incoming_addresses on source ─▶ "N1,N2,N3"
    reconcile ACF source list = {N1*,N2,N3}  (N1 highest weight)
  ... D1-N1 crashes ...
    ACF IO thread detects connection loss
    do_auto_conn_failover() picks next candidate (N2)
    reconnect to N2 (AUTO_POSITION) ─────────▶ resumes
  next process_source():
    SHOW wsrep_incoming_addresses ─▶ "N2,N3"
    reconcile ACF source list = {N2*,N3}  (drop N1)
```

---

## 15. Build integration

* `sql/CMakeLists.txt`: add `wsrep_async_failover.cc` to the `SQL_SHARED`/server
  sources (inside the `WITH_WSREP` conditional already used for other
  `wsrep_*.cc`).
* `storage/perfschema/CMakeLists.txt`: add
  `table_wsrep_async_failover_status.cc`.
* No new third-party dependency.
* Code compiles under the project's `-Werror` profile (no unused params, marked
  `override`, etc.).
