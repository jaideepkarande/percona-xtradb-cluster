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

`on_view()` captures the totally-ordered membership under `m_mutex` — whether
the view is primary, this node's `own_index()`, and the index of the elected
replica: the **first member with a non-empty incoming address**. Members with an
empty incoming address (arbitrator / garbd) do not run `mysqld` and are skipped,
so the coordinator never elects an arbitrator. `compute_election()` then reads
that cached snapshot:

```cpp
// on_view() — runs in total order, only snapshots + signals the worker
m_view_primary       = (view.status() == wsrep::view::primary);
m_view_own_index     = view.own_index();
m_view_elected_index = -1;
for (size_t i = 0; i < view.members().size(); ++i)
  if (!view.members()[i].incoming().empty()) { m_view_elected_index = i; break; }

// compute_election() — m_mutex held by caller
if (m_view_seen)
  return m_view_primary && m_view_elected_index >= 0 &&
         m_view_own_index == m_view_elected_index;
// fallback until the first view arrives (startup / lone node):
// use the wsrep_cluster_status / wsrep_local_index globals, index 0.
```

Galera orders members identically on every node, so every node computes the same
`m_view_elected_index` without extra messaging. Until the first view is
delivered (startup, single node) the coordinator falls back to the
`wsrep_cluster_status` / `wsrep_cluster_size` / `wsrep_local_index` globals
maintained by `log_view()`.

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

## 5. Driving replication from the coordinator THD

The coordinator owns a single internal `THD` (`create_internal_thd()`,
`COM_DAEMON`, `wsrep_on=false` so its control statements never replicate). All
control statements run on that THD through an `Ed_connection`
(`execute_direct`), wrapped by the small helpers `acf_exec()` (statements),
`acf_query_scalar()` and `acf_query_rows()` (result sets) at the top of
`wsrep_async_failover.cc`:

* **Membership / liveness** is read from `performance_schema` tables directly.
* **`START REPLICA` / `STOP REPLICA`** (with an optional `FOR CHANNEL`) start and
  stop the managed channel. Whether the channel is currently running is read via
  `channel_is_active()` from `sql/rpl_channel_service_interface.h` (the only
  service-interface entry point the coordinator uses).
* **`SET GLOBAL super_read_only = ON/OFF`** toggles split-brain protection, so
  the normal `fix_super_read_only` path executes correctly rather than poking
  `opt_super_readonly` directly.

The SQL path was chosen over `channel_start()/channel_stop()` because it reuses
the exact, well-tested privilege/state validation that a DBA's manual
`START REPLICA` would hit, and keeps the coordinator's surface to a handful of
statements. Before starting a channel the coordinator verifies it is configured
**with `SOURCE_AUTO_POSITION=1`** (reads `HOST` and `AUTO_POSITION` from
`performance_schema.replication_connection_configuration`); a channel without
auto-positioning is refused and surfaced, never started (**FR-A4**).

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

Checks (FR-G2), evaluated in order; the first non-`OK` verdict wins. Every check
fails **open** on a query error (does not block), so a transient
`performance_schema` hiccup cannot wedge failover:

1. **Gap detection (FR-G2a).** Read `@@GLOBAL.gtid_executed` and scan each
   comma-separated per-UUID set. A UUID carrying more than one interval
   (e.g. `uuid:1-5:8-10`, i.e. ≥2 `:` separators) has a hole in the middle — a
   gap. A set that merely *starts* above 1 is **not** a gap (that is the normal
   result of `gtid_purged` after SST/log rotation). Implemented by
   `acf_gtid_has_gap()`.
2. **Xid ↔ GTID_NEXT agreement (FR-G2c), local half.** `XA RECOVER` is run; any
   in-doubt (prepared-but-not-committed) transaction means the node's Xid/GTID
   state is mid-flight, and (re)starting a DR replica against it risks the
   historical PXC GTID inconsistency. Verdict `XID_MISMATCH`. The *full
   cross-member* comparison (each member's last `Xid` → `GTID_NEXT` must agree)
   requires a cluster-wide rendezvous and is a **documented follow-up**; the
   local in-doubt check is the safe, self-contained portion shipped now.
3. **Errant transactions (FR-G2b).** Compute
   `errant = gtid_executed − received_via_channel − local_origin_set`
   using `GTID_SUBTRACT`. Any GTID originating from neither the managed source
   nor this node is errant. The received set is
   `performance_schema.replication_connection_status.RECEIVED_TRANSACTION_SET`
   for the managed channel; before the link has delivered anything the check is
   skipped (verdict `OK`). The **local-origin set is two UUIDs, not one**:
   `@@server_uuid` (which in PXC carries only the occasional non-wsrep
   housekeeping GTID) **and** the cluster-wide wsrep GTID UUID
   `wsrep_cluster_state_uuid`, under which Galera stamps every replicated write
   on this cluster. Subtracting only `server_uuid` mis-flags the cluster's own
   workload as errant as soon as the channel has delivered anything (the
   active-replica / circular case), so both are subtracted. There is **no
   `wsrep_gtid_mode` in 8.4**; the cluster GTID UUID is always distinct from
   every node's `server_uuid`. Regression-guarded by
   `galera.pxc_5201_circular`.

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

The ACF managed-group definition and the candidate source list both live on the
**replica** cluster, so source-list maintenance runs on the **receiver's elected
node** — `refresh_source_list()` is called from `process_receiver()` when the
node is elected, independent of the `SOURCE`/`RECEIVER`/`BOTH` mode flags. (A
cluster configured purely as `SOURCE` is the async primary and holds no local
ACF list, so it has no per-iteration source action.) This corrects an earlier
draft that gated the work on `SOURCE` mode, where the primary cluster — which has
no ACF tables — would have been the one running it.

`refresh_source_list(thd, channel)`:

1. Read the managed group rows for the channel from
   `performance_schema.replication_asynchronous_connection_failover_managed`
   where `MANAGED_TYPE='GaleraCluster'`. If none, return.
2. Build the **desired** candidate set:
   * every registered managed-group seed `(HOST, PORT)` is always desired;
   * in a co-located (`BOTH`) deployment, fold in the live membership published
     in `wsrep_incoming_addresses` (read from
     `performance_schema.global_status`), parsed into `(host, port)` pairs by
     `acf_parse_incoming_addresses()`. This marks the membership as *known*.
3. **Add** every desired candidate that is missing, via
   `asynchronous_connection_failover_add_source()` (idempotent).
4. **Prune** departed members via
   `asynchronous_connection_failover_delete_source()` — but **only when the live
   membership is known** (step 2, `BOTH`), so the coordinator never removes a
   candidate it could not observe.
5. The existing ACF IO thread / `Source_IO_monitor` performs the actual
   reconnect to a surviving candidate when the current source dies (**FR-B3**);
   with equal candidate weights it keeps the current connection while membership
   is unchanged, avoiding needless flapping (**FR-B5**).

### 8.1 Scope: cross-datacenter membership pull (follow-up)

Full add-**and**-prune reconciliation is delivered for the co-located (`BOTH`)
case, driven by the locally-published `wsrep_incoming_addresses`. For the
cross-datacenter case (primary=`SOURCE`, DR=`RECEIVER`), the DR node cannot read
the *primary* cluster's `wsrep_incoming_addresses` without reaching across the
WAN, so that path is currently **add-only** (it keeps every registered seed
present as a candidate and does not prune). The remaining piece — a short-lived,
timeout-bounded pull of `SHOW STATUS LIKE 'wsrep_incoming_addresses'` from the
currently-connected source over the replication credentials — is the documented
follow-up (tracked with the destructive two-cluster tests §2.5 of the testing
plan). The reconciliation engine (add/prune, `desired` vs `current`) is already
in place and is exercised by the `BOTH`-mode path, so the follow-up only has to
feed it the remote address list.

The `GaleraCluster` managed type is what tells PXC to use Galera membership
discovery instead of GR's `performance_schema.replication_group_members`.

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
