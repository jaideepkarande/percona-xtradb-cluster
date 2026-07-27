# WL#PXC-5201: Native Automatic Asynchronous Replication Failover — Low Level Design

**Affects:** Percona XtraDB Cluster 8.0 / 8.4 — **Status:** Complete

| [Description](WL_PXC-5201_Description.md) | [Requirements](WL_PXC-5201_FR.md) | Dependent Tasks | [High Level Architecture](WL_PXC-5201_HLD.md) | **Low Level Design** | [Compatibility & Bugs](WL_PXC-5201_Compatibility_and_Bugs.md) |
|---|---|---|---|---|---|

File paths are relative to the PXC source root.

---

## 1. COMPONENT MAP

```
sql/wsrep_async_failover.h          new  — public API, Status snapshot, mode/GTID enums
sql/wsrep_async_failover.cc         new  — coordinator, worker thread, election, GTID gate,
                                           super_read_only, receiver/source processing
sql/wsrep_var.{h,cc}                edit — sys-var ON_UPDATE hooks (start/stop, wakeup)
sql/sys_vars.cc                     edit — Sys_var_* registration (WITH_WSREP block)
sql/wsrep_mysqld.{h,cc}             edit — global var storage + init()/deinit() wiring
sql/wsrep_server_service.cc         edit — call on_view() from log_view()
sql/mysqld.cc                       edit — register key_THREAD_wsrep_async_failover PSI key
sql/rpl_async_conn_failover_add_managed_udf.cc      edit — accept "GaleraCluster"
sql/rpl_async_conn_failover_delete_managed_udf.cc   edit — drop UUID-only managed-name check
storage/perfschema/table_wsrep_async_failover_status.{h,cc}  new — P_S table
storage/perfschema/pfs_engine_table.cc              edit — register the P_S table share
sql/CMakeLists.txt                                  edit — add wsrep_async_failover.cc
storage/perfschema/CMakeLists.txt                   edit — add the P_S table source
```

The coordinator lives in the `sql/wsrep_*` layer, where PXC already mediates
between Galera and the server, and reuses the existing replication service
interfaces rather than touching `rpl_io_monitor.cc` / `rpl_replica.cc`. **No file
under `percona-xtradb-cluster-galera/`, `wsrep-lib/`, or the wsrep-API submodule
is modified** (see [Compatibility & Bugs](WL_PXC-5201_Compatibility_and_Bugs.md)).

---

## 2. SYSTEM VARIABLES

Globals defined in `sql/wsrep_mysqld.cc`, declared in `sql/wsrep_mysqld.h`,
registered in `sql/sys_vars.cc` inside `#ifdef WITH_WSREP`, with update callbacks
in `sql/wsrep_var.cc`.

```cpp
extern bool  wsrep_async_failover;             // master switch
extern ulong wsrep_async_failover_mode;        // enum OFF/RECEIVER/SOURCE/BOTH
extern char *wsrep_async_failover_channel;     // managed channel name
extern ulong wsrep_async_failover_gtid_check;  // enum OFF/WARN/ENFORCE
extern bool  wsrep_async_failover_read_only;   // manage super_read_only
extern uint  wsrep_async_failover_check_interval; // seconds (1..3600)

enum enum_wsrep_acf_mode { ACF_OFF, ACF_RECEIVER, ACF_SOURCE, ACF_BOTH };
enum enum_wsrep_acf_gtid { GTID_CHECK_OFF, GTID_CHECK_WARN, GTID_CHECK_ENFORCE };
```

* `wsrep_async_failover_update()` → `refresh_enabled()` starts/stops the worker.
* `wsrep_async_failover_wakeup_update()` → `wakeup()` signals the worker so other
  changes take effect promptly without waiting for the timer.

---

## 3. COORDINATOR OBJECT & WORKER THREAD

```cpp
class Wsrep_async_failover {
 public:
  static Wsrep_async_failover &instance();
  void init();                 // once, from wsrep_init_globals()
  void deinit();               // once, from wsrep_deinit_server() — joins the thread
  void on_view(const wsrep::view &view);  // total-order: snapshot + signal only
  void refresh_enabled();      // start/stop worker to match wsrep_async_failover
  void wakeup();               // signal m_cond
  struct Status { /* enabled, mode, channel, is_active_replica, elected_index,
                     cluster_size, gtid_verdict, super_read_only_managed,
                     last_action, last_action_time */ };
  Status snapshot();
 private:
  void run();                  // worker loop
  void process_receiver();     // Scenario A + super_read_only + source-list refresh
  void process_source();       // (see §8 — receiver's elected node owns the list)
  bool compute_election();     // own_index == first-eligible index
  // m_mutex, m_cond, m_thread, m_abort, cached view snapshot, m_status ...
};
```

* `on_view()` runs in total order and **only** copies the view snapshot and
  signals `m_cond` (NFR1).
* `run()` waits on `m_cond` with a timeout of `wsrep_async_failover_check_interval`
  seconds, then — if enabled and in a primary component — runs
  `process_receiver()` and/or `process_source()` per `wsrep_async_failover_mode`.
* PSI key `key_THREAD_wsrep_async_failover` is registered in `sql/mysqld.cc` so the
  worker appears in `performance_schema.threads`.

---

## 4. ELECTION (Scenario A)

`on_view()` captures, under `m_mutex`, the totally-ordered membership:

```cpp
// on_view() — total order, snapshot + signal only
m_view_primary       = (view.status() == wsrep::view::primary);
m_view_own_index     = view.own_index();
m_view_elected_index = -1;
for (size_t i = 0; i < view.members().size(); ++i)
  if (!view.members()[i].incoming().empty()) { m_view_elected_index = i; break; }

// compute_election() — m_mutex held by caller
if (m_view_seen)
  return m_view_primary && m_view_elected_index >= 0 &&
         m_view_own_index == m_view_elected_index;
// fallback until first view: wsrep_cluster_status/size + wsrep_local_index==0
```

Members with an empty incoming address (arbitrator / `garbd`) do not run `mysqld`
and are skipped, so the coordinator never elects an arbitrator. Galera orders
members identically on every node, so all nodes compute the same
`m_view_elected_index` with no extra messaging (FR2, NFR2).

`process_receiver()`:

1. Not in primary component → return (FR24).
2. `self_elected = compute_election()`.
3. Manage `super_read_only` (§6) when `wsrep_async_failover_read_only`.
4. If `self_elected`:
   * run the GTID gate (§7); on failure in `ENFORCE` → log, set verdict, do **not**
     start, retry next iteration;
   * if the managed channel is not running and is configured with a source and
     `SOURCE_AUTO_POSITION=1` → `START REPLICA` (§5);
   * refresh the ACF candidate source list (§8);
   * status `IS_ACTIVE_REPLICA=YES`.
5. Else (not elected):
   * if the managed channel is running locally → `STOP REPLICA` (FR5/FR6);
   * status `IS_ACTIVE_REPLICA=NO`.

---

## 5. DRIVING REPLICATION FROM THE COORDINATOR THD

The coordinator owns one internal THD (`create_internal_thd()`, `COM_DAEMON`,
`wsrep_on=false` so its control statements never replicate). All control
statements run on that THD through an `Ed_connection` (`execute_direct`), wrapped
by helpers `acf_exec()` (statements), `acf_query_scalar()` and `acf_query_rows()`
(result sets):

* **Membership / liveness / config** are read from `performance_schema` tables.
* **`START REPLICA` / `STOP REPLICA`** (optional `FOR CHANNEL`) start/stop the
  managed channel; `channel_is_active()`
  (`sql/rpl_channel_service_interface.h`) reports whether it is running.
* **`SET GLOBAL super_read_only = ON/OFF`** toggles split-brain protection via the
  normal `fix_super_read_only` path.

Before starting a channel the coordinator verifies it is configured with a source
**and** `SOURCE_AUTO_POSITION=1` (reads `HOST`/`AUTO_POSITION` from
`replication_connection_configuration`); a channel without auto-positioning is
refused and surfaced, never started (FR4). This also avoids noisy "server is not
configured as replica" errors when the feature is enabled before the channel is
provisioned.

The SQL path was chosen over `channel_start()/channel_stop()` so the coordinator
reuses the exact privilege/state validation a DBA's manual `START REPLICA` hits,
keeping the surface to a handful of statements.

---

## 6. super_read_only MANAGEMENT

On a RECEIVER/BOTH node with `wsrep_async_failover_read_only=ON`:

* Each iteration ensures `super_read_only=ON` and records that the coordinator set
  the bit (`m_set_super_read_only=true`).
* Replication appliers bypass `super_read_only`, so the elected node still applies
  the relay log (FR18).
* If the operator sets `wsrep_async_failover_read_only=OFF`, the coordinator clears
  only the bit it set (FR19); a DBA-set `super_read_only` is untouched.
* Disabling the whole feature (`wsrep_async_failover=OFF`) stops the thread and
  intentionally leaves `super_read_only` in place — an operator clears it.

This mirrors InnoDB-Cluster semantics: the whole DR cluster is read-only to
applications, removing any window where traffic mistakenly directed at DR could
diverge.

---

## 7. GTID CONSISTENCY GATE

Implemented in `gtid_gate()`, evaluated before every `START REPLICA` on the
elected node. Policy from `wsrep_async_failover_gtid_check`. Checks are evaluated
in order; the first non-`OK` verdict wins. Every check **fails open** on a query
error (does not block) so a transient `performance_schema` hiccup cannot wedge
failover (FR handling for §8.6 of the HLD).

**7.1. Gap detection (FR13a).** Read `@@GLOBAL.gtid_executed` and scan each
per-UUID set. A UUID carrying more than one interval (e.g. `uuid:1-5:8-10`, ≥2
`:` separators) has a hole → gap. A set that merely *starts* above 1 is **not** a
gap (normal after `gtid_purged` / SST / log rotation). Implemented by
`acf_gtid_has_gap()`. Verdict `GAP`.

**7.2. Xid ↔ GTID_NEXT agreement (FR13c), local half.** `XA RECOVER` is run; any
in-doubt (prepared-but-not-committed) transaction means the node's Xid/GTID state
is mid-flight, and (re)starting a DR replica against it risks the historical PXC
GTID inconsistency. Verdict `XID_MISMATCH`. The *full cross-member* comparison
(each member's last `Xid` → `GTID_NEXT` must agree) requires a cluster-wide
rendezvous and is a documented follow-up; the local in-doubt check is the safe,
self-contained portion shipped now.

**7.3. Errant transactions (FR13b, FR16).**
```
errant = gtid_executed − received_via_channel − local_origin_set
```
computed with `GTID_SUBTRACT`. `received_via_channel` is
`replication_connection_status.RECEIVED_TRANSACTION_SET` for the managed channel;
before the link has delivered anything the check is skipped (verdict `OK`). The
**local-origin set is two UUIDs, not one**: `@@server_uuid` (which in PXC carries
only occasional non-wsrep housekeeping GTIDs) **and** the cluster-wide wsrep GTID
UUID `wsrep_cluster_state_uuid`, under which Galera stamps every replicated write.
Subtracting only `server_uuid` mis-flags the cluster's own workload as errant as
soon as the channel has delivered anything (the circular / active-replica case),
so both are subtracted (**bug fixed during development — see
[Compatibility & Bugs §5.1](WL_PXC-5201_Compatibility_and_Bugs.md)**;
regression-guarded by `galera.pxc_5201_circular`). There is no `wsrep_gtid_mode`
in 8.4; the cluster GTID UUID is always distinct from every node's `server_uuid`.

Verdicts: `OK`, `GAP`, `ERRANT`, `XID_MISMATCH`, `SKIPPED`.

* `ENFORCE`: non-`OK` ⇒ do not start; log `[wsrep-acf] GTID consistency check
  failed (<verdict>); replica not started`; retry next iteration.
* `WARN`: log the warning, proceed.
* `OFF`: verdict `SKIPPED`, proceed.

The verdict is stored in `m_status.gtid_verdict` and surfaced via
`wsrep_async_failover_gtid_consistent` and the P_S table.

---

## 8. SOURCE-SIDE AUTO-POPULATION (Scenario B)

The ACF managed-group definition and the candidate list both live on the
**replica** cluster, so source-list maintenance runs on the **receiver's elected
node** — `refresh_source_list()` is called from `process_receiver()` when the
node is elected, independent of the mode flags. (A cluster configured purely as
`SOURCE` is the async primary and holds no local ACF list, so it has no
per-iteration source action. This corrects an earlier draft that gated the work on
`SOURCE` mode — see [Compatibility & Bugs §5.4](WL_PXC-5201_Compatibility_and_Bugs.md).)

`refresh_source_list(thd, channel)`:

1. Read managed-group rows for the channel from
   `replication_asynchronous_connection_failover_managed` where
   `MANAGED_TYPE='GaleraCluster'`. If none, return.
2. Build the **desired** candidate set:
   * every registered managed-group seed `(HOST, PORT)` is always desired;
   * in a co-located (`BOTH`) deployment, fold in the live membership published in
     `wsrep_incoming_addresses` (read from `global_status`), parsed by
     `acf_parse_incoming_addresses()`. This marks membership as *known*.
3. **Add** every missing desired candidate via
   `asynchronous_connection_failover_add_source()` (idempotent).
4. **Prune** departed members via
   `asynchronous_connection_failover_delete_source()` — but **only when live
   membership is known** (step 2), so the coordinator never removes a candidate it
   could not observe.
5. The stock ACF IO thread / `Source_IO_monitor` performs the actual reconnect to a
   surviving candidate when the current source dies (FR9); with equal weights it
   keeps the current connection while membership is unchanged (FR11).

### 8.1. Scope: cross-datacenter membership pull (follow-up)

Full add-**and**-prune reconciliation is delivered for the co-located (`BOTH`)
case using the locally-published `wsrep_incoming_addresses`. For the
cross-datacenter case (primary=`SOURCE`, DR=`RECEIVER`), the DR node cannot read
the *primary's* `wsrep_incoming_addresses` without reaching across the WAN, so
that path is currently **add-only** (keeps every seed present, does not prune).
The remaining piece — a short-lived, timeout-bounded pull of `SHOW STATUS LIKE
'wsrep_incoming_addresses'` from the connected source over the replication
credentials — is the documented follow-up, tracked with the destructive
two-cluster tests. The reconciliation engine (add/prune, desired vs current) is
already in place and exercised by the `BOTH`-mode path.

### 8.2. UDF changes

`sql/rpl_async_conn_failover_add_managed_udf.cc` previously rejected any
`Managed_type` other than the 16-char `"GroupReplication"`, and enforced a
UUID-format check on the managed name. It is relaxed to also accept the 13-char
`"GaleraCluster"`, and the UUID check is applied **only** for `GroupReplication`:

```cpp
const bool is_gr     = (len == 16 && !strcmp(t, "GroupReplication"));
const bool is_galera = (len == 13 && !strcmp(t, "GaleraCluster"));
if (!is_gr && !is_galera) { /* "Managed type must be GroupReplication or GaleraCluster." */ }
// UUID validity check on managed_name is now gated on is_gr only.
```

`sql/rpl_async_conn_failover_delete_managed_udf.cc` drops its unconditional
UUID-only managed-name validity check (that UDF carries no managed_type argument),
so a `GaleraCluster` label such as `d1` can be deleted. Row lookup keyed by
`channel + managed name` decides whether a row exists. No storage change is needed
— `Managed_type` is already a free-text column.

---

## 9. HOOK INTO THE VIEW-CHANGE PATH

`Wsrep_server_service::log_view()` (`sql/wsrep_server_service.cc`) already runs on
every Galera view and updates `wsrep_cluster_size`, `wsrep_local_index`, … A
single call is appended near the end:

```cpp
#include "sql/wsrep_async_failover.h"
...
void Wsrep_server_service::log_view(...) {
  ... // existing status updates
  Wsrep_async_failover::instance().on_view(view);
}
```

`on_view` is intentionally trivial (copy + signal) to keep total-order processing
fast.

---

## 10. PERFORMANCE SCHEMA TABLE

New read-only, in-memory table `performance_schema.wsrep_async_failover_status`,
following `table_replication_asynchronous_connection_failover.{h,cc}`.

| Column | Type | Meaning |
|--------|------|---------|
| `ENABLED` | `ENUM('YES','NO')` | `wsrep_async_failover` on this node |
| `MODE` | `VARCHAR(16)` | OFF / RECEIVER / SOURCE / BOTH |
| `CHANNEL_NAME` | `VARCHAR(64)` | managed channel |
| `IS_ACTIVE_REPLICA` | `ENUM('YES','NO')` | is this node the elected active replica |
| `ELECTED_INDEX` | `BIGINT` | own_index of elected node (-1 = none) |
| `CLUSTER_SIZE` | `BIGINT` | members in current primary view |
| `GTID_CONSISTENCY` | `VARCHAR(16)` | OK / GAP / ERRANT / XID_MISMATCH / SKIPPED |
| `SUPER_READ_ONLY_MANAGED` | `ENUM('YES','NO')` | coordinator holds super_read_only |
| `LAST_ACTION` | `VARCHAR(512)` | last decision (human readable) |
| `LAST_ACTION_TIMESTAMP` | `TIMESTAMP(6)` | when (nullable) |

* `table_wsrep_async_failover_status.h/.cc` defines `m_share`, the field list, and
  `rnd_init/rnd_next/read_row_values`. It produces a **single row** from
  `Wsrep_async_failover::instance().snapshot()`.
* Registered in `storage/perfschema/pfs_engine_table.cc` (`all_shares[]`) and
  added to `storage/perfschema/CMakeLists.txt`.

### 10.1. Affected enumeration result files

Adding a P_S table changes the canonical table list; these result files are
re-recorded (`mysql-test-run.pl --record`), all additive diffs (one new table /
its column block):

```
suite/perfschema/r/{all_tests,table_schema,schema,information_schema,dml_handler}.result
```

---

## 11. CONCURRENCY, LOCKING & SHUTDOWN

* `m_mutex` guards the pending-view flag, the cached view snapshot, and the status
  snapshot. The worker copies what it needs under the lock, then releases it before
  doing SQL.
* The view callback never blocks on SQL; worst case it waits briefly for `m_mutex`.
* `deinit()` sets `m_abort`, signals `m_cond`, and `my_thread_join`s the worker,
  called from the wsrep shutdown sequence before the provider is unloaded (NFR7).
* All SQL on the worker THD checks `thd->killed` / server-shutdown so a hung source
  connection cannot block shutdown beyond the connect timeout.

---

## 12. SEQUENCE DIAGRAMS

### 12.1. Receiver failover (Scenario A)

```
DR-N1 (active)   DR-N2            DR-N3            Galera
   x crash
                  ◀── view: members=[N2,N3] ──▶
   (gone)        on_view→worker   on_view→worker
                 elect: idx0=N2   elect: idx0=N2
                 self=Y           self=N
                 GTID gate OK      STOP REPLICA (noop)
                 super_read_only=ON
                 START REPLICA ───────────────────────────▶ primary source
                 status: IS_ACTIVE_REPLICA=YES
```

### 12.2. Source failover (Scenario B)

```
DR active replica            Primary cluster (D1)
  refresh_source_list():       N1(connected) N2 N3
    reconcile ACF list = {N1*,N2,N3}   (N1 highest weight)
  ... D1-N1 crashes ...
    ACF IO thread detects loss
    do_auto_conn_failover() picks next candidate (N2)
    reconnect to N2 (AUTO_POSITION) ─────────▶ resumes
  next refresh_source_list():
    reconcile ACF list = {N2*,N3}      (prune N1, when membership known)
```

---

## 13. BUILD INTEGRATION

* `sql/CMakeLists.txt`: add `wsrep_async_failover.cc` inside the existing
  `WITH_WSREP` conditional.
* `storage/perfschema/CMakeLists.txt`: add
  `table_wsrep_async_failover_status.cc`.
* No new third-party dependency. Compiles under the project's `-Werror` profile.

For the enumeration of every bug fixed during development and the full
cross-version / upgrade / protocol compatibility proof, see
[Compatibility & Bugs](WL_PXC-5201_Compatibility_and_Bugs.md).
