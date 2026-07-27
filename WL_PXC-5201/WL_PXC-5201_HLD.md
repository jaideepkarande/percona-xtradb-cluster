# WL#PXC-5201: Native Automatic Asynchronous Replication Failover — High Level Architecture

**Affects:** Percona XtraDB Cluster 8.0 / 8.4 — **Status:** Complete

| [Description](WL_PXC-5201_Description.md) | [Requirements](WL_PXC-5201_FR.md) | Dependent Tasks | **High Level Architecture** | [Low Level Design](WL_PXC-5201_LLD.md) | [Compatibility & Bugs](WL_PXC-5201_Compatibility_and_Bugs.md) |
|---|---|---|---|---|---|

---

## 1. INTRODUCTION

This worklog introduces a **Cluster-Aware Asynchronous Replication Failover
Coordinator** inside the PXC WSREP service layer. Its purpose is to keep the
single classic (asynchronous) replication channel that links two Galera clusters
alive across the failure of either endpoint, by binding the channel's lifecycle
to Galera cluster membership.

```
   DR cluster D2 [N1*, N2, N3]                 DR cluster D2 [N2*, N3]
   N1* --async--> (running)      N1 leaves     N2* --async--> (running)
   N2, N3 (stopped)              --------->    N3 (stopped)

   The elected active replica N1 fails; the next Galera view promotes N2,
   which verifies GTID state and resumes the channel with AUTO_POSITION.
```

The design reuses the existing MySQL *Asynchronous Connection Failover* (ACF)
mechanism from `WL#12649` / `WL#14019` / `WL#14020` for the *source-side*
mechanics (weighted candidate selection, the monitor IO thread, the persisted
`mysql.replication_asynchronous_connection_failover*` tables). PXC-5201 supplies
the **Galera-aware policy**: which DR node is the replica, which primary nodes are
healthy sources, and whether the local GTID state is safe. This keeps the new
surface area small and the failure modes well understood.

The feature is split into two cooperating halves:

1. **Receiver logic (Scenario A):** deterministic election of a single active
   replica in the DR cluster, `super_read_only` enforcement, the GTID gate, and
   `START`/`STOP REPLICA`.
2. **Source logic (Scenario B):** keeping the ACF candidate-source list aligned
   with the primary cluster's live Galera membership, so the stock ACF IO thread
   reconnects to a surviving source on failure.

---

## 2. COORDINATOR OVERVIEW

Every node runs one coordinator (`Wsrep_async_failover`, a singleton with one
background worker thread). It reacts to two inputs:

* the **Galera view change** — the authoritative, totally-ordered membership
  signal, delivered through `Wsrep_server_service::log_view()`; and
* a **periodic timer** (`wsrep_async_failover_check_interval`, default 5 s) so a
  missed callback or a slow-to-settle state self-heals.

```
                        ┌──────────────────────────────────────────────┐
   Galera view change ─▶│  Wsrep_async_failover (per node, 1 worker)    │
   periodic timer ─────▶│                                              │
                        │  ┌───────────────┐   ┌────────────────────┐  │
                        │  │ Receiver logic│   │   Source logic     │  │
                        │  │  (Scenario A) │   │   (Scenario B)     │  │
                        │  └──────┬────────┘   └─────────┬──────────┘  │
                        └─────────┼──────────────────────┼─────────────┘
              elect / START/STOP  │                       │ refresh ACF source list
              super_read_only     ▼                       ▼
                    START REPLICA / STOP REPLICA   replication_asynchronous_
                    SET GLOBAL super_read_only     connection_failover table
```

All blocking work (SQL, `START`/`STOP REPLICA`, `SET GLOBAL super_read_only`) runs
on the **worker thread** via an internal `Ed_connection`, never inside the total
-order view callback (NFR1). The callback only snapshots the view and signals the
worker.

### 2.1. Why reuse ACF instead of re-implementing failover

The upstream worklogs already implement, in battle-tested server code, the hard
parts of source failover. PXC-5201 feeds that mechanism Galera membership instead
of Group Replication membership, via a new managed type. This is the same design
philosophy as `WL#14019` (which fed ACF from GR membership) — only the membership
source differs.

---

## 3. ELECTION MODEL (Scenario A)

The current Galera view is the input. Election rule (deterministic, no extra
messaging):

* The view callback captures, under a mutex: whether the view is a primary
  component, this node's `own_index()`, and the index of the **first member with
  a non-empty incoming address** (members with an empty incoming address are
  arbitrators / `garbd` and are skipped, so an arbitrator is never elected).
* A node is the active replica iff the view is primary and its `own_index()`
  equals that elected index.

Because Galera orders members identically on every node, every node computes the
same elected index without any additional protocol. When the elected node
disappears, the next view yields a new first-eligible member which then resumes
the channel. No leader lease or heartbeat is needed beyond Galera's own
membership. Until the first view arrives (startup / lone node) the coordinator
falls back to the `wsrep_cluster_status` / `wsrep_cluster_size` /
`wsrep_local_index` globals. See [LLD §4](WL_PXC-5201_LLD.md).

---

## 4. SECURITY CONTEXT

This section proves, against the delivered code, that PXC-5201 does not widen the
attack surface, mishandle credentials, or introduce a SQL-injection path.

**4.1. Execution identity.** The coordinator's worker runs on a dedicated
internal THD created with `create_internal_thd()` and marked `COM_DAEMON`, with
`thd->variables.wsrep_on = false`
(`sql/wsrep_async_failover.cc:368-370`). It is a server-internal system thread —
not reachable by any client connection — and executes only a fixed, closed set of
statement shapes: `START`/`STOP REPLICA`, `SET GLOBAL super_read_only=ON/OFF`,
the ACF `add_source`/`delete_source` UDF calls, and read-only `SELECT`s against
`performance_schema` / GTID functions. There is no code path by which a client can
make the coordinator run an arbitrary statement.

**4.2. Authorization on the user-facing surface.** The only user-facing entry
points are the six system variables and the managed-source UDF.
* Setting a `GLOBAL` system variable requires `SYSTEM_VARIABLES_ADMIN` (or
  `SUPER`) — this is enforced by the server for all `GLOBAL_VAR`s, not weakened
  here (`sql/sys_vars.cc`, PXC-5201 block).
* `asynchronous_connection_failover_add_managed()` still requires `SUPER` **or**
  `REPLICATION_SLAVE_ADMIN`
  (`sql/rpl_async_conn_failover_add_managed_udf.cc:202-209`). PXC-5201 only relaxed
  the *value* validation for the managed-type / managed-name arguments (B4/B5); it
  did **not** touch the privilege check.

**4.3. Credential handling — none.** The coordinator never reads, stores, logs,
or transmits replication credentials. As in `WL#12649`, the DBA configures the
channel user/password once via `CHANGE REPLICATION SOURCE`; the coordinator only
issues `START`/`STOP REPLICA`, which reuse the already-persisted credentials. A
scan of `sql/wsrep_async_failover.cc` shows no `PASSWORD` / `USER` / credential
string handling. Error-log lines carry only channel names, host:port, GTID sets,
and verdicts — never secrets.

**4.4. SQL-construction / injection surface.** The coordinator builds SQL by
string concatenation, so each embedded value is accounted for:
* **String literals are escaped.** `acf_sql_quote()` (in
  `sql/wsrep_async_failover.cc`) escapes `'` and `\` and is applied to every
  string value placed inside quotes — the channel name (via `acf_for_channel()`
  and directly), the received GTID set, the managed-group name, and candidate
  host names.
* **Numeric values are not user free-text, and are read as canonical decimals.**
  Ports and weights are inserted unquoted, but they originate only from
  server-maintained sources — the `PORT` column of the ACF tables (read as
  `CAST(PORT AS CHAR)`, so the value is always a canonical decimal string, never a
  raw binary/control-byte blob) and the `wsrep_incoming_addresses` status value
  parsed by `acf_parse_incoming_addresses()` — never from a client string. The
  weight is a hard-coded literal (`80`).
* **The one externally-set string that reaches SQL is the channel name**
  (`wsrep_async_failover_channel`, settable only with the admin privilege of
  §4.2), and it is escaped at every use site above. No client-controlled value
  reaches SQL unescaped.

**4.5. Observability exposes no secrets and is read-only.** The Performance Schema
table is registered with `&pfs_readonly_acl` and `write_row = nullptr` /
`delete_all_rows = nullptr`
(`storage/perfschema/table_wsrep_async_failover_status.cc:67-80`), so it cannot be
written by any user. Its columns are operational status only (role, election
result, GTID verdict, last action) — no credentials or connection secrets.

**4.6. `super_read_only`.** It is toggled through `SET GLOBAL super_read_only`
(`:439,:457`) so the normal `fix_super_read_only` privilege/state path runs; the
coordinator tracks ownership and releases only the bit it set (§6 of the LLD,
`m_holds_super_read_only`), so it can never clear protection a DBA established.

---

## 5. CROSS-VERSION REPLICATION

**There is no impact on cross-version replication.**

* The coordinator issues only standard `START REPLICA` / `STOP REPLICA` /
  `SET GLOBAL super_read_only` and read-only `performance_schema` queries. It
  introduces **no new binary-log event type, no new replication payload, and no
  change to any existing event's format.** *Code evidence:* neither PXC-5201
  commit touches `libbinlogevents/`, `sql/log_event*`, or the `sql/rpl_*` event
  classes (verified `git show --name-only`; full file list in
  [Compatibility & Bugs §4](WL_PXC-5201_Compatibility_and_Bugs.md)).
* The coordinator's control statements run on an internal THD with
  `wsrep_on=false` (`sql/wsrep_async_failover.cc:370`), and every new system
  variable is declared `NOT_IN_BINLOG` (`sql/sys_vars.cc`, PXC-5201 block), so
  they are **never written to the binary log and never Galera-replicated** — a
  peer node (any version) never sees them.
* The `GaleraCluster` managed type is stored as free text in the existing
  `mysql.replication_asynchronous_connection_failover_managed.Managed_type`
  column (no schema change — see §6); it is data, not protocol.
* Because the underlying transport is stock GTID-based asynchronous replication
  with `SOURCE_AUTO_POSITION=1`, the normal MySQL cross-version replication
  compatibility rules apply unchanged; PXC-5201 neither tightens nor relaxes
  them.

Full proof and the source-tree evidence are in
[Compatibility & Bugs §2](WL_PXC-5201_Compatibility_and_Bugs.md).

---

## 6. UPGRADE / DOWNGRADE

**No `mysql_upgrade`/bootstrap SQL is required and no `mysql.*` system-table
schema changes are introduced. The one data-dictionary effect — a
`PFS_DD_VERSION` bump for the new Performance Schema table — is handled
automatically by the server on first start, exactly as for every P_S table.**

* **No persisted user/system-table schema change.** Unlike `WL#12649` (which
  added the `SOURCE_CONNECTION_AUTO_FAILOVER` column to `slave_master_info`),
  PXC-5201 adds **no** column to any `mysql.*` system table and no new persisted
  table. It reuses the existing ACF tables as-is. *Code evidence:* the change set
  contains no `.sql` bootstrap file and no `mysql.*` DDL.
* **The new Performance Schema table** `wsrep_async_failover_status` is
  **code-defined and in-memory** — `ENGINE=PERFORMANCE_SCHEMA`, declared inline as
  a `Plugin_table` and producing a single row from
  `Wsrep_async_failover::instance().snapshot()` on each read
  (`storage/perfschema/table_wsrep_async_failover_status.cc`). It carries no
  on-disk row data.
* **P_S data-dictionary version bump (required, automatic).** Because the *set*
  of P_S tables changed, `PFS_DD_VERSION` is bumped `80407 → 80408`
  (`storage/perfschema/pfs_dd_version.h`; the value stays `≤ MYSQL_VERSION_ID`,
  enforced by the `static_assert` in `ha_perfschema.cc`). On upgrade the server
  detects that its compiled `PFS_DD_VERSION` is newer than the DD stored on disk
  and **recreates the performance_schema DD table definitions itself** — no DBA
  action, no `mysql_upgrade` step for the operator, no user-data migration. This
  is the standard mechanism used for every P_S table addition (it is what the
  `perfschema.dd_version_check` guard verifies), and it is why that test's
  published-schema signature and `PFS_DD_VERSION` were updated in the same change.
* **New system variables default to OFF/empty** — `DEFAULT(false)` for
  `wsrep_async_failover`, `DEFAULT(0)`=`OFF` for the mode, `DEFAULT("")` for the
  channel (`sql/sys_vars.cc`, PXC-5201 block) — so a freshly upgraded server
  behaves identically to before until a DBA opts in (FR21). The variables are
  standard `Sys_var_*` with `CMD_LINE(...)` and are not marked `NON_PERSIST`, so
  `SET PERSIST` and `my.cnf` both work (FR22).
* **Rolling upgrade is safe:** with the feature off, a mixed-version PXC cluster
  is unaffected; nodes can be upgraded one at a time. The coordinator only acts in
  a primary component and never replicates its actions, so an upgraded node and a
  not-yet-upgraded node never disagree over the wire.
* **Downgrade caveat (documented, not a blocker):** the only persisted artifact a
  DBA can create is a candidate row of managed type `GaleraCluster` in the ACF
  table. An older binary's ACF monitor only understands `GroupReplication` and
  will not process a `GaleraCluster` row (it is inert data). Before downgrading to
  a pre-PXC-5201 binary, remove any `GaleraCluster` managed rows with
  `asynchronous_connection_failover_delete_managed()` and clear a
  coordinator-held `super_read_only`. No corruption results either way; this is a
  cleanliness step.

Full proof and the source-tree evidence are in
[Compatibility & Bugs §3](WL_PXC-5201_Compatibility_and_Bugs.md).

---

## 7. PROTOCOL

**There are no changes in any protocol.**

* **No Galera / wsrep protocol change.** PXC-5201 touches **zero** files under
  `percona-xtradb-cluster-galera/`, `wsrep-lib/`, or the wsrep-API submodule.
  The GCS/EVS/replication protocol version constants (`GCS_PROTO_MAX`, etc.) are
  untouched, so the group-communication handshake and provider protocol versions
  are unchanged. A PXC-5201 node and a pre-PXC-5201 node negotiate the same
  provider protocol.
* **No client/server wire-protocol change.** No new command packet, capability
  flag, or status field is added to the MySQL client protocol. The new
  observability surface is entirely SQL-level (a Performance Schema table and
  `SHOW STATUS` variables).
* **No replication protocol change** — see §5.

Full proof and the source-tree evidence are in
[Compatibility & Bugs §4](WL_PXC-5201_Compatibility_and_Bugs.md).

---

## 8. FAILURE MODEL SPECIFICATION

Each failure mode below names the code that handles it, so the handling is
verifiable, not asserted.

* **8.1. Active DR replica node crashes / leaves.** The next Galera primary view
  promotes the next eligible member, which runs the GTID gate and issues
  `START REPLICA` (Scenario A). Detection ≤ one view change + one check interval.
  Handled by `on_view()` → `process_once()` → `process_receiver()`
  (`sql/wsrep_async_failover.cc:288-316,393-432,582-688`).
* **8.2. DR cluster loses quorum (no primary component).** The coordinator does
  nothing (FR24): `process_once()` returns early when `elected_index < 0` and
  records *"Non-primary component; coordinator idle."*
  (`sql/wsrep_async_failover.cc:402-411`). It never starts a replica or rewrites
  the source list outside a primary component.
* **8.3. Connected primary source crashes.** The stock ACF IO thread detects the
  connection loss and reconnects to the highest-weight surviving candidate; the
  coordinator later prunes the departed member from the candidate list in
  `refresh_source_list()` (`:690-787`).
* **8.4. No candidate source available.** ACF fails as upstream with
  `ER_RPL_ASYNC_RECONNECT_FAIL_NO_SOURCE`; the coordinator surfaces the condition
  and retries once a candidate appears.
* **8.5. Unsafe GTID state on the elected node.** With `gtid_check=ENFORCE`,
  `gtid_gate()` returns `false` and `process_receiver()` returns **before** any
  `START REPLICA` (`:600-614`); the verdict (`GAP`/`ERRANT`/`XID_MISMATCH`) is
  stored and retried on the next iteration once the state clears.
* **8.6. Transient `performance_schema` query error inside the gate.** Every gate
  check **fails open**: a failed `acf_query_scalar()`/`acf_query_rows()` yields
  verdict `OK`/proceed rather than blocking (`:499-502,:513,:568-572`), so a
  transient hiccup cannot wedge failover.
* **8.7. `START REPLICA` refused for an unprovisioned / non-auto-position
  channel.** `process_receiver()` starts the channel only when it is configured
  with a source **and** `AUTO_POSITION=1` (`:616-668`), avoiding both noisy errors
  and lost binlog coordinates (B7/FR4).
* **8.8. Server shutdown, possibly with a hung SQL step.** `deinit()` → 
  `stop_thread()` sets `m_abort`, broadcasts the condition variable, and
  `my_thread_join`s the worker (`:229-268`); the worker sleeps on
  `mysql_cond_timedwait` between iterations (no busy poll, `:379`) so shutdown is
  bounded (NFR6/NFR7).

### 8.9. Concurrency & known-limitation notes (full disclosure)

* All mutable coordinator state (`m_status`, the view snapshot, `m_holds_super_
  read_only`, thread flags) is guarded by `m_mutex`; `on_view()` only snapshots and
  signals (`:300-316`), so nothing blocking runs in Galera total order (NFR1).
* The worker reads a few global system variables (`wsrep_async_failover_channel`,
  `_mode`, `_gtid_check`, `_read_only`, `_check_interval`) without holding
  `LOCK_global_system_variables`. This matches the established pattern for the
  other `wsrep_*` globals and is benign: the values change rarely, a stale read
  simply self-corrects on the next iteration, and the periodic timer bounds the
  staleness to one interval.
* The cross-datacenter source-list *prune* is intentionally scoped out for the
  pure `RECEIVER` case (add-only) because the DR node cannot observe the primary's
  live membership without a WAN pull; this is disclosed in
  [LLD §8.1](WL_PXC-5201_LLD.md), not silently limited.

---

## 9. USER INTERFACE

### 9.1. System variables

All variables are `GLOBAL`, dynamic (`SET GLOBAL`), persistable (`SET PERSIST`),
and settable in `my.cnf`.

| Variable | Type | Default | Purpose |
|----------|------|---------|---------|
| `wsrep_async_failover` | BOOLEAN | `OFF` | Master switch for the coordinator. |
| `wsrep_async_failover_mode` | ENUM(`OFF`,`RECEIVER`,`SOURCE`,`BOTH`) | `OFF` | Role this cluster plays in the async link. |
| `wsrep_async_failover_channel` | STRING | `''` | Managed async channel (empty = default channel). |
| `wsrep_async_failover_gtid_check` | ENUM(`OFF`,`WARN`,`ENFORCE`) | `ENFORCE` | GTID-consistency policy before (re)starting the replica. |
| `wsrep_async_failover_read_only` | BOOLEAN | `ON` | Keep the DR cluster in `super_read_only`. |
| `wsrep_async_failover_check_interval` | INTEGER seconds (1–3600) | `5` | Periodic re-evaluation interval. |

### 9.2. New managed source type

`asynchronous_connection_failover_add_managed()` now accepts `GaleraCluster`
alongside `GroupReplication`:

```sql
SELECT asynchronous_connection_failover_add_managed(
  'dc_link',        -- async channel
  'GaleraCluster',  -- managed type (NEW)
  'd1-cluster',     -- managed name (a label for the source cluster; not a UUID)
  '10.0.0.11',      -- seed source host (any healthy primary node)
  3306,             -- seed source port
  '',               -- network namespace
  80,               -- primary weight (1..100)
  60);              -- secondary weight (1..100)

SELECT asynchronous_connection_failover_delete_managed('dc_link', 'd1-cluster');
```

### 9.3. End-to-end setup

```ini
# Primary cluster D1 — every node my.cnf
gtid_mode=ON
enforce_gtid_consistency=ON
wsrep_async_failover=ON
wsrep_async_failover_mode=SOURCE
```
```ini
# DR cluster D2 — every node my.cnf
gtid_mode=ON
enforce_gtid_consistency=ON
wsrep_async_failover=ON
wsrep_async_failover_mode=RECEIVER
wsrep_async_failover_channel=dc_link
wsrep_async_failover_gtid_check=ENFORCE
wsrep_async_failover_read_only=ON
```
```sql
-- DR cluster: define the channel on every D2 node (once per node)
CHANGE REPLICATION SOURCE TO
   SOURCE_HOST='d1-node1', SOURCE_PORT=3306,
   SOURCE_USER='repl', SOURCE_PASSWORD='<secret>',
   SOURCE_AUTO_POSITION=1, SOURCE_CONNECTION_AUTO_FAILOVER=1
 FOR CHANNEL 'dc_link';

-- DR cluster: register the primary as a managed Galera source (once)
SELECT asynchronous_connection_failover_add_managed(
  'dc_link', 'GaleraCluster', 'd1-cluster', 'd1-node1', 3306, '', 80, 60);
```

> Do **not** run `START REPLICA` yourself — the coordinator starts it on the
> elected node.

---

## 10. OBSERVABILITY

### 10.1. Performance Schema — `wsrep_async_failover_status`

One row per node describing the local coordinator state:

| Column | Meaning |
|--------|---------|
| `ENABLED` | Coordinator enabled on this node (`YES`/`NO`). |
| `MODE` | `OFF` / `RECEIVER` / `SOURCE` / `BOTH`. |
| `CHANNEL_NAME` | Managed async channel. |
| `IS_ACTIVE_REPLICA` | This node is the elected active replica (`YES`/`NO`). |
| `ELECTED_INDEX` | wsrep index of the elected replica (`-1` if none). |
| `CLUSTER_SIZE` | Members in the current primary component. |
| `GTID_CONSISTENCY` | Last gate verdict: `OK` / `GAP` / `ERRANT` / `XID_MISMATCH` / `SKIPPED`. |
| `SUPER_READ_ONLY_MANAGED` | Coordinator is holding `super_read_only` (`YES`/`NO`). |
| `LAST_ACTION` | Human-readable last decision. |
| `LAST_ACTION_TIMESTAMP` | When it was taken (nullable). |

### 10.2. Status variables

`wsrep_async_failover_role`, `wsrep_async_failover_is_active_replica`,
`wsrep_async_failover_gtid_consistent` (read from the same snapshot).

### 10.3. Existing ACF tables (unchanged)

`replication_asynchronous_connection_failover`,
`replication_asynchronous_connection_failover_managed`, and
`replication_connection_configuration.SOURCE_CONNECTION_AUTO_FAILOVER` remain the
source of truth for the candidate list and managed-group configuration.

### 10.4. Error log

Stable prefix `[wsrep-acf]`, e.g.:

```
[wsrep-acf] asynchronous replication failover coordinator started
[wsrep-acf] elected this node (index 0) as active async replica for channel 'dc_link'
[wsrep-acf] enabled super_read_only on DR cluster
[wsrep-acf] GTID consistency check failed (ERRANT: ...) for channel 'dc_link'; replica not started
[wsrep-acf] yielded active async replica role for channel 'dc_link'
```

---

## 11. COMPATIBILITY & SAFETY SUMMARY

* **Default off** (FR21). **Graceful, non-disruptive disable** (FR23). **No split
  brain** via `super_read_only` (FR17–19). **GTID gate** refuses unsafe starts
  (FR12–16).
* **No protocol, wire-format, or schema change** — see §5–§7 and the dedicated
  [Compatibility & Bugs](WL_PXC-5201_Compatibility_and_Bugs.md) document.

See [Low Level Design](WL_PXC-5201_LLD.md) for the implementation detail.
