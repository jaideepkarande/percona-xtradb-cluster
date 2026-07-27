# PXC-5201 — User Documentation (hand-off to the docs team)

This document is written for the Percona documentation team. It describes the
**user-facing** behaviour, configuration, and observability of the
*Cluster-Aware Asynchronous Replication Failover* feature added by PXC-5201. It
intentionally avoids implementation internals (those live in
`low_level_design.md`).

---

## 1. Feature name and one-line description

**Cluster-Aware Asynchronous Replication Failover** — Percona XtraDB Cluster can
automatically manage the classic (asynchronous) replication link between two
Galera clusters in an Active/Disaster-Recovery topology, re-electing the replica
node and failing the source connection over to surviving nodes without any
external tooling or DBA intervention.

Availability: Percona XtraDB Cluster 8.0.45-36 and later.

---

## 2. Concept

In a multi-datacenter deployment:

* **Primary (D1)** and **DR (D2)** are each a separate, internally
  highly-available Galera cluster.
* The two clusters are linked by one **asynchronous replication channel**:
  a *source* node in D1 streams binlog to a *replica* node in D2.

Historically that single channel was a single point of failure. With this
feature enabled:

* If the **replica** node in D2 fails, PXC automatically elects another D2 node
  and resumes the channel (**receiver failover**).
* If the **source** node in D1 fails, PXC automatically moves the replica's
  connection to a surviving D1 node (**source failover**), keeping the candidate
  source list in step with D1's live membership.
* The DR cluster is automatically placed in `super_read_only` to prevent
  split-brain writes.
* GTID consistency is verified before the replica is (re)started.

The feature builds on, and requires, GTID-based replication
(`gtid_mode=ON`, `enforce_gtid_consistency=ON`) and channel
`SOURCE_AUTO_POSITION=1`.

---

## 3. System variables

All variables are `GLOBAL`, dynamic (settable with `SET GLOBAL`), persistable
(`SET PERSIST`), and may be set in `my.cnf`. They take effect on all nodes where
they are set; in practice you set them identically on every node of a cluster.

| Variable | Type | Default | Scope | Dynamic | Description |
|----------|------|---------|-------|---------|-------------|
| `wsrep_async_failover` | Boolean | `OFF` | Global | Yes | Master switch. When `ON`, the failover coordinator runs on this node. |
| `wsrep_async_failover_mode` | Enum | `OFF` | Global | Yes | Role of this cluster in the async link. One of `OFF`, `RECEIVER`, `SOURCE`, `BOTH`. |
| `wsrep_async_failover_channel` | String | `''` (empty) | Global | Yes | Name of the asynchronous replication channel the coordinator manages on the receiver side. Empty means the default channel. |
| `wsrep_async_failover_gtid_check` | Enum | `ENFORCE` | Global | Yes | GTID-consistency policy applied before (re)starting the replica. One of `OFF`, `WARN`, `ENFORCE`. |
| `wsrep_async_failover_read_only` | Boolean | `ON` | Global | Yes | When acting as `RECEIVER`/`BOTH`, automatically keep the DR cluster in `super_read_only`. |
| `wsrep_async_failover_check_interval` | Integer (seconds) | `5` | Global | Yes | How often the coordinator re-evaluates membership and consistency, in addition to reacting immediately to cluster view changes. Range 1–3600. |

### Mode values

* `OFF` — coordinator does nothing (even if `wsrep_async_failover=ON`).
* `RECEIVER` — this is the DR cluster; it elects an active replica, keeps it
  running, and (when enabled) enforces `super_read_only`.
* `SOURCE` — this is the primary cluster; it cooperates as a managed source
  group so the DR side can discover its members.
* `BOTH` — symmetric configuration (e.g. bi-directional DR).

### GTID-check values

* `ENFORCE` (default) — if errant transactions (or other inconsistencies) are
  detected, the replica is **not** started/relocated; the condition is logged
  and shown in the status table; the coordinator retries once the state clears.
* `WARN` — the inconsistency is logged as a warning but the replica is still
  started.
* `OFF` — the check is skipped entirely.

---

## 4. New managed source type

The existing UDF `asynchronous_connection_failover_add_managed()` now accepts a
new managed type, **`GaleraCluster`**, alongside the existing
`GroupReplication`:

```sql
SELECT asynchronous_connection_failover_add_managed(
  'channel_name',     -- async channel
  'GaleraCluster',    -- managed type (NEW)
  'primary-cluster',  -- managed name (a label for the source cluster)
  '10.0.0.11',        -- seed source host (any healthy primary node)
  3306,               -- seed source port
  '',                 -- network namespace
  80,                 -- primary weight (1..100)
  60);                -- secondary weight (1..100)
```

Use `asynchronous_connection_failover_delete_managed('channel_name',
'primary-cluster')` to remove it. All other ACF UDFs
(`..._add_source`, `..._delete_source`, `..._reset`) are unchanged.

---

## 5. Observability

### 5.1 Performance Schema table

A new table, `performance_schema.wsrep_async_failover_status`, exposes one row
describing the coordinator state on the local node:

| Column | Description |
|--------|-------------|
| `ENABLED` | `YES`/`NO` — whether the coordinator is enabled on this node. |
| `MODE` | `OFF` / `RECEIVER` / `SOURCE` / `BOTH`. |
| `CHANNEL_NAME` | The managed async channel. |
| `IS_ACTIVE_REPLICA` | `YES` if this node is the currently elected active replica. |
| `ELECTED_INDEX` | wsrep index of the elected active replica node (`-1` if none). |
| `CLUSTER_SIZE` | Members in the current primary component. |
| `GTID_CONSISTENCY` | Verdict of the last GTID gate: `OK`, `ERRANT`, `GAP`, `XID_MISMATCH`, or `SKIPPED`. |
| `SUPER_READ_ONLY_MANAGED` | `YES` if the coordinator is currently holding `super_read_only`. |
| `LAST_ACTION` | Human-readable description of the last decision. |
| `LAST_ACTION_TIMESTAMP` | When that action was taken. |

Example:

```sql
SELECT * FROM performance_schema.wsrep_async_failover_status\G
```

### 5.2 Existing ACF tables

The source candidate list and managed-group configuration remain visible in the
standard tables:

* `performance_schema.replication_asynchronous_connection_failover`
* `performance_schema.replication_asynchronous_connection_failover_managed`
* `performance_schema.replication_connection_configuration`
  (column `SOURCE_CONNECTION_AUTO_FAILOVER`)

### 5.3 Error log

The coordinator logs notable transitions with the stable prefix `[wsrep-acf]`,
for example:

```
[wsrep-acf] asynchronous replication failover coordinator started
[wsrep-acf] elected this node (index 0) as active async replica for channel 'dc_link'
[wsrep-acf] enabled super_read_only on DR cluster
[wsrep-acf] GTID consistency check failed (ERRANT: ...) for channel 'dc_link'; replica not started
[wsrep-acf] yielded active async replica role for channel 'dc_link'
```

---

## 6. End-to-end setup guide (for the docs "How to" section)

### Prerequisites
* Two PXC clusters (Primary D1, DR D2), each running this PXC version.
* `gtid_mode=ON`, `enforce_gtid_consistency=ON` on every node of both clusters.
* A replication user that exists on D1 (it replicates across the cluster via
  Galera) and is permitted to connect from D2.

### Step 1 — Primary cluster (D1), every node `my.cnf`
```ini
[mysqld]
gtid_mode=ON
enforce_gtid_consistency=ON
wsrep_async_failover=ON
wsrep_async_failover_mode=SOURCE
```

### Step 2 — DR cluster (D2), every node `my.cnf`
```ini
[mysqld]
gtid_mode=ON
enforce_gtid_consistency=ON
wsrep_async_failover=ON
wsrep_async_failover_mode=RECEIVER
wsrep_async_failover_channel=dc_link
wsrep_async_failover_gtid_check=ENFORCE
wsrep_async_failover_read_only=ON
```

### Step 3 — DR cluster, define the channel on every D2 node (run once per node)
```sql
CHANGE REPLICATION SOURCE TO
   SOURCE_HOST='d1-node1', SOURCE_PORT=3306,
   SOURCE_USER='repl', SOURCE_PASSWORD='<secret>',
   SOURCE_AUTO_POSITION=1,
   SOURCE_CONNECTION_AUTO_FAILOVER=1
 FOR CHANNEL 'dc_link';
```

### Step 4 — DR cluster, register the primary as a managed Galera source (run once)
```sql
SELECT asynchronous_connection_failover_add_managed(
  'dc_link', 'GaleraCluster', 'd1-cluster',
  'd1-node1', 3306, '', 80, 60);
```

> Do **not** run `START REPLICA` yourself. The coordinator starts it on the
> elected node.

### Step 5 — Verify
```sql
SELECT IS_ACTIVE_REPLICA, ELECTED_INDEX, GTID_CONSISTENCY
  FROM performance_schema.wsrep_async_failover_status;   -- on each D2 node
```
Exactly one D2 node reports `IS_ACTIVE_REPLICA=YES`.

---

## 7. Operational notes & caveats (for the docs "Notes/Limitations" section)

* **Default off.** Without `wsrep_async_failover=ON` the behaviour is exactly as
  in previous releases; the feature is opt-in.
* **Disabling is non-disruptive.** Setting `wsrep_async_failover=OFF` stops the
  coordinator but does not stop an already-running replica; the bit-managed
  `super_read_only` is released only if the coordinator set it.
* **Single active replica.** Only one node per DR cluster runs the channel at a
  time, guaranteed by deterministic election on the Galera view.
* **Applications and the DR cluster.** With `wsrep_async_failover_read_only=ON`
  the whole DR cluster is read-only to applications. Direct your write traffic
  to the primary cluster only. Replication still applies on DR (the applier
  bypasses `super_read_only`).
* **GTID gate.** In `ENFORCE` mode the coordinator refuses to start the replica
  if it detects errant transactions, and surfaces the reason in the status
  table and error log. Resolve the inconsistency (or set the policy to `WARN`)
  to proceed.
* **Multi-UUID caveat.** Errant-transaction detection is most precise when the
  cluster uses a single GTID UUID (`wsrep_gtid_mode=ON`). See the engineering
  notes if your deployment intentionally mixes server UUIDs.
* **Credentials.** The feature does not create the replication account or the
  channel definition; create those once as shown above.

---

## 8. Cross-references for the documentation site

* Upstream MySQL *Asynchronous Connection Failover* (the underlying mechanism):
  the MySQL Replication manual, "Switching Sources and Replicas with
  Asynchronous Connection Failover".
* PXC *GTID* and *async replication between clusters* existing pages should link
  to this feature as the recommended way to make the inter-cluster link
  resilient.
