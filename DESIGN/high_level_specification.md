# PXC-5201 — High Level Specification

**Worklog:** PXC-5201
**Title:** Native Automatic Asynchronous Replication Failover for Multi-Datacenter PXC Clusters
**Target version:** Percona XtraDB Cluster 8.0.45-36 (and forward-port to 8.4)
**Status:** Design / Implementation
**Related upstream work:** Oracle MySQL `WL#12649`, `WL#14019`, `WL#14020`
(Automatic connection failover for Async Replication Channels, steps I–III) — commits
`571f6504443bec2c64c64e17db641fd1849bca60` and `7b1a55e2dfbbc000af1bbc4c15eb1537db3b950d`.

---

## 1. Problem statement

A typical multi-datacenter Percona XtraDB Cluster (PXC) deployment runs two
independent Galera clusters:

```
        Datacenter 1 (PRIMARY / Active)            Datacenter 2 (DR / Passive)
   ┌─────────────────────────────────┐        ┌─────────────────────────────────┐
   │  D1-N1   D1-N2   D1-N3           │        │  D2-N1   D2-N2   D2-N3           │
   │   ▲────────▲───────▲ Galera sync │        │   ▲────────▲───────▲ Galera sync │
   └───┼─────────────────────────────┘        └───┼─────────────────────────────┘
       │ (async SOURCE = D1-N1)                    │ (async REPLICA = D2-N1)
       └───────────────  asynchronous replication  ───────────────────┘
```

Inside each datacenter, Galera provides synchronous, multi-master, self-healing
high availability. **Between** the datacenters, a single classic MySQL
asynchronous replication channel links one *source* node in D1 to one *replica*
node in D2.

That async link is a **single point of failure**:

* **Replica failure (Scenario A).** If `D2-N1` (the active replica) crashes or
  leaves its Galera cluster, replication into the DR site stops until a DBA
  manually runs `CHANGE REPLICATION SOURCE` + `START REPLICA` on another D2 node.
* **Source failure (Scenario B).** If `D1-N1` (the active source) crashes,
  replication stops. MySQL 8.0 ships *Asynchronous Connection Failover* (ACF)
  that can move the replica's connection to another source, but the candidate
  source list must be configured and maintained by hand. PXC does not currently
  derive that list from its own cluster membership.

PXC-5201 makes PXC **natively** manage the async link by binding it to Galera
cluster membership and state, so that both scenarios are handled automatically,
with no external orchestrator (Orchestrator, custom cron, ProxySQL scripts, …).

---

## 2. Goals

| # | Goal |
|---|------|
| G1 | Automatically re-elect a new **replica** node inside the DR cluster when the active replica fails (Scenario A). |
| G2 | Automatically keep the **source candidate list** in sync with the healthy members of the primary cluster and fail the connection over to a surviving source (Scenario B). |
| G3 | Require **zero external tooling** — everything is driven from inside `mysqld`/`wsrep`. |
| G4 | Guarantee **GTID consistency is verified** before automated async management acts (gaps, errant transactions, `Xid → GTID_NEXT` agreement across members). |
| G5 | Put the DR cluster into `super_read_only` automatically to remove any split-brain window (mirrors the InnoDB-Cluster behaviour). |
| G6 | Provide **operational visibility** through Performance Schema and status variables. |
| G7 | Be **safe by default** — the feature is `OFF` until explicitly enabled, and degrades to today's manual behaviour when disabled. |

## 3. Non-goals

* Replacing Galera's intra-cluster synchronous replication.
* Conflict resolution / multi-primary async topologies (active/active between DCs).
* Automatic provisioning of the replication *user* / credentials (the DBA still
  creates the replication account and the channel definition once).
* Cross-version GTID translation. Both clusters are expected to run a PXC
  release that contains PXC-5201 (or at least be GTID-mode `ON`,
  `AUTO_POSITION=1`).
* WAN topology discovery (which nodes are reachable across the WAN is derived
  from the cluster's own `wsrep_incoming_addresses`, not probed independently).

---

## 4. Solution overview

PXC-5201 introduces a **Cluster-Aware Asynchronous Replication Failover
Coordinator** (henceforth *the coordinator*) inside the WSREP service layer.

The coordinator reacts to Galera **view changes** (the authoritative, totally
ordered membership signal already delivered to every node) and to a periodic
timer. It builds on the *existing* MySQL ACF machinery instead of replacing it:

* **Receiver (DR) side — Scenario A.**
  Every node in the DR cluster runs the coordinator. On each primary view it
  deterministically **elects** exactly one *active replica* node (the member at
  `own_index == 0` of the current primary component, i.e. the lowest-ordered
  live member). The elected node:
  1. verifies GTID consistency (G4),
  2. ensures the DR cluster is `super_read_only` (G5),
  3. issues `START REPLICA` for the managed channel.
  Non-elected nodes ensure the channel is **stopped** locally. When the elected
  node leaves the cluster, the next view promotes a new node, which transparently
  resumes the channel using `AUTO_POSITION=1` GTIDs.

* **Source (primary) side — Scenario B.**
  The active replica node keeps the ACF source list
  (`performance_schema.replication_asynchronous_connection_failover`) aligned
  with the *healthy* members of the primary cluster. The primary cluster is
  registered as a **managed source group** of a new managed type
  `GaleraCluster`. The coordinator periodically reads the primary cluster's
  live membership and (re)writes the candidate source rows. The existing ACF IO
  thread then performs the actual reconnect to a surviving source when the
  current one dies — exactly the mechanism delivered by `WL#12649`/`WL#14020`,
  but fed by Galera membership instead of Group Replication membership.

```
                        ┌──────────────────────────────────────────────┐
   Galera view change ─▶│  Wsrep_async_failover_coordinator (per node)  │
   periodic timer ─────▶│                                              │
                        │  ┌───────────────┐   ┌────────────────────┐  │
                        │  │ Receiver logic│   │   Source logic     │  │
                        │  │  (Scenario A) │   │   (Scenario B)     │  │
                        │  └──────┬────────┘   └─────────┬──────────┘  │
                        └─────────┼──────────────────────┼─────────────┘
                                  │                       │
              elect / START/STOP  │                       │ refresh ACF source list
              super_read_only     ▼                       ▼
                       rpl_channel_service_interface   replication_asynchronous_
                       (start_slave / stop_slave)      connection_failover table
```

### 4.1 Why reuse ACF instead of re-implementing failover

The Oracle worklogs already implement, in battle-tested server code, the hard
parts of *source* failover: weighted candidate selection, retry/timeout
handling, quorum checks, the monitor IO thread, and the persisted
`mysql.replication_asynchronous_connection_failover*` tables. PXC-5201 supplies
the **Galera-aware policy** (who is the replica, which sources are healthy,
is GTID state safe) and lets the proven mechanism do the mechanics. This keeps
the new surface area small and the failure modes well understood.

---

## 5. User-visible surface (summary)

New global system variables (all `wsrep_`-prefixed, dynamic):

| Variable | Type | Default | Purpose |
|----------|------|---------|---------|
| `wsrep_async_failover` | BOOLEAN | `OFF` | Master switch for the coordinator. |
| `wsrep_async_failover_mode` | ENUM(`OFF`,`RECEIVER`,`SOURCE`,`BOTH`) | `OFF` | Role this cluster plays in the async link. |
| `wsrep_async_failover_channel` | STRING | `''` | Async channel the coordinator manages on the receiver side. |
| `wsrep_async_failover_gtid_check` | ENUM(`OFF`,`WARN`,`ENFORCE`) | `ENFORCE` | GTID-consistency policy before (re)starting the replica. |
| `wsrep_async_failover_read_only` | BOOLEAN | `ON` | Keep the DR cluster in `super_read_only`. |
| `wsrep_async_failover_check_interval` | INTEGER (s) | `5` | Periodic re-evaluation interval. |

New managed source type accepted by `asynchronous_connection_failover_add_managed`:
`GaleraCluster`.

New Performance Schema table: `performance_schema.wsrep_async_failover_status`
(one row per node, exposing role, election result, active channel, GTID-check
verdict and last action). See `low_level_design.md` §7.

New status variables: `wsrep_async_failover_role`,
`wsrep_async_failover_is_active_replica`,
`wsrep_async_failover_gtid_consistent`.

---

## 6. Example end-to-end configuration

On **both** clusters, once:

```sql
-- enable GTILD-based replication everywhere (pre-requisite)
-- gtid_mode = ON, enforce_gtid_consistency = ON  (my.cnf)
```

On the **primary** cluster (D1), each node:

```ini
# my.cnf
wsrep_async_failover            = ON
wsrep_async_failover_mode       = SOURCE
```

On the **DR** cluster (D2), each node:

```ini
# my.cnf
wsrep_async_failover            = ON
wsrep_async_failover_mode       = RECEIVER
wsrep_async_failover_channel    = 'dc_link'
wsrep_async_failover_gtid_check = ENFORCE
wsrep_async_failover_read_only  = ON
```

On **one** DR node, once (the definition is created on every DR node, but the
coordinator only *runs* it on the elected node):

```sql
-- register the primary cluster as a managed Galera source group
SELECT asynchronous_connection_failover_add_managed(
         'dc_link', 'GaleraCluster',
         'd1-cluster',                 -- managed name == primary cluster name
         'd1-n1.example.com', 3306, '', 80, 60);

CHANGE REPLICATION SOURCE TO
   SOURCE_HOST='d1-n1.example.com', SOURCE_PORT=3306,
   SOURCE_USER='repl', SOURCE_PASSWORD='...',
   SOURCE_AUTO_POSITION=1,
   SOURCE_CONNECTION_AUTO_FAILOVER=1
 FOR CHANNEL 'dc_link';
```

From this point on:

* DBA never runs `START REPLICA` again — the coordinator does, on whichever DR
  node is currently elected.
* If the elected DR node dies, another DR node resumes the channel within one
  Galera view change + GTID check.
* If the connected D1 source dies, ACF moves the connection to a surviving D1
  node, whose address was published into the source list by the coordinator.

---

## 7. Compatibility & safety

* **Default off.** With `wsrep_async_failover=OFF` PXC behaves exactly as today;
  the manual `CHANGE REPLICATION SOURCE` workflow is unchanged.
* **Graceful disable.** Setting the variable to `OFF` at runtime stops the
  coordinator thread; it does **not** stop an already-running replica or clear
  `super_read_only`, so disabling is non-disruptive and reversible.
* **No split brain.** With `wsrep_async_failover_read_only=ON` the entire DR
  cluster is `super_read_only`; only the replication applier (which bypasses
  `super_read_only`) may write, so application traffic accidentally pointed at
  DR cannot diverge.
* **GTID gate.** With `wsrep_async_failover_gtid_check=ENFORCE` the coordinator
  refuses to start/relocate the replica while it detects GTID gaps or errant
  transactions, surfacing the condition instead of silently corrupting DR.

See `requirements.md` for the normative requirement list and
`low_level_design.md` for the implementation detail.
