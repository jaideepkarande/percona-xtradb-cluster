# WL#PXC-5201: Native Automatic Asynchronous Replication Failover — Requirements

**Affects:** Percona XtraDB Cluster 8.0 / 8.4 — **Status:** Complete

| [Description](WL_PXC-5201_Description.md) | **Requirements** | Dependent Tasks | [High Level Architecture](WL_PXC-5201_HLD.md) | [Low Level Design](WL_PXC-5201_LLD.md) | [Compatibility & Bugs](WL_PXC-5201_Compatibility_and_Bugs.md) |
|---|---|---|---|---|---|

Keywords **MUST**, **SHALL**, **SHOULD**, **MAY** are used per RFC 2119. Each
requirement is traceable to the [Low Level Design](WL_PXC-5201_LLD.md) and to the
MTR tests in `mysql-test/suite/galera/t/pxc_5201_*`.

---

## FUNCTIONAL REQUIREMENTS

### Receiver-side failover — Scenario A (DR replica node failure)

**FR1.** While enabled in `RECEIVER`/`BOTH` mode, the cluster **MUST** designate
exactly **one** active replica node at any time within a single primary
component.

**FR2.** The election **MUST** be deterministic and identical on every node given
the same Galera view, without inter-node messaging beyond what Galera already
provides, so nodes never disagree about who is active.

**FR3.** When the active replica node leaves the primary component (crash,
graceful shutdown, or a partition that excludes it), a surviving node **MUST** be
elected and **MUST** resume the managed channel automatically, with no operator
action.

**FR4.** A newly elected node **MUST** resume replication using GTID
auto-positioning (`SOURCE_AUTO_POSITION=1`); a channel not configured for
auto-positioning **MUST** be refused (never started) so no binlog coordinates are
lost.

**FR5.** Non-elected nodes **MUST** ensure the managed channel's receiver (IO) and
applier (SQL) threads are stopped locally, guaranteeing single-writer semantics
for the async link.

**FR6.** When a previously-active node rejoins, it **MUST NOT** take over the
channel unless it wins the election again; otherwise it **MUST** stop its channel
threads.

### Source-side failover — Scenario B (primary source node failure)

**FR7.** While enabled in `SOURCE`/`BOTH` mode, the primary cluster **MUST** be
representable as a *managed source group* of the new managed type
`GaleraCluster`.

**FR8.** The active replica node **MUST** keep the candidate source list
(`performance_schema.replication_asynchronous_connection_failover`) aligned with
the healthy (primary-component) members of the primary cluster: members that join
are added; members that leave are removed **when their live membership is
observable** (see LLD §8.1 for the cross-datacenter scope note).

**FR9.** When the currently-connected source fails, the replica **MUST** fail the
connection over to another candidate source using the existing ACF reconnect
mechanism, with no operator action.

**FR10.** The managed-source UDF `asynchronous_connection_failover_add_managed()`
**MUST** accept the new managed-type string `GaleraCluster` in addition to
`GroupReplication`, and **MUST NOT** enforce a UUID-format check on the managed
name for the `GaleraCluster` type (Galera cluster labels are not UUIDs).

**FR11.** The candidate weights (primary weight / secondary weight) **SHOULD** be
configurable and **SHOULD** prefer keeping the current connection stable — no
needless flapping when membership is unchanged.

### GTID consistency gate

**FR12.** Before the coordinator starts or relocates the replica it **MUST**
evaluate GTID consistency of the local node according to
`wsrep_async_failover_gtid_check`.

**FR13.** The check **MUST** detect (a) **gaps** in `gtid_executed`, (b) **errant
transactions** — GTIDs originating from neither the managed source nor this
cluster — and (c) **in-doubt** (prepared-but-not-committed) transactions
representing the local half of the historical PXC `Xid → GTID_NEXT` inconsistency
class.

**FR14.** In `ENFORCE` mode a failed check **MUST** prevent the replica from
starting/relocating and **MUST** be surfaced (error log + status table). In
`WARN` mode it **MUST** log a warning but proceed. In `OFF` mode the check is
skipped.

**FR15.** The GTID verdict (`OK` / `GAP` / `ERRANT` / `XID_MISMATCH` / `SKIPPED`)
**MUST** be exposed for observability (Performance Schema table + status
variable).

**FR16.** Errant-transaction detection **MUST NOT** mis-flag the cluster's own
replicated workload — which Galera stamps under the cluster-wide wsrep GTID UUID
(`wsrep_cluster_state_uuid`), distinct from any node's `@@server_uuid` — as
errant. Both the node UUID and the cluster UUID **MUST** be treated as
local-origin (regression-guarded by `galera.pxc_5201_circular`).

### Split-brain protection

**FR17.** While enabled in `RECEIVER`/`BOTH` mode with
`wsrep_async_failover_read_only=ON`, every node of the DR cluster **MUST** be
placed in `super_read_only`.

**FR18.** The replication applier **MUST** still be able to apply on the DR
cluster while `super_read_only` is set (the applier bypasses read-only).

**FR19.** Toggling `wsrep_async_failover_read_only=OFF` **MUST** clear only the
`super_read_only` bit the coordinator itself set; a `super_read_only` established
independently by the DBA **MUST** be left untouched.

### Configuration & lifecycle

**FR20.** All behaviour **MUST** be controlled by the `wsrep_async_failover*`
system variables (HLD §9).

**FR21.** The feature **MUST** default to `OFF`; an unconfigured or upgraded
server **MUST** behave exactly as before (no measurable behavioural change from
the baseline).

**FR22.** Variables **MUST** be dynamically settable (`SET GLOBAL`) and, where
relevant, persistable (`SET PERSIST`) and settable from `my.cnf`.

**FR23.** Enabling the feature **MUST** start the coordinator; disabling it
**MUST** stop the coordinator thread cleanly (no leaked threads, no shutdown
hang). Disabling **MUST NOT** stop an already-running replica or clear a
coordinator-held `super_read_only` (non-disruptive disable).

**FR24.** The coordinator **MUST NOT** act on a node that is not in the primary
component: a non-primary / partitioned node **MUST** never start a replica or
rewrite the source list.

---

## NON-FUNCTIONAL REQUIREMENTS

**NFR1 (Safety / total order).** The coordinator **MUST NOT** perform blocking or
long-running SQL inside the Galera view callback (which runs in total order); all
such work **MUST** be handed to a dedicated background worker thread. The view
callback only snapshots membership and signals the worker.

**NFR2 (Determinism).** Given identical views and identical configuration, all
nodes **MUST** reach the same election decision without extra inter-node
messaging.

**NFR3 (Latency).** Failover (detection → new replica running) **SHOULD** complete
within `wsrep_async_failover_check_interval` + one Galera view change + the GTID
check — typically a few seconds.

**NFR4 (Observability).** Every state transition and every refused action **MUST**
be logged to the error log with the stable, greppable prefix `[wsrep-acf]` and
reflected in `performance_schema.wsrep_async_failover_status`.

**NFR5 (No regression).** With the feature `OFF` there **MUST** be no measurable
behavioural change versus the baseline build; the existing ACF, GR, and galera
test suites **MUST** continue to pass.

**NFR6 (Resource).** The coordinator **MUST** use at most one background thread per
node and **MUST** sleep on a condition variable between checks (no busy polling).

**NFR7 (Shutdown).** Server shutdown **MUST** join the coordinator thread within a
bounded time; a hung source connection **MUST NOT** block shutdown beyond the
connect timeout.

**NFR8 (Compatibility).** The change **MUST NOT** alter any on-the-wire protocol,
replication event format, persisted data-dictionary object, or Galera
provider/protocol version. See
[Compatibility & Bugs](WL_PXC-5201_Compatibility_and_Bugs.md) and HLD §5–§7 for
the proofs.

---

## NON REQUIREMENTS

**NR1.** Replacing Galera's intra-cluster synchronous replication.

**NR2.** Conflict resolution or active/active async topologies between
datacenters.

**NR3.** Automatic provisioning of the replication user / credentials or of the
channel definition — the DBA still creates the replication account and runs
`CHANGE REPLICATION SOURCE` once per DR node.

**NR4.** Cross-version GTID translation. Both clusters are expected to run a PXC
release containing PXC-5201 (or at least GTID mode `ON` with
`SOURCE_AUTO_POSITION=1`).

**NR5.** Independent WAN topology probing. Reachability is derived from each
cluster's own `wsrep_incoming_addresses`, not discovered by probing.

**NR6.** Recovery from simultaneous loss of quorum in the DR cluster — with no
primary component the coordinator intentionally does nothing (FR24).

---

## CONSTRAINTS & ASSUMPTIONS

* **AS1.** `gtid_mode=ON` and `enforce_gtid_consistency=ON` on all nodes of both
  clusters.
* **AS2.** The managed async channel is defined (with credentials) on every node
  of the receiver cluster; only its *running* is automated.
* **AS3.** The replication user exists on the primary cluster and is usable from
  any primary node (standard PXC assumption — accounts replicate via Galera).
* **AS4.** Both clusters run a PXC build containing PXC-5201.
* **AS5.** WAN connectivity between the published source addresses and the DR
  cluster exists.

---

## ACCEPTANCE CRITERIA

| Criterion | Requirement(s) | Verified by | Status |
|-----------|----------------|-------------|--------|
| Variables/defaults, coordinator start/stop, lone-node election, P_S table | FR20–23, NFR5 | `galera.pxc_5201_basic` | shipped |
| `GaleraCluster` managed type accepted; `GroupReplication` still works; garbage rejected | FR10 | `galera.pxc_5201_managed_type` | shipped |
| GTID gap/errant/in-doubt blocks in `ENFORCE`, warns in `WARN`, skips in `OFF` | FR12–15 | `galera.pxc_5201_gtid_consistency` | shipped |
| Cluster's own workload not mis-flagged as errant (circular/active-replica case) | FR16 | `galera.pxc_5201_circular` | shipped |
| DR cluster `super_read_only`; knob-off releases only the managed bit | FR17–19 | `galera.pxc_5201_super_read_only` | shipped |
| ACF candidate list add/prune reconciliation (`BOTH` mode) | FR7,FR8,FR10 | `galera.pxc_5201_source_list` | shipped |
| Feature OFF == baseline; no coordinator thread | FR21, NFR5 | `galera.pxc_5201_disabled_noop` | shipped |
| Kill active DR replica → another DR node resumes | FR3,FR4,FR5 | `galera.pxc_5201_receiver_failover` | follow-up (`big_test`, 2-cluster) |
| Kill connected primary source → connection relocates | FR8,FR9,FR11 | `galera.pxc_5201_source_failover` | follow-up (`big_test`, 2-cluster) |
