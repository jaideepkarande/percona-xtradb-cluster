# PXC-5201 — Requirements

Normative requirements for *Native Automatic Asynchronous Replication Failover
for Multi-Datacenter PXC Clusters*. Keywords **MUST**, **SHOULD**, **MAY** are
used per RFC 2119.

Each requirement is tagged so it can be traced to the design
(`low_level_design.md`) and the tests (`testing_plan.md`).

---

## 1. Functional requirements

### 1.1 Receiver-side failover (Scenario A — DR replica node failure)

* **FR-A1.** While enabled in `RECEIVER`/`BOTH` mode, the cluster **MUST**
  designate exactly **one** active replica node at any time within a single
  primary component.
* **FR-A2.** The election **MUST** be deterministic and identical on every node
  given the same Galera view, so that nodes do not disagree about who is active.
* **FR-A3.** When the active replica node leaves the cluster (crash, graceful
  shutdown, network partition that excludes it from the primary component), a
  surviving node **MUST** be elected and **MUST** resume the managed channel
  automatically, without operator action.
* **FR-A4.** A newly elected node **MUST** resume replication using GTID
  auto-positioning (`SOURCE_AUTO_POSITION=1`) so no binlog coordinates are lost.
* **FR-A5.** Non-elected nodes **MUST** ensure the managed channel's receiver
  (IO) and applier (SQL) threads are stopped locally, to guarantee single-writer
  semantics for the async link.
* **FR-A6.** When a previously-active node rejoins, it **MUST NOT** take over the
  channel unless it wins the election again; if it does not, it **MUST** stop
  its channel threads.

### 1.2 Source-side failover (Scenario B — primary source node failure)

* **FR-B1.** While enabled in `SOURCE`/`BOTH` mode, the primary cluster **MUST**
  be representable as a *managed source group* of type `GaleraCluster`.
* **FR-B2.** The active replica node **MUST** keep the candidate source list
  (`performance_schema.replication_asynchronous_connection_failover`) aligned
  with the **healthy** (primary-component, synced) members of the primary
  cluster: members that join are added, members that leave are removed.
* **FR-B3.** When the currently-connected source fails, the replica **MUST**
  fail the connection over to another candidate source from the list, using the
  existing ACF reconnect mechanism, without operator action.
* **FR-B4.** The managed-source UDF
  `asynchronous_connection_failover_add_managed` **MUST** accept the new managed
  type string `GaleraCluster` in addition to `GroupReplication`.
* **FR-B5.** The candidate weights **SHOULD** be configurable (primary weight /
  secondary weight) and **SHOULD** prefer keeping the current connection stable
  (no needless flapping when membership is unchanged).

### 1.3 GTID consistency gate

* **FR-G1.** Before the coordinator starts or relocates the replica, it **MUST**
  evaluate GTID consistency of the local node according to
  `wsrep_async_failover_gtid_check`.
* **FR-G2.** The check **MUST** detect: (a) gaps in `gtid_executed`, (b) errant
  transactions (GTIDs from a source set the cluster did not originate /
  receive through the managed channel), and (c) disagreement between the last
  committed `Xid` and the corresponding `GTID_NEXT` across cluster members
  (the historical PXC inconsistency class).
* **FR-G3.** In `ENFORCE` mode a failed check **MUST** prevent the replica from
  starting/relocating and **MUST** be surfaced (error log + status table). In
  `WARN` mode it **MUST** log a warning but proceed. In `OFF` mode the check is
  skipped.
* **FR-G4.** The GTID verdict **MUST** be exposed for observability
  (status variable + Performance Schema table).

### 1.4 Split-brain protection

* **FR-S1.** While enabled in `RECEIVER`/`BOTH` mode with
  `wsrep_async_failover_read_only=ON`, every node of the DR cluster **MUST** be
  placed in `super_read_only`.
* **FR-S2.** The replication applier **MUST** still be able to apply on the DR
  cluster while `super_read_only` is set (applier bypasses read-only).
* **FR-S3.** Toggling `wsrep_async_failover_read_only=OFF` **MUST NOT**
  automatically clear `super_read_only` already established by the DBA through
  other means; the coordinator only manages the bit it set.

### 1.5 Configuration & lifecycle

* **FR-C1.** All behaviour **MUST** be controlled by the `wsrep_async_failover*`
  system variables enumerated in `high_level_specification.md` §5.
* **FR-C2.** The feature **MUST** default to `OFF`; an unconfigured/upgraded
  server behaves exactly as before.
* **FR-C3.** Variables **MUST** be dynamically settable (`SET GLOBAL`) and,
  where relevant, persistable (`SET PERSIST`) and settable from `my.cnf`.
* **FR-C4.** Enabling the feature **MUST** start the coordinator; disabling it
  **MUST** stop the coordinator thread cleanly (no leaked threads, no hang on
  shutdown).
* **FR-C5.** The coordinator **MUST NOT** act on a node that is not in the
  primary component (a non-primary / partitioned node must never start a replica
  or rewrite the source list).

---

## 2. Non-functional requirements

* **NFR-1 (Safety).** The coordinator **MUST NOT** perform blocking or
  long-running SQL inside the Galera view callback (which runs in total order);
  such work **MUST** be handed to a dedicated worker thread.
* **NFR-2 (Determinism).** Given identical views and identical configuration,
  all nodes **MUST** reach the same election decision without inter-node
  messaging beyond what Galera already provides.
* **NFR-3 (Latency).** Failover (detection → new replica running) **SHOULD**
  complete within `wsrep_async_failover_check_interval` + the time for one
  Galera view change + the GTID check, typically a few seconds.
* **NFR-4 (Observability).** Every state transition and every refused action
  **MUST** be logged to the error log with a stable, greppable prefix
  (`[wsrep-acf]`) and reflected in
  `performance_schema.wsrep_async_failover_status`.
* **NFR-5 (No regression).** With the feature `OFF`, there **MUST** be no
  measurable behavioural change versus the baseline build; existing ACF, GR and
  galera test suites **MUST** continue to pass.
* **NFR-6 (Resource).** The coordinator **MUST** use at most one background
  thread per node and **MUST** sleep on a condition variable between checks
  (no busy polling).
* **NFR-7 (Shutdown).** Server shutdown **MUST** join the coordinator thread
  within a bounded time.

---

## 3. Constraints & assumptions

* **AS-1.** `gtid_mode=ON` and `enforce_gtid_consistency=ON` on all nodes of
  both clusters.
* **AS-2.** The managed async channel is defined (with credentials) on every
  node of the receiver cluster; only the *running* of it is automated.
* **AS-3.** The replication user exists on the primary cluster and is usable
  from any primary node (standard PXC assumption — accounts replicate via
  Galera).
* **AS-4.** Both clusters run a PXC build containing PXC-5201.
* **AS-5.** WAN connectivity between the published source addresses and the DR
  cluster exists; address discovery uses each cluster's
  `wsrep_incoming_addresses`.

---

## 4. Acceptance criteria (high level)

| Criterion | Verified by | Status |
|-----------|-------------|--------|
| Variables/defaults, coordinator start/stop, election of lone node, P_S table | `galera.pxc_5201_basic` | shipped |
| `GaleraCluster` managed type accepted; `GroupReplication` still works | `galera.pxc_5201_managed_type` | shipped |
| GTID gap/errant/in-doubt blocks in `ENFORCE`, warns in `WARN`, skips in `OFF` | `galera.pxc_5201_gtid_consistency` | shipped |
| DR cluster is `super_read_only`; disabling the knob releases only the managed bit (FR-S3) | `galera.pxc_5201_super_read_only` | shipped |
| ACF candidate source list add/prune reconciliation (`BOTH` mode) | `galera.pxc_5201_source_list` | shipped |
| Feature OFF == baseline behaviour; no coordinator thread | `galera.pxc_5201_disabled_noop` | shipped |
| Killing the active DR replica node promotes another DR node and replication resumes | `galera.pxc_5201_receiver_failover` | follow-up (`big_test`, 2-cluster) |
| Killing the connected primary source moves the connection to a surviving primary node | `galera.pxc_5201_source_failover` | follow-up (`big_test`, 2-cluster) |
| Status visible in `performance_schema.wsrep_async_failover_status` | every test above | shipped |

The destructive two-cluster criteria (`receiver_failover`, `source_failover`)
and the cross-datacenter `wsrep_incoming_addresses` pull they exercise are the
documented follow-up (see `low_level_design.md` §8.1). See `testing_plan.md` for
the full matrix.
