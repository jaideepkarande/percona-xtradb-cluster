# WL#PXC-5201: Native Automatic Asynchronous Replication Failover for Multi-Datacenter PXC Clusters

**Affects:** Percona XtraDB Cluster 8.0 / 8.4 — **Status:** Complete

| Description | Requirements | Dependent Tasks | High Level Architecture | Low Level Design | Compatibility & Bugs |
|-------------|--------------|-----------------|-------------------------|------------------|----------------------|

> This document set is written in the same format as the Oracle MySQL Worklogs
> that PXC-5201 builds on — `WL#12649`, `WL#14019`, `WL#14020` (*Automatic
> connection failover for Async Replication Channels*, steps I–III). Each tab of
> the original worklog corresponds to one file:
> [Description](WL_PXC-5201_Description.md) ·
> [Requirements](WL_PXC-5201_FR.md) ·
> [High Level Architecture](WL_PXC-5201_HLD.md) ·
> [Low Level Design](WL_PXC-5201_LLD.md) ·
> [Compatibility & Bugs](WL_PXC-5201_Compatibility_and_Bugs.md).

---

## EXECUTIVE SUMMARY

This worklog makes Percona XtraDB Cluster (PXC) **natively** manage the classic
(asynchronous) MySQL replication link that connects two independent Galera
clusters in an Active / Disaster-Recovery topology, so that the link survives the
loss of either endpoint **without any external orchestrator or DBA
intervention**.

Two independent Galera clusters (D1 = primary/active, D2 = DR/passive) are linked
by a single classic asynchronous replication channel: one *source* node in D1
streams its binary log to one *replica* node in D2. Inside each datacenter Galera
provides synchronous, self-healing HA; **between** the datacenters that single
async channel is a single point of failure.

After this worklog is implemented:

* If the **replica** node in D2 fails, PXC automatically **re-elects** another D2
  node and resumes the channel (*receiver failover*, Scenario A).
* If the **source** node in D1 fails, PXC automatically moves the replica's
  connection to a surviving D1 node (*source failover*, Scenario B), keeping the
  candidate-source list in step with D1's live Galera membership.
* The DR cluster is automatically held in `super_read_only` to remove any
  split-brain write window.
* GTID consistency is verified before the replica is (re)started.

The mechanism is built on top of the existing MySQL *Asynchronous Connection
Failover* (ACF) machinery delivered by `WL#12649` / `WL#14019` / `WL#14020`
rather than re-implementing it: PXC supplies the **Galera-aware policy** (who is
the replica, which sources are healthy, is the GTID state safe) and lets the
proven server mechanism perform the mechanics. The feature is **OFF by default**;
an unconfigured or upgraded server behaves exactly as in previous releases.

---

## TERMINOLOGY

Terms are inherited from the upstream worklogs and extended for the cluster case.

**Asynchronous replication roles**

* **sender / source:** the endpoint that sends binlog data (a node in the primary
  cluster D1).
* **receiver / replica:** the endpoint that receives data (a node in the DR
  cluster D2).
* **managed channel:** the single classic async channel that this worklog
  automates the *running* of. Its definition (credentials, `SOURCE_HOST`, …) is
  still created once by the DBA on every DR node.
* **sender list / candidate source list:** the list of potential sources on the
  receiver
  (`performance_schema.replication_asynchronous_connection_failover`), each with
  connection details and a weight. On source failure the receiver connects to the
  highest-weight surviving candidate.
* **failover weight (priority):** a number 1–100 (100 highest) that orders
  candidate sources. PXC-5201 exposes *primary weight* and *secondary weight* for
  a managed cluster.
* **managed source group:** a group of sources registered together via
  `asynchronous_connection_failover_add_managed()`, so the candidate list is
  auto-maintained. Upstream supported the managed type `GroupReplication`; this
  worklog adds **`GaleraCluster`**.

**Cluster / PXC terms**

* **Galera view / membership change:** the authoritative, totally-ordered signal
  Galera delivers to every node whenever the cluster membership changes. It is the
  trigger for this worklog's coordinator.
* **primary component:** the quorum-holding partition of a Galera cluster. The
  coordinator never acts outside a primary component (core split-brain guard).
* **active replica (elected node):** the single DR node that is currently running
  the managed channel, chosen by a deterministic election on the Galera view.
* **coordinator:** the *Cluster-Aware Asynchronous Replication Failover
  Coordinator* (`Wsrep_async_failover`) introduced by this worklog — one
  background thread per node.
* **GTID gate:** the pre-start consistency check (gap / errant / in-doubt) run by
  the coordinator before it starts or relocates the replica.

**Coordinator role (`wsrep_async_failover_mode`)**

* `OFF` — coordinator does nothing.
* `RECEIVER` — this is the DR cluster; elect an active replica, keep it running,
  enforce `super_read_only`.
* `SOURCE` — this is the primary cluster; be representable as a managed
  `GaleraCluster` source group.
* `BOTH` — symmetric (e.g. bi-directional DR / co-located test topology).

---

## MOTIVATION

Multi-datacenter PXC deployments are common: each datacenter is an
internally-HA Galera cluster, and the two are linked by classic asynchronous
replication for cross-region DR. Today that inter-cluster link is fragile:

* **Replica failure (Scenario A).** If the active DR replica node crashes or
  leaves its Galera cluster, replication into the DR site simply stops until a
  human runs `CHANGE REPLICATION SOURCE` + `START REPLICA` on another DR node.
* **Source failure (Scenario B).** If the active source node in D1 crashes,
  replication stops. Stock MySQL ACF can move the connection to another source,
  **but the candidate list must be built and maintained by hand** — PXC does not
  derive it from its own cluster membership.

The result is that customers bolt on external tooling (Orchestrator, custom cron
jobs, ProxySQL scripts) to paper over a gap that PXC is in the best position to
close, because PXC already receives an authoritative, totally-ordered membership
signal (the Galera view) on every node.

The driver of this worklog is to make PXC keep the inter-cluster async link alive
by binding it to Galera membership — eliminating the manual sender-list
maintenance and the manual replica re-pointing, while adding the safety rails a
cluster needs (single active replica, `super_read_only` on DR, GTID verification)
that a bare MySQL replica does not enforce on its own.

---

## USER STORIES

**1. Replica node failure — automatic re-election (Scenario A)**

> As a DBA I run one async channel from primary cluster D1 into DR cluster D2. If
> the D2 node currently running the channel dies, I want another D2 node to take
> over and resume replication automatically.

```
   DR cluster D2 [N1*, N2, N3]              DR cluster D2 [N2*, N3]
   N1* --async--> (running)                 N1  x (crashed / left)
   N2     (stopped)          N1 leaves      N2* --async--> (running)
   N3     (stopped)          ----------->   N3     (stopped)

   The elected active replica N1 fails. The next Galera view promotes N2,
   which verifies GTID state and resumes the channel with AUTO_POSITION.
   Exactly one D2 node runs the channel at any time.
```

**2. Source node failure — automatic connection failover (Scenario B)**

> As a DBA I want the DR replica to stay connected to *some* healthy D1 node. If
> the connected D1 source dies, the replica should reconnect to a surviving D1
> node without me editing any list.

```
   Primary D1 [S1, S2, S3]                  Primary D1 [S2, S3]
   S1(connected) --async--> [R (DR)]        S1 x        [R (DR)]
   S2                                       S2(connected)--async--^
   S3            S1 crashes  ----------->    S3
   candidate list {S1*,S2,S3}               candidate list {S2*,S3}

   ACF (fed by Galera membership) fails the connection over to S2 and the
   coordinator prunes the departed S1 from the candidate list.
```

**3. New source node joins the primary — automatic list growth**

> As a DBA, when I add a node to the primary cluster D1, I want it to become a
> failover candidate automatically.

```
   Primary D1 [S1, S2]        S3 joins      Primary D1 [S1, S2, S3]
   candidate list {S1,S2}     ---------->   candidate list {S1,S2,S3}

   The coordinator reconciles the ACF candidate list against D1's live
   Galera membership; the new node is added as a candidate.
```

**4. Accidental write to DR — prevented by design**

> As a DBA I want the whole DR cluster to reject application writes so a
> mis-pointed client cannot diverge DR from the primary.

```
   Application --write--> [DR cluster]   =>  ER_OPTION_PREVENTS_STATEMENT
   Replication applier ---------------->     applies (bypasses super_read_only)

   With wsrep_async_failover_read_only=ON every DR node is super_read_only;
   only the replication applier may write.
```

**5. Unsafe GTID state — replica start refused**

> As a DBA I want PXC to refuse to (re)start the DR replica if it detects errant
> transactions or an in-doubt state, rather than silently corrupting DR.

```
   Elected DR node has errant GTIDs   =>  gtid_check=ENFORCE:
                                          channel NOT started,
                                          verdict ERRANT surfaced in P_S +
                                          error log, retried once state clears.
```

---

## HIGH-LEVEL PICTURE

```
        Datacenter 1 (PRIMARY / Active)            Datacenter 2 (DR / Passive)
   ┌─────────────────────────────────┐        ┌─────────────────────────────────┐
   │  D1-N1   D1-N2   D1-N3           │        │  D2-N1   D2-N2   D2-N3           │
   │   ▲────────▲───────▲ Galera sync │        │   ▲────────▲───────▲ Galera sync │
   └───┼─────────────────────────────┘        └───┼─────────────────────────────┘
       │ async SOURCE = elected D1 node            │ async REPLICA = elected D2 node
       └───────────────  asynchronous replication  ───────────────────┘
       mode = SOURCE                               mode = RECEIVER
       (managed GaleraCluster group)               (super_read_only, GTID gate,
                                                    single elected active replica)
```

See [Requirements](WL_PXC-5201_FR.md) for the normative requirement list,
[High Level Architecture](WL_PXC-5201_HLD.md) for the architecture including the
**Cross-Version / Upgrade / Protocol** sections, and
[Low Level Design](WL_PXC-5201_LLD.md) for the implementation.
