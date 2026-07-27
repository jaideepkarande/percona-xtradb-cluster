# WL#PXC-5201: Compatibility Proofs & Bugs Fixed

**Affects:** Percona XtraDB Cluster 8.0 / 8.4 — **Status:** Complete

| [Description](WL_PXC-5201_Description.md) | [Requirements](WL_PXC-5201_FR.md) | Dependent Tasks | [High Level Architecture](WL_PXC-5201_HLD.md) | [Low Level Design](WL_PXC-5201_LLD.md) | **Compatibility & Bugs** |
|---|---|---|---|---|---|

This document is the dedicated home for two things the upstream worklogs keep in
their HLD sections 5–7 and post-push notes:

1. **Bugs fixed** during PXC-5201 development (§1, detailed write-ups in §5).
2. **Proof that PXC-5201 introduces no cross-version, upgrade/downgrade, or
   protocol issues** (§2 / §3 / §4).

Each proof below is grounded in the delivered code (file:line references). The
matching **SECURITY CONTEXT** proof — no new privilege bypass, no credential
handling, no SQL-injection path — lives in
[HLD §4](WL_PXC-5201_HLD.md#4-security-context), also fully code-cited. The
implementation reviewed here is `sql/wsrep_async_failover.{h,cc}`,
`storage/perfschema/table_wsrep_async_failover_status.cc`, the two ACF UDF files,
`sql/sys_vars.cc` and `sql/wsrep_var.cc`.

The compatibility claims mirror the wording the Oracle worklogs use — *"There is
no impact on cross-version replication."* / *"There are no changes in any
protocol."* — but here each claim is backed by concrete source-tree evidence.

---

## 1. BUGS FIXED (SUMMARY)

These defects were found and fixed while developing and testing PXC-5201.
Detailed write-ups are in §5.

| # | Bug | Fix | Regression guard |
|---|-----|-----|------------------|
| B1 | Circular / active-replica case: the cluster's **own** replicated workload was mis-flagged as an **errant** transaction, so the GTID gate refused to (re)start the replica in `ENFORCE`. | Subtract **both** `@@server_uuid` **and** the cluster-wide wsrep GTID UUID (`wsrep_cluster_state_uuid`) as local-origin, not just `server_uuid`. | `galera.pxc_5201_circular` |
| B2 | GTID gate could not parse an interval whose upper bound was `2^63-1` — the GTID-set parser rejects it as *"Malformed GTID set"*. | Use `9223372036854775806` (`2^63-2`) as the interval upper bound. | `galera.pxc_5201_gtid_consistency` |
| B3 | Before the managed channel had received anything, errant-set subtraction produced a non-empty remainder, flagging a clean node's own GTIDs as errant. | Early-return verdict `OK` when the channel's received set is empty (no source baseline to gate against). | `galera.pxc_5201_gtid_consistency` |
| B4 | `asynchronous_connection_failover_add_managed()` rejected every managed type except `GroupReplication`, and enforced a UUID-format check on the managed name — blocking a Galera cluster label. | Accept `GaleraCluster` (length 13); gate the UUID check on the `GroupReplication` type only. | `galera.pxc_5201_managed_type` |
| B5 | `asynchronous_connection_failover_delete_managed()` unconditionally rejected any managed name that was not a valid UUID, so `GaleraCluster` rows could not be deleted. | Remove the UUID-only check; let the `channel + managed name` row lookup decide existence. | `galera.pxc_5201_managed_type` |
| B6 | Source-list refresh was gated on `SOURCE` mode, i.e. it would run on the primary cluster — which holds no local ACF tables — instead of the receiver. | Run `refresh_source_list()` from `process_receiver()` on the receiver's elected node, independent of the mode flags. | `galera.pxc_5201_source_list` |
| B7 | The coordinator could emit noisy *"server is not configured as replica"* errors and could start a channel that would lose binlog coordinates. | Issue `START REPLICA` only when the channel has a configured source **and** `SOURCE_AUTO_POSITION=1` (FR4). | `galera.pxc_5201_basic` |
| B8 | Disabling the read-only knob could clear a `super_read_only` bit the DBA had set independently. | Track ownership: release only the bit the coordinator itself set (FR19). | `galera.pxc_5201_super_read_only` |

Fixes B1–B8 landed across commits `7bf00676283` (*PXC-5201*) and `4b9e60c4089`
(*WL test cases and fix circular replication*).

---

## 2. CROSS-VERSION REPLICATION — PROOF OF NO IMPACT

> **Claim:** There is no impact on cross-version replication.

**Evidence**

1. **No new / changed binary-log event.** PXC-5201 adds no event type, no event
   field, and no replication payload. The coordinator only issues standard
   `START REPLICA` / `STOP REPLICA` / `SET GLOBAL super_read_only` and read-only
   `performance_schema` queries. A replica running the feature streams exactly the
   event set a manually-configured replica of the same server version would. Grep
   confirms no change under `libbinlogevents/`, `sql/log_event*`, or
   `sql/rpl_*` event code from this worklog's commits.

2. **Control statements never cross the wire.** The coordinator's worker THD is
   created with `thd->variables.wsrep_on = false`
   (`sql/wsrep_async_failover.cc:370`), and every new system variable is declared
   `NOT_IN_BINLOG` (`sql/sys_vars.cc`, PXC-5201 block), so `START/STOP REPLICA`,
   `SET GLOBAL super_read_only`, and any variable change are **not** binlogged and
   **not** Galera-replicated. No peer node — of any version — ever observes them.

3. **The new managed type is data, not protocol.** `GaleraCluster` is stored as
   free text in the existing
   `mysql.replication_asynchronous_connection_failover_managed.Managed_type`
   column. No column, type, or constraint changed (see §3). A peer never receives
   this value over replication; it is local ACF configuration.

4. **Transport is unchanged stock replication.** The link is ordinary GTID-based
   asynchronous replication with `SOURCE_AUTO_POSITION=1`. MySQL's existing
   cross-version replication rules therefore apply unchanged — PXC-5201 neither
   tightens nor relaxes them, because it adds nothing to the replication stream.

**Conclusion:** a PXC-5201 node can act as source or replica against a different
server version exactly as a non-PXC-5201 node of the same base version would.
There is no cross-version replication impact.

---

## 3. UPGRADE / DOWNGRADE — PROOF OF NO ISSUE

> **Claim:** No upgrade step is required; no data-dictionary or system-table
> schema change is introduced.

**Evidence**

1. **No persisted schema change.** Contrast `WL#12649`, which added the
   `SOURCE_CONNECTION_AUTO_FAILOVER` column to `slave_master_info`. PXC-5201 adds
   **no** column to any `mysql.*` system table and **no** new persisted table. The
   file list of both PXC-5201 commits contains no `.sql` bootstrap file, no
   `sql/dd/` change, and no `scripts/*` system-table DDL.

2. **The new Performance Schema table is in-memory and code-defined.**
   `wsrep_async_failover_status` is declared inline as a `Plugin_table` with
   `ENGINE=PERFORMANCE_SCHEMA`, registered via its `m_share` in
   `storage/perfschema/pfs_engine_table.cc`, and produces a single row from
   `Wsrep_async_failover::instance().snapshot()` on each read
   (`storage/perfschema/table_wsrep_async_failover_status.cc`). It is read-only
   (`&pfs_readonly_acl`, `write_row=nullptr`) and has no on-disk row data.

3. **P_S data-dictionary version bump (required, automatic — corrects an earlier
   draft of this doc).** Adding any P_S table changes the set of P_S table
   definitions the server publishes into the data dictionary, so `PFS_DD_VERSION`
   is bumped `80407 → 80408` (`storage/perfschema/pfs_dd_version.h`, kept
   `≤ MYSQL_VERSION_ID=80408` per the `static_assert` in `ha_perfschema.cc`). On
   first start with the new binary the server notices its compiled `PFS_DD_VERSION`
   is newer than the on-disk DD and **recreates the performance_schema DD
   definitions automatically** — no operator `mysql_upgrade` step, no user-data
   migration, no risk to user tables. The `perfschema.dd_version_check` guard test
   enforces this: its published-schema signature and `PFS_DD_VERSION` row were
   updated in the same change (new signature
   `88ce401b95b226dcc95ba90f80bdc279efa4d64f430b9d9ac5fe76e886962d84`). This is the
   standard, well-trodden path for every P_S table addition and does not affect
   replication or the on-the-wire protocols (§2, §4).

4. **New sysvars default to OFF/empty (FR21).** A freshly upgraded server behaves
   identically to the previous release until a DBA opts in. There is no implicit
   behaviour change on upgrade.

5. **Rolling upgrade is safe.** With the feature off, a mixed-version PXC cluster
   is unaffected and nodes can be upgraded one at a time. Even with the feature on,
   the coordinator acts only within a primary component (FR24) and never
   replicates its actions (§2), so an upgraded and a not-yet-upgraded node cannot
   disagree over the wire.

6. **Downgrade caveat (documented, not a corruption risk).** The only persisted
   artifact a DBA can create is a candidate row of managed type `GaleraCluster` in
   the ACF managed table. A pre-PXC-5201 binary's ACF monitor only understands
   `GroupReplication` and simply does not act on a `GaleraCluster` row — it is
   inert data, not a crash or corruption. Recommended clean downgrade steps:
   * `asynchronous_connection_failover_delete_managed()` any `GaleraCluster` rows;
   * clear a coordinator-held `super_read_only`;
   * remove the `wsrep_async_failover*` options from `my.cnf` (an older binary
     rejects unknown `--wsrep_async_failover*` options at startup — the standard
     behaviour for any removed option, and the reason this cleanup is listed).

**Conclusion:** for the operator, upgrade is a no-op beyond installing the binary
— the only data-dictionary effect (the P_S table's `PFS_DD_VERSION` bump) is
applied automatically by the server on first start, with no `mysql_upgrade` step
and no user-data migration. Downgrade needs only the documented cleanup of opt-in
configuration. No `mysql.*` user/system-table schema change is involved.

---

## 4. PROTOCOL — PROOF OF NO CHANGE

> **Claim:** There are no changes in any protocol.

**Evidence**

1. **Zero Galera / wsrep changes.** Both PXC-5201 commits touch **no** file under
   `percona-xtradb-cluster-galera/`, `wsrep-lib/`, or the wsrep-API submodule, and
   they do **not** move the galera submodule gitlink. Verified:

   ```
   $ git show --stat 7bf00676283 --name-only | grep -E 'percona-xtradb-cluster-galera|wsrep-lib|wsrep-API'
     (no output)
   $ git show --stat 4b9e60c4089 --name-only | grep -E 'percona-xtradb-cluster-galera|wsrep-lib|wsrep-API'
     (no output)
   ```

   The group-communication protocol version constants are therefore untouched
   (e.g. `gcs/src/gcs_act_proto.hpp: #define GCS_PROTO_MAX 5`). The GCS/EVS
   handshake and the replication/application provider protocol versions are
   unchanged, so a PXC-5201 node and a pre-PXC-5201 node negotiate exactly the same
   provider protocol and can co-exist in one cluster.

2. **No client/server wire-protocol change.** No new command packet, capability
   flag, handshake field, or `SHOW STATUS` protocol-level addition. The new
   observability surface is entirely SQL-level: one in-memory Performance Schema
   table and three server status variables.

3. **No replication-protocol change** — established in §2 (no new/changed binlog
   event; control statements are not replicated).

**Conclusion:** every protocol — Galera group communication, wsrep provider,
MySQL client/server, and MySQL replication — is byte-for-byte unchanged by
PXC-5201.

---

## 5. DETAILED BUG WRITE-UPS

### 5.1. B1 — Cluster's own workload mis-flagged as errant (circular replication)

**Symptom.** With `wsrep_async_failover_gtid_check=ENFORCE`, once the managed
channel had delivered any transaction, the GTID gate returned verdict `ERRANT` and
refused to (re)start the replica — even on a perfectly consistent cluster. This
broke the active-replica case and any circular/bi-directional topology.

**Root cause.** Errant detection computed
`errant = gtid_executed − received − local_origin`, but `local_origin` was only
`@@server_uuid`. In PXC every replicated write is stamped under the **cluster-wide
wsrep GTID UUID** (`wsrep_cluster_state_uuid`), which is distinct from any node's
`server_uuid`. So the cluster's own committed workload fell outside both
`received` and `local_origin` and was counted as errant.

**Fix.** Treat **both** `@@server_uuid` **and** `wsrep_cluster_state_uuid` as
local-origin and subtract both. (There is no `wsrep_gtid_mode` in 8.4; the cluster
UUID is always present and always distinct from the node UUIDs.) See
[LLD §7.3](WL_PXC-5201_LLD.md). Regression guard: `galera.pxc_5201_circular`
(commit `4b9e60c4089`).

### 5.2. B2 — "Malformed GTID set" on the interval upper bound

**Symptom.** GTID-gate SQL that built an interval up to the absolute maximum
(`2^63-1`) failed with *"Malformed GTID set"*.

**Root cause.** The GTID-set parser rejects `2^63-1` as an interval endpoint.

**Fix.** Use `9223372036854775806` (`2^63-2`) as the upper bound.

### 5.3. B3 — Empty received-set false positive

**Symptom.** On a node where the channel had not yet received anything, the gate
reported `ERRANT` for the node's own cluster GTIDs.

**Root cause.** With an empty `received` set there is no source baseline to gate
against, yet subtraction still yielded a non-empty remainder.

**Fix.** Early-return verdict `OK` when the channel's received set is empty
(nothing to gate). See [LLD §7.3](WL_PXC-5201_LLD.md).

### 5.4. B4 / B5 — Managed-type and managed-name UDF validation

**Symptom.** `asynchronous_connection_failover_add_managed(..., 'GaleraCluster',
...)` was rejected; and a `GaleraCluster` row could not be deleted because
`..._delete_managed()` demanded a UUID managed name.

**Root cause.** `add_managed_init()` accepted only the 16-char `"GroupReplication"`
and always ran a UUID-format check on the managed name;
`delete_managed_init()` ran the same unconditional UUID check.

**Fix.**
* `add_managed`: accept the 13-char `"GaleraCluster"` as well, and gate the UUID
  check on the `GroupReplication` type only
  (`sql/rpl_async_conn_failover_add_managed_udf.cc`).
* `delete_managed`: remove the UUID-only check entirely; the `channel + managed
  name` row lookup decides existence
  (`sql/rpl_async_conn_failover_delete_managed_udf.cc`).

See [LLD §8.2](WL_PXC-5201_LLD.md). Regression guard:
`galera.pxc_5201_managed_type`.

### 5.5. B6 — Source-list refresh ran on the wrong cluster

**Symptom.** In an earlier draft the candidate-list reconciliation never ran,
because it was gated on `SOURCE` mode.

**Root cause.** The ACF managed-group definition and candidate list live on the
**replica** cluster. Gating refresh on `SOURCE` mode meant the primary cluster
(which has no local ACF tables) was the one asked to run it.

**Fix.** Call `refresh_source_list()` from `process_receiver()` on the receiver's
elected node, independent of the mode flags. See [LLD §8](WL_PXC-5201_LLD.md).
Regression guard: `galera.pxc_5201_source_list`.

### 5.6. B7 — Unsafe / noisy `START REPLICA`

**Symptom.** Enabling the feature before the channel was provisioned produced
*"server is not configured as replica"* errors; a channel without
auto-positioning could be started and lose binlog coordinates.

**Fix.** `process_receiver()` issues `START REPLICA` only when the channel has a
configured source **and** `SOURCE_AUTO_POSITION=1`, read together from
`replication_connection_configuration` (FR4). See [LLD §5](WL_PXC-5201_LLD.md).

### 5.7. B8 — `super_read_only` ownership

**Symptom.** Turning `wsrep_async_failover_read_only=OFF` could clear a
`super_read_only` the DBA had set independently.

**Fix.** The coordinator records whether *it* set the bit and, on knob-off,
releases only that bit; a DBA-set `super_read_only` is left untouched (FR19). See
[LLD §6](WL_PXC-5201_LLD.md). Regression guard: `galera.pxc_5201_super_read_only`.

---

## 6. VERIFICATION

The compatibility claims and bug fixes are exercised by the `galera` MTR suite:

```
galera.pxc_5201_basic            galera.pxc_5201_gtid_consistency
galera.pxc_5201_managed_type     galera.pxc_5201_super_read_only
galera.pxc_5201_source_list      galera.pxc_5201_circular
galera.pxc_5201_disabled_noop
```

Plus the regression guard that the feature is a true no-op when off
(`pxc_5201_disabled_noop`, NFR5) and the existing ACF / GR / galera suites, which
continue to pass unchanged.
