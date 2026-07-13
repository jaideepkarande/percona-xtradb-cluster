# PXC-5201 — Testing Plan

Tests live in the `galera` MTR suite (`mysql-test/suite/galera/`) because the
feature requires the WSREP provider. Multi-datacenter scenarios are emulated
with two small Galera clusters (or, where a full two-cluster harness is too
heavy for CI, with a wsrep-loaded chain as the existing
`pxc_async_conn_failover.test` does).

All new tests are gated by `--source include/have_wsrep_provider.inc` and, where
crash/kill timing matters, `--source include/big_test.inc`.

---

## 1. Test matrix

| Test name | Requirement(s) | Type | Summary |
|-----------|----------------|------|---------|
| `pxc_5201_basic` | FR-C1..C4, NFR-5 | functional | Variables exist, defaults, dynamic set/persist, coordinator starts/stops, P_S table present. |
| `pxc_5201_managed_type` | FR-B4 | functional | `asynchronous_connection_failover_add_managed` accepts `GaleraCluster`; rejects garbage; `GroupReplication` still works. |
| `pxc_5201_election` | FR-A1,A2,A6 | functional | In a multi-node wsrep set, exactly one node reports `IS_ACTIVE_REPLICA=YES`; deterministic across nodes. |
| `pxc_5201_receiver_failover` | FR-A3,A4,A5 | functional/destructive | Kill the active replica node; another node resumes the channel and data flows. |
| `pxc_5201_source_failover` | FR-B1,B2,B3,B5 | functional/destructive | Source list tracks primary membership; killing the connected source relocates the connection. |
| `pxc_5201_super_read_only` | FR-S1,S2,S3 | functional | DR cluster is `super_read_only`; applier still applies; disabling the knob releases only the managed bit. |
| `pxc_5201_gtid_consistency` | FR-G1..G4 | functional | Injected gap/errant ⇒ blocked in ENFORCE, warns in WARN, skipped in OFF; verdict visible. |
| `pxc_5201_disabled_noop` | FR-C2, NFR-5 | regression | With feature OFF, behaviour identical to baseline; no coordinator thread. |

Existing tests that must keep passing (regression guard):
`galera.pxc_async_conn_failover`, `galera.pxc_acf_add_delete_managed`,
`galera.galera_as_slave_async_monitor`, the `group_replication.gr_acf_*` suite,
and the `perfschema` enumeration tests (after `--record`).

---

## 2. Detailed test designs

### 2.1 `pxc_5201_basic`
1. `--source include/have_wsrep_provider.inc`.
2. Assert all six `wsrep_async_failover*` variables exist with documented
   defaults (`SELECT ... FROM performance_schema.global_variables`).
3. `SET GLOBAL wsrep_async_failover=ON` → assert
   `performance_schema.threads` shows a `wsrep_async_failover` thread and the
   P_S row reports `ENABLED=YES`.
4. `SET GLOBAL wsrep_async_failover=OFF` → thread gone, `ENABLED=NO`.
5. `SELECT * FROM performance_schema.wsrep_async_failover_status` returns exactly
   one row with the documented columns.
6. Invalid values rejected (`SET GLOBAL wsrep_async_failover_mode='BOGUS'`).

### 2.2 `pxc_5201_managed_type`
1. `SELECT asynchronous_connection_failover_add_managed('c','GaleraCluster','grp','127.0.0.1',PORT,'',80,60)` → succeeds, row visible in
   `..._failover_managed` with `MANAGED_TYPE='GaleraCluster'`.
2. `...add_managed(...,'NotAType',...)` → error
   *"Managed type must be GroupReplication or GaleraCluster."*.
3. `...add_managed(...,'GroupReplication',...)` → still succeeds (no regression).
4. `asynchronous_connection_failover_delete_managed` cleans up.

### 2.3 `pxc_5201_election`
1. Bring up N wsrep-loaded servers acting as one logical DR set.
2. Configure all with `wsrep_async_failover=ON`, `mode=RECEIVER`,
   same channel, define (but don't manually start) the channel.
3. `--let` loop over servers: assert exactly one has
   `IS_ACTIVE_REPLICA=YES` and its `ELECTED_INDEX` equals its own
   `wsrep_local_index`; all others `NO`.

### 2.4 `pxc_5201_receiver_failover` (destructive)
1. Two-cluster emulation: a primary source server and a DR set (≥2 wsrep nodes).
2. Enable feature on DR set; create + auto-start channel; insert on source;
   assert data arrives on DR via the elected node.
3. Identify the active replica node; `--source include/kill_and_restart...`
   or `rpl_stop_server.inc` to remove it.
4. `--let $wait_condition` until another DR node reports `IS_ACTIVE_REPLICA=YES`.
5. Insert more on source; assert it lands on the surviving DR node (replication
   resumed automatically). Verify via `assert_grep` of the error log for the
   `[wsrep-acf] Elected this node ...` line.

### 2.5 `pxc_5201_source_failover` (destructive)
1. Primary cluster = ≥2 wsrep nodes registered as one `GaleraCluster` managed
   group; DR replica connected to primary node A.
2. Wait until the ACF source list
   (`performance_schema.replication_asynchronous_connection_failover`) lists
   both primary members (auto-populated by the coordinator).
3. Stop primary node A; assert
   `replication_connection_configuration.PORT` on the replica moves to node B
   and replication continues (insert on B, read on DR).
4. Assert departed node A is removed from the source list after the next
   refresh.

### 2.6 `pxc_5201_super_read_only`
1. Enable feature `RECEIVER`, `wsrep_async_failover_read_only=ON`.
2. Assert `@@GLOBAL.super_read_only=1` on every DR node and a normal client
   write fails with `ER_OPTION_PREVENTS_STATEMENT`.
3. Insert on source; assert it still applies on DR (applier bypasses read-only).
4. `SET GLOBAL wsrep_async_failover_read_only=OFF`; assert the coordinator
   clears the bit it set; manually-set `super_read_only` (separate sub-case) is
   left untouched.

### 2.7 `pxc_5201_gtid_consistency`
Uses debug sync / a crafted errant GTID (the project already has a helper for
"UDF execution adds errant GTID", see `PXC-4238`).
1. `gtid_check=ENFORCE`: inject an errant GTID on the elected node
   (`SET GTID_NEXT=...; BEGIN; COMMIT;`), force re-election/restart; assert the
   channel is **not** started, verdict `ERRANT` in the P_S table, and the
   `[wsrep-acf] GTID consistency check failed` line is logged.
2. Clear errant; assert verdict returns to `OK` and the channel starts.
3. `gtid_check=WARN`: same injection ⇒ channel starts, warning logged.
4. `gtid_check=OFF`: verdict `SKIPPED`, channel starts.

### 2.8 `pxc_5201_disabled_noop`
1. With `wsrep_async_failover=OFF`, repeat the core of
   `pxc_async_conn_failover` and assert identical results, no coordinator
   thread, no `super_read_only` side effects.

---

## 3. How to run

```bash
cd mysql-test

# single new test
./mysql-test-run.pl --suite=galera --do-test=pxc_5201_basic

# all new feature tests
./mysql-test-run.pl --suite=galera --do-test=pxc_5201

# regression guard
./mysql-test-run.pl --suite=galera --do-test=pxc_async_conn_failover
./mysql-test-run.pl --suite=galera --do-test=pxc_acf_add_delete_managed

# re-record perfschema enumeration after adding the P_S table
./mysql-test-run.pl --suite=perfschema --record table_schema schema \
    information_schema all_tests dml_handler
```

The big/destructive tests additionally need `--big-test`.

---

## 4. CI considerations

* The destructive 2-cluster tests are tagged `big_test` so they only run in the
  extended pipeline, not on every push.
* Each test is self-contained: it enables the feature at the start and fully
  resets it (`SET GLOBAL wsrep_async_failover=OFF`, delete managed/source rows,
  `CHANGE REPLICATION SOURCE ... SOURCE_CONNECTION_AUTO_FAILOVER=0`, clear
  `super_read_only`) at the end, so suite ordering is irrelevant.
* `call mtr.add_suppression(...)` is used for the expected
  `[wsrep-acf]` informational lines and the standard "master's UUID has changed"
  warning seen when a channel relocates.

---

## 5. Result-file maintenance

Because PXC-5201 adds one Performance Schema table, the deterministic
enumeration result files listed in `low_level_design.md` §10.1 are re-recorded
as part of landing the change. The diffs are additive (one table / its column
block) and reviewed to contain nothing but the new table.
