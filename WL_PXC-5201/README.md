# WL#PXC-5201 — Worklog document set

*Native Automatic Asynchronous Replication Failover for Multi-Datacenter PXC
Clusters.*

These files reproduce the Oracle MySQL Worklog tab layout (Description /
Requirements / High Level Architecture / Low Level Design), the same format used
by the upstream worklogs this feature builds on — `WL#12649`, `WL#14019`,
`WL#14020`. A fifth document collects the bug fixes and the cross-version /
upgrade / protocol compatibility proofs.

**These are verification documents, not just formatting.** The Oracle worklog's
standard sections — SECURITY CONTEXT, CROSS-VERSION REPLICATION, UPGRADE/DOWNGRADE,
PROTOCOL, FAILURE MODEL — exist to force a proof that the *delivered code* does not
carry that class of issue. Each such section here is grounded in the actual
implementation with `file:line` references, and any real caveat is disclosed
rather than glossed over.

| Tab / doc | File | Corresponds to |
|-----------|------|----------------|
| Description | [WL_PXC-5201_Description.md](WL_PXC-5201_Description.md) | Executive summary, terminology, motivation, user stories |
| Requirements | [WL_PXC-5201_FR.md](WL_PXC-5201_FR.md) | FR / NFR / non-requirements / acceptance criteria |
| High Level Architecture | [WL_PXC-5201_HLD.md](WL_PXC-5201_HLD.md) | Architecture + **§4 Security Context**, **§5 Cross-version**, **§6 Upgrade/Downgrade**, **§7 Protocol**, **§8 Failure Model** — all code-cited |
| Low Level Design | [WL_PXC-5201_LLD.md](WL_PXC-5201_LLD.md) | Component map, election, GTID gate, source list, P_S table |
| Compatibility & Bugs | [WL_PXC-5201_Compatibility_and_Bugs.md](WL_PXC-5201_Compatibility_and_Bugs.md) | **Bugs fixed** + code-cited **proof of no cross-version / upgrade / protocol impact** |

Code reviewed for the proofs: `sql/wsrep_async_failover.{h,cc}`,
`storage/perfschema/table_wsrep_async_failover_status.cc`,
`sql/rpl_async_conn_failover_{add,delete}_managed_udf.cc`, `sql/sys_vars.cc`,
`sql/wsrep_var.cc`.

**Implementation commits:** `7bf00676283` (*PXC-5201*), `4b9e60c4089` (*WL test
cases and fix circular replication*).

The engineering write-up and the formal `DESIGN/*.md` docs remain the primary
in-tree references; this set is the worklog-formatted presentation layer over
them.
