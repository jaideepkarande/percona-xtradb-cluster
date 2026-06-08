# Provoke an inconsistency on node_2 by introducing two local deletions that
# are not replicated and then queuing enough UPDATEs from node_1 to trigger
# applier conflicts.
#
# Expects: node_1 and node_2 connections to exist; table t1 with 8 rows;
#          node_1 configured with pc.weight=2 so it can hold quorum alone.
# Leaves:  node_2 disconnected from the cluster (wsrep_cluster_size = 1 on
#          node_2); node_1 forming a 1-node primary component.

--disable_query_log

--connection node_2
# Allow large slave queue without flow control
SET GLOBAL wsrep_provider_options='gcs.fc_limit=1K';
# Introduce 2 inconsistencies locally (not replicated)
SET wsrep_on=OFF;
DELETE FROM t1 WHERE f1 = 2;
DELETE FROM t1 WHERE f1 = 4;
SET wsrep_on=ON;

# Build up slave queue:
# - first 8 events will be picked up by slave threads
# - 2 more events will wait in the slave queue
LOCK TABLES t1 WRITE;

--connection node_1
UPDATE t1 SET f2 = 1 WHERE f1 = 1;
UPDATE t1 SET f2 = 1 WHERE f1 = 2;
UPDATE t1 SET f2 = 1 WHERE f1 = 3;
UPDATE t1 SET f2 = 1 WHERE f1 = 4;
UPDATE t1 SET f2 = 2 WHERE f1 = 4;
UPDATE t1 SET f2 = 3 WHERE f1 = 4;
UPDATE t1 SET f2 = 1 WHERE f1 = 5;
UPDATE t1 SET f2 = 1 WHERE f1 = 6;
UPDATE t1 SET f2 = 1 WHERE f1 = 7;
UPDATE t1 SET f2 = 1 WHERE f1 = 8;

--connection node_2
# make sure all events landed in slave queue (>= 1 avoids racing on exact count)
SET wsrep_on=OFF;
--let $wait_condition = SELECT VARIABLE_VALUE >= 1 FROM performance_schema.global_status WHERE VARIABLE_NAME = 'wsrep_local_recv_queue';
--source include/wait_condition.inc
SET wsrep_on=ON;
UNLOCK TABLES;

--enable_query_log

# node_2 detects the inconsistency, gets voted out and leaves the cluster.
# With debug binaries evs.inactive_timeout=PT30S, so the view change can take
# over 30 seconds. Use a 60-second timeout to survive that.
--let $wait_timeout = 60
--let $members=0
--source include/wsrep_wait_membership.inc
