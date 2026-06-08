# Standard log suppressions for tests that provoke an inconsistency on node_2
# and vote it out of the cluster.  Source this from the connection of each
# node that will see inconsistency-related messages.
#
# Usage (repeat for each participating node connection):
#   --connection node_N
#   --source include/galera_inconsistency_suppressions.inc

CALL mtr.add_suppression('Inconsistency detected: Inconsistent by consensus on');
CALL mtr.add_suppression('last left .* greater than drain seqno');
CALL mtr.add_suppression("Failed to apply write set: ");
