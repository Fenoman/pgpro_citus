--
-- ADD_COORDINATOR
--

-- node trying to add itself without specifying groupid => 0 should error out
-- first remove the coordinator to for testing master_add_node for coordinator
SELECT master_remove_node('localhost', :master_port);
SELECT master_add_node('localhost', :master_port);

SELECT master_add_node('localhost', :master_port, groupid => 0) AS master_nodeid \gset

-- adding the same node again should return the existing nodeid
SELECT master_add_node('localhost', :master_port, groupid => 0) = :master_nodeid;

-- adding another node with groupid=0 should error out
SELECT master_add_node('localhost', 12345, groupid => 0) = :master_nodeid;

-- start_metadata_sync_to_node() for coordinator should raise a notice
SELECT start_metadata_sync_to_node('localhost', :master_port);
