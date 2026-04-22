use super::*;

#[tokio::test(flavor = "multi_thread", worker_threads = 5)]
async fn test_leader_failover() -> anyhow::Result<()> {
    init_tracing();

    let (_network_guard, node_configs) = reserve_node_configs(5).await?;
    let node_clients = make_node_clients(node_configs.len());

    let (_config, _pcfg, mut raft_nodes, mut state_machines, mut server_handles) =
        setup_connected_cluster(&node_configs, node_clients.clone(), None).await?;
    let refs = node_config_refs(&node_configs);

    // Initialize the cluster membership on every node.
    for (i, _) in raft_nodes.iter().enumerate() {
        let membership = openraft::Membership::from(membership_from(&refs));
        info!(
            "Node {} initializing with membership configuration: {:?}",
            node_configs[i].0, membership
        );
    }
    initialize_all_nodes_membership(&raft_nodes, &node_configs);

    // Wait until all nodes agree on the leader.
    let (old_leader, leader_index) =
        wait_for_consistent_leader_index(&raft_nodes, &node_configs, Duration::from_secs(10))
            .await?;
    info!("Initial leader elected: {:?}", old_leader);

    // Verify that all nodes see the elected leader.
    assert_all_nodes_see_leader(&raft_nodes, old_leader);

    // Simulate a leader failure by "killing" the leader's server.
    info!(
        "Simulating leader failure: killing leader node {} at {}",
        old_leader, node_configs[leader_index].1
    );
    // Dropping the server handle simulates a crash.
    server_handles.remove(leader_index).abort();
    {
        let _ = raft_nodes.remove(leader_index);
        let _ = state_machines.remove(leader_index);
    }

    // Wait for a new leader to be elected and for all nodes to agree.
    let new_leader =
        wait_for_leader_consistent_excluding(&raft_nodes, old_leader, Duration::from_secs(30))
            .await?;
    info!("New leader elected: {:?}", new_leader);
    assert_ne!(
        new_leader, old_leader,
        "The new leader should be different from the old leader."
    );

    Ok(())
}
