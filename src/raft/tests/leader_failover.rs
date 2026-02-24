use super::*;

#[tokio::test(flavor = "multi_thread", worker_threads = 5)]
async fn test_leader_failover() -> anyhow::Result<()> {
    init_tracing();

    let node_configs = vec![
        (1, "127.0.0.1:24007"),
        (2, "127.0.0.1:24008"),
        (3, "127.0.0.1:24009"),
        (4, "127.0.0.1:24010"),
        (5, "127.0.0.1:24011"),
    ];
    let node_clients = make_node_clients(node_configs.len());

    let (_config, pcfg, mut raft_nodes, mut state_machines, mut server_handles) =
        setup_cluster(&node_configs, node_clients.clone(), None).await?;

    // Give servers a moment to start up.
    tokio::time::sleep(Duration::from_secs(2)).await;

    // Connect a client to each node.
    connect_clients(&node_configs, &node_clients, &pcfg, |i, _| {
        format!("127.0.0.1:{}", 26001 + i as u16)
    })
    .await?;

    // Initialize the cluster membership on every node.
    for (i, raft) in raft_nodes.clone().into_iter().enumerate() {
        let initial_nodes = membership_from(&node_configs);
        let membership = openraft::Membership::from(initial_nodes.clone());
        info!(
            "Node {} initializing with membership configuration: {:?}",
            node_configs[i].0, membership
        );

        let raft_clone = raft.clone();
        tokio::spawn(async move {
            raft_clone.initialize(initial_nodes).await.unwrap();
        });
    }

    // Wait until all nodes agree on the leader.
    let old_leader = wait_for_leader_consistent(&raft_nodes, Duration::from_secs(10)).await?;
    info!("Initial leader elected: {:?}", old_leader);

    // Verify that all nodes see the elected leader.
    for (i, raft) in raft_nodes.iter().enumerate() {
        let observed_leader = raft.metrics().borrow().current_leader;
        assert_eq!(
            observed_leader,
            Some(old_leader),
            "Node {} sees a different leader",
            i + 1
        );
    }

    // Simulate a leader failure by "killing" the leader's server.
    let leader_index = node_configs
        .iter()
        .position(|(id, _)| *id == old_leader)
        .expect("Leader must be one of the nodes");

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
