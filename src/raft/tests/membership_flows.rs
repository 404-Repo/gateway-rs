use super::*;

use std::collections::BTreeSet;

#[tokio::test(flavor = "multi_thread", worker_threads = 5)]
async fn test_add_node_to_three_node_cluster() -> Result<()> {
    init_tracing();

    // Set up a three-node cluster.
    let initial_configs = vec![
        (1, "127.0.0.1:27001"),
        (2, "127.0.0.1:27002"),
        (3, "127.0.0.1:27003"),
    ];
    let node_clients = make_node_clients(initial_configs.len());

    let (config, pcfg, mut raft_nodes, mut state_machines, mut server_handles) =
        setup_cluster(&initial_configs, node_clients.clone(), None).await?;

    tokio::time::sleep(Duration::from_secs(2)).await;

    connect_clients(&initial_configs, &node_clients, &pcfg, |i, _| {
        format!("127.0.0.1:{}", 28001 + i as u16)
    })
    .await?;

    // Initialize the cluster membership with all three nodes as voters.
    let initial_members = membership_from(&initial_configs);
    raft_nodes[0].initialize(initial_members).await?;

    // Wait for the leader to be elected.
    let leader_id = wait_for_leader_consistent(&raft_nodes, Duration::from_secs(10)).await?;
    let leader_index = initial_configs
        .iter()
        .position(|(id, _)| *id == leader_id)
        .expect("Leader must be one of the initial nodes");

    // Issue a client write to set a key/value pair.
    client_write_and_wait(
        &raft_nodes[leader_index],
        &raft_nodes,
        set_request("test_key", "test_value"),
    )
    .await?;

    // Verify that all initial nodes applied the command.
    assert_state_machine_value(&state_machines, "test_key", "test_value").await;

    // Now, add a new node (node 4) to the existing three-node cluster.
    let new_node_id = 4;
    let new_node_addr = "127.0.0.1:27004";
    let (new_raft, new_sm, new_server) = setup_node(
        new_node_id,
        new_node_addr,
        Arc::clone(&config),
        node_clients.clone(),
        None,
    )
    .await?;
    raft_nodes.push(new_raft);
    state_machines.push(new_sm);
    server_handles.push(new_server);

    // Create a client connection for the new node.
    let new_client_addr = "127.0.0.1:28004";
    let new_client = create_rclient(new_node_addr, new_client_addr, &pcfg).await?;
    node_clients
        .insert_sync(new_node_addr.to_string(), new_client)
        .map_err(|e| anyhow::anyhow!("{:?}", e))?;

    // Add the new node as a learner using the current leader.
    add_learner_and_wait(
        &raft_nodes[leader_index],
        &raft_nodes,
        new_node_id,
        new_node_addr,
    )
    .await?;

    // Verify that the new node's state machine caught up with the previously applied log.
    let new_node_sm = state_machines.last().unwrap();
    let sm = new_node_sm.state_machine.read().await;
    let value = sm.data.get("test_key").unwrap();
    let value_str: String = rmp_serde::from_slice(value).unwrap();
    assert_eq!(value_str, "test_value");

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 5)]
async fn test_add_voter_node_to_three_node_cluster_change_membership() -> Result<()> {
    init_tracing();

    // Set up an initial three-node cluster.
    let initial_configs = vec![
        (1, "127.0.0.1:29001"),
        (2, "127.0.0.1:29002"),
        (3, "127.0.0.1:29003"),
    ];

    let node_clients = make_node_clients(initial_configs.len());

    let (config, pcfg, mut raft_nodes, mut state_machines, mut server_handles) =
        setup_cluster(&initial_configs, node_clients.clone(), None).await?;

    // Allow the servers time to start up.
    tokio::time::sleep(Duration::from_secs(2)).await;

    // Establish client connections for the initial nodes.
    connect_clients(&initial_configs, &node_clients, &pcfg, |i, _| {
        format!("127.0.0.1:{}", 30001 + i as u16)
    })
    .await?;

    // Initialize the cluster membership for the initial three nodes.
    let initial_members = membership_from(&initial_configs);
    raft_nodes[0].initialize(initial_members).await?;

    // Wait until a leader is elected.
    let leader_id = wait_for_leader_consistent(&raft_nodes, Duration::from_secs(10)).await?;
    let leader_index = initial_configs
        .iter()
        .position(|(id, _)| *id == leader_id)
        .expect("Leader must be one of the nodes");

    // Bring up a new node (node 4) that will be added as a voter.
    let new_node_id = 4;
    let new_node_addr = "127.0.0.1:29004";
    let (new_raft, new_sm, new_server) = setup_node(
        new_node_id,
        new_node_addr,
        Arc::clone(&config),
        node_clients.clone(),
        None,
    )
    .await?;
    raft_nodes.push(new_raft);
    state_machines.push(new_sm);
    server_handles.push(new_server);

    // Create a client connection for the new node.
    let new_client_addr = "127.0.0.1:30004";
    let new_client = create_rclient(new_node_addr, new_client_addr, &pcfg).await?;

    node_clients
        .insert_sync(new_node_addr.to_string(), new_client)
        .map_err(|e| anyhow::anyhow!("{:?}", e))?;

    // Add the new node as a learner before promoting it to a voter.
    add_learner_and_wait(
        &raft_nodes[leader_index],
        &raft_nodes,
        new_node_id,
        new_node_addr,
    )
    .await?;

    // Build the new membership configuration as a set of node IDs.
    // Since node 4 is already added as a learner, this promotion will convert it to a voter.
    let new_members: BTreeSet<u64> = initial_configs
        .iter()
        .map(|(node_id, _)| *node_id)
        .chain(std::iter::once(new_node_id))
        .collect();

    // Issue the membership change to promote the learner to a voter.
    let change_resp = raft_nodes[leader_index]
        .change_membership(new_members, false)
        .await?;
    wait_for_log_commit(&raft_nodes, change_resp.log_id).await?;

    // Issue a client write after the membership change.
    client_write_and_wait(
        &raft_nodes[leader_index],
        &raft_nodes,
        set_request("test_key", "test_value"),
    )
    .await?;

    // Verify that every node in the cluster, including the newly promoted voter, has applied the command.
    assert_state_machine_value(&state_machines, "test_key", "test_value").await;

    Ok(())
}
