use super::*;

use std::time::Instant;

#[tokio::test(flavor = "multi_thread", worker_threads = 5)]
async fn test_node_restart_from_snapshot_retains_voter() -> anyhow::Result<()> {
    init_tracing();

    let node_configs = vec![
        (1, "127.0.0.1:31001"),
        (2, "127.0.0.1:31002"),
        (3, "127.0.0.1:31003"),
    ];

    let storage_paths: Vec<NodeStoragePaths> = node_configs
        .iter()
        .map(|(id, _)| NodeStoragePaths::for_node(*id, 3))
        .collect::<Result<Vec<_>>>()?;

    let node_clients = make_node_clients(node_configs.len());

    let (config, pcfg, mut raft_nodes, mut state_machines, mut server_handles) =
        setup_cluster(&node_configs, node_clients.clone(), Some(&storage_paths)).await?;

    tokio::time::sleep(Duration::from_secs(2)).await;

    connect_clients(&node_configs, &node_clients, &pcfg, |_, _| {
        "127.0.0.1:0".to_string()
    })
    .await?;

    let initial_members = membership_from(&node_configs);
    raft_nodes[0].initialize(initial_members).await?;

    let leader_id = wait_for_leader_consistent(&raft_nodes, Duration::from_secs(10)).await?;
    let leader_index = node_configs
        .iter()
        .position(|(id, _)| *id == leader_id)
        .expect("Leader must be part of node configs");

    for (idx, (node_id, _)) in node_configs.iter().enumerate() {
        wait_for_node_to_be_voter(&raft_nodes[idx], *node_id, Duration::from_secs(10)).await?;
    }

    let snapshot_key = "snapshot_test_key";
    client_write_and_wait(
        &raft_nodes[leader_index],
        &raft_nodes,
        set_request(snapshot_key, "snapshot_value"),
    )
    .await?;

    raft_nodes[leader_index].trigger().snapshot().await?;
    wait_for_snapshot_file(
        &storage_paths[leader_index].snapshot_dir,
        Duration::from_secs(10),
    )
    .await?;

    {
        let leader_server = server_handles.remove(leader_index);
        leader_server.abort();

        let _ = raft_nodes.remove(leader_index);
        let _ = state_machines.remove(leader_index);
    }

    tokio::time::sleep(Duration::from_secs(5)).await;

    let (restarted_raft, restarted_sm, restarted_server) = setup_node(
        leader_id,
        node_configs[leader_index].1,
        Arc::clone(&config),
        node_clients.clone(),
        Some(&storage_paths[leader_index]),
    )
    .await?;

    raft_nodes.insert(leader_index, restarted_raft.clone());
    state_machines.insert(leader_index, restarted_sm.clone());
    server_handles.insert(leader_index, restarted_server);

    let server_addr = node_configs[leader_index].1;
    let server_key = server_addr.to_string();
    let replacement_client = RClientBuilder::new()
        .remote_addr(server_addr)
        .server_name("localhost")
        .local_bind_addr("127.0.0.1:0")
        .dangerous_skip_verification(true)
        .protocol_cfg(pcfg.clone())
        .build()
        .await?;
    node_clients
        .entry_async(server_key.clone())
        .await
        .and_modify(|existing| {
            *existing = replacement_client.clone();
        })
        .or_insert_with(|| replacement_client.clone());

    wait_for_leader_consistent(&raft_nodes, Duration::from_secs(15)).await?;
    wait_for_node_to_be_voter(
        &raft_nodes[leader_index],
        leader_id,
        Duration::from_secs(10),
    )
    .await?;

    let sm = state_machines[leader_index].state_machine.read().await;
    let value = sm
        .data
        .get(snapshot_key)
        .expect("Value should persist across restart");
    let value_str: String = rmp_serde::from_slice(value).unwrap();
    assert_eq!(value_str, "snapshot_value");

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 5)]
async fn test_add_voter_node_after_snapshot_compaction() -> Result<()> {
    init_tracing();

    // Set up an initial three-node cluster with persistent storage so snapshots
    // produce on-disk artifacts and purge earlier log entries.
    let initial_configs = vec![
        (1, "127.0.0.1:33001"),
        (2, "127.0.0.1:33002"),
        (3, "127.0.0.1:33003"),
    ];

    let mut storage_paths: Vec<NodeStoragePaths> = initial_configs
        .iter()
        .map(|(id, _)| NodeStoragePaths::for_node(*id, 3))
        .collect::<Result<Vec<_>>>()?;

    let node_clients = make_node_clients(initial_configs.len() + 1);

    let (config, pcfg, mut raft_nodes, mut state_machines, mut server_handles) =
        setup_cluster(&initial_configs, node_clients.clone(), Some(&storage_paths)).await?;

    tokio::time::sleep(Duration::from_secs(2)).await;

    connect_clients(&initial_configs, &node_clients, &pcfg, |i, _| {
        format!("127.0.0.1:{}", 34001 + i as u16)
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

    // Issue a client write that will later be recovered via snapshot installation.
    let pre_snapshot_write = raft_nodes[leader_index]
        .client_write(set_request("pre_snapshot_key", "pre_snapshot_value"))
        .await?;
    let pre_snapshot_log_id = pre_snapshot_write.log_id;
    wait_for_log_commit(&raft_nodes, pre_snapshot_log_id).await?;

    assert_state_machine_value(&state_machines, "pre_snapshot_key", "pre_snapshot_value").await;

    // Trigger a snapshot to compact the log and remove earlier entries from the log store.
    raft_nodes[leader_index].trigger().snapshot().await?;
    wait_for_snapshot_file(
        &storage_paths[leader_index].snapshot_dir,
        Duration::from_secs(10),
    )
    .await?;

    raft_nodes[leader_index]
        .trigger()
        .purge_log(pre_snapshot_log_id.index)
        .await?;

    let purge_timeout = Duration::from_secs(10);
    let purge_start = Instant::now();
    loop {
        let mut log_store = get_log_store_handle(initial_configs[leader_index].1)?;
        let log_state = log_store.get_log_state().await?;

        if log_state.last_purged_log_id == Some(pre_snapshot_log_id) {
            break;
        }

        if purge_start.elapsed() > purge_timeout {
            bail!(
                "Leader log store was not purged to {:?} within {:?}",
                pre_snapshot_log_id,
                purge_timeout
            );
        }

        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    // Bring up a new node that will be added as a voter after compaction.
    let new_node_id = 4;
    let new_node_addr = "127.0.0.1:33004";
    let new_node_paths = NodeStoragePaths::for_node(new_node_id, 3)?;
    let new_snapshot_dir = new_node_paths.snapshot_dir.clone();
    let (new_raft, new_sm, new_server) = setup_node(
        new_node_id,
        new_node_addr,
        Arc::clone(&config),
        node_clients.clone(),
        Some(&new_node_paths),
    )
    .await?;
    storage_paths.push(new_node_paths);
    raft_nodes.push(new_raft);
    state_machines.push(new_sm);
    server_handles.push(new_server);

    // Create a client connection for the new node.
    let new_client_addr = "127.0.0.1:34004";
    let new_client = create_rclient(new_node_addr, new_client_addr, &pcfg).await?;
    node_clients
        .insert_sync(new_node_addr.to_string(), new_client)
        .map_err(|e| anyhow::anyhow!("{:?}", e))?;

    // Add the new node as a learner; this should trigger a snapshot transfer since the
    // earlier log entries have been purged by compaction.
    add_learner_and_wait(
        &raft_nodes[leader_index],
        &raft_nodes,
        new_node_id,
        new_node_addr,
    )
    .await?;

    wait_for_snapshot_file(&new_snapshot_dir, Duration::from_secs(20)).await?;

    // Ensure the new node replayed the snapshot and now contains the data written before
    // it joined the cluster.
    let expected_key = "pre_snapshot_key";
    let expected_value = "pre_snapshot_value";
    let start = Instant::now();
    let timeout = Duration::from_secs(10);
    loop {
        {
            let new_node_state = state_machines
                .last()
                .expect("new node state machine should be present")
                .state_machine
                .read()
                .await;
            if let Some(bytes) = new_node_state.data.get(expected_key) {
                let value: String = rmp_serde::from_slice(bytes).unwrap();
                if value == expected_value {
                    break;
                }
            }
        }

        if start.elapsed() > timeout {
            bail!(
                "New node did not receive expected snapshot data within {:?}",
                timeout
            );
        }

        tokio::time::sleep(Duration::from_millis(200)).await;
    }

    // Promote the new node to a voter via membership change.
    let new_members: std::collections::BTreeSet<u64> = initial_configs
        .iter()
        .map(|(node_id, _)| *node_id)
        .chain(std::iter::once(new_node_id))
        .collect();

    let change_resp = raft_nodes[leader_index]
        .change_membership(new_members, false)
        .await?;
    wait_for_log_commit(&raft_nodes, change_resp.log_id).await?;

    // Issue a client write after the membership change so the new voter applies
    // post-compaction log entries too.
    client_write_and_wait(
        &raft_nodes[leader_index],
        &raft_nodes,
        set_request("post_snapshot_key", "post_snapshot_value"),
    )
    .await?;

    assert_state_machine_value(&state_machines, "pre_snapshot_key", "pre_snapshot_value").await;
    assert_state_machine_value(&state_machines, "post_snapshot_key", "post_snapshot_value").await;

    Ok(())
}
