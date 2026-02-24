use super::*;
use std::collections::BTreeMap;

use openraft::Membership;

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn test_three_node_cluster() -> Result<()> {
    init_tracing();

    let node_configs = vec![
        (1, "127.0.0.1:21001"),
        (2, "127.0.0.1:21002"),
        (3, "127.0.0.1:21003"),
    ];
    let node_clients = make_node_clients(node_configs.len());

    let (_config, pcfg, raft_nodes, state_machines, _server_handles) =
        setup_cluster(&node_configs, node_clients.clone(), None).await?;

    tokio::time::sleep(Duration::from_secs(2)).await;

    connect_clients(&node_configs, &node_clients, &pcfg, |i, _| {
        format!("127.0.0.1:{}", 22001 + i)
    })
    .await?;

    // Initialize and configure the first node as leader
    let mut initial_nodes = BTreeMap::new();
    initial_nodes.insert(
        1,
        BasicNode {
            addr: node_configs[0].1.to_string(),
        },
    );

    raft_nodes[0].initialize(initial_nodes).await?;

    wait_for_leader(&raft_nodes, Duration::from_secs(10)).await?;

    // Add learner nodes and wait for synchronization
    let last_log_id = {
        let mut last_log_id = None;
        for (_i, (node_id, addr)) in node_configs.iter().enumerate().skip(1) {
            let node = BasicNode {
                addr: addr.to_string(),
            };
            let add_learner_result = raft_nodes[0].add_learner(*node_id, node, false).await?;
            last_log_id = Some(add_learner_result.log_id);
        }
        last_log_id
    };

    if let Some(log_id) = last_log_id {
        wait_for_log_commit(&raft_nodes, log_id).await?;
    } else {
        panic!("log_id must not be None");
    }

    // Write and verify test data
    client_write_and_wait(
        &raft_nodes[0],
        &raft_nodes,
        set_request("test_key", "test_value"),
    )
    .await?;

    assert_state_machine_value(&state_machines, "test_key", "test_value").await;

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 5)]
async fn test_vote_mode_all_initialized() -> Result<()> {
    init_tracing();

    let node_configs = vec![
        (1, "127.0.0.1:24004"),
        (2, "127.0.0.1:24005"),
        (3, "127.0.0.1:24006"),
    ];
    let node_clients = make_node_clients(node_configs.len());

    let (_config, pcfg, raft_nodes, state_machines, _server_handles) =
        setup_cluster(&node_configs, node_clients.clone(), None).await?;

    tokio::time::sleep(Duration::from_secs(2)).await;

    connect_clients(&node_configs, &node_clients, &pcfg, |i, _| {
        format!("127.0.0.1:{}", 25001 + i as u16)
    })
    .await?;

    for (i, raft) in raft_nodes.clone().into_iter().enumerate() {
        let initial_nodes = membership_from(&node_configs);
        let membership: Membership<_, _> = Membership::from(initial_nodes.clone());
        info!(
            "Node {} initializing with membership configuration: {}",
            node_configs[i].0, membership
        );

        tokio::spawn(async move {
            raft.initialize(initial_nodes).await.unwrap();
        });
    }

    // Wait until a leader is elected.
    let leader_id = wait_for_leader_consistent(&raft_nodes, Duration::from_secs(10)).await?;

    info!("Leader elected: {:?}", leader_id);
    for (i, raft) in raft_nodes.iter().enumerate() {
        let observed_leader = raft.metrics().borrow().current_leader;
        assert_eq!(
            observed_leader,
            Some(leader_id),
            "Node {} sees a different leader",
            i + 1
        );
    }

    let leader_index = node_configs
        .iter()
        .position(|(id, _)| *id == leader_id)
        .expect("Leader must be one of the nodes");
    client_write_and_wait(
        &raft_nodes[leader_index],
        &raft_nodes,
        set_request("vote_mode_key", "vote_mode_value"),
    )
    .await?;

    // Verify that the write has propagated to the state machines of all nodes.
    assert_state_machine_value(&state_machines, "vote_mode_key", "vote_mode_value").await;

    Ok(())
}
