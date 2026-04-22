use super::*;
use crate::raft::store::{RateLimitDelta, Subject, rate_limit_key};
use std::time::{SystemTime, UNIX_EPOCH};

const USER_SUBJECT_ID: u128 = 11;
const COMPANY_SUBJECT_ID: u128 = 22;
const GENERIC_IP_SUBJECT_ID: u128 = 33;
const GENERIC_GLOBAL_SUBJECT_ID: u128 = 0;

struct RateLimitSyncCluster {
    node_configs: Vec<OwnedNodeConfig>,
    raft_nodes: Vec<Raft>,
    state_machines: Vec<Arc<StateMachineStore>>,
    server_handles: Vec<RServer>,
}

async fn setup_rate_limit_sync_cluster() -> Result<RateLimitSyncCluster> {
    let (_network_guard, node_configs) = reserve_node_configs(3).await?;
    let node_clients = make_node_clients(node_configs.len());
    let (_config, _pcfg, raft_nodes, state_machines, server_handles) =
        setup_connected_cluster(&node_configs, node_clients, None).await?;
    initialize_first_node_membership(&raft_nodes, &node_configs).await?;

    Ok(RateLimitSyncCluster {
        node_configs,
        raft_nodes,
        state_machines,
        server_handles,
    })
}

fn current_day_epoch() -> u64 {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock")
        .as_secs();
    now / 86400
}

fn all_subject_deltas(day_epoch: u64) -> Vec<RateLimitDelta> {
    vec![
        RateLimitDelta {
            subject: Subject::User,
            id: USER_SUBJECT_ID,
            day_epoch,
            add_active: 0,
            add_day: 2,
        },
        RateLimitDelta {
            subject: Subject::Company,
            id: COMPANY_SUBJECT_ID,
            day_epoch,
            add_active: 0,
            add_day: 3,
        },
        RateLimitDelta {
            subject: Subject::GenericIp,
            id: GENERIC_IP_SUBJECT_ID,
            day_epoch,
            add_active: 0,
            add_day: 4,
        },
        RateLimitDelta {
            subject: Subject::GenericGlobal,
            id: GENERIC_GLOBAL_SUBJECT_ID,
            day_epoch,
            add_active: 0,
            add_day: 5,
        },
    ]
}

async fn assert_subject_counters(
    state_machines: &[Arc<StateMachineStore>],
    day_epoch: u64,
    expected_multiplier: u64,
) {
    for sm in state_machines {
        let user_key = rate_limit_key(Subject::User, USER_SUBJECT_ID);
        let company_key = rate_limit_key(Subject::Company, COMPANY_SUBJECT_ID);
        let generic_ip_key = rate_limit_key(Subject::GenericIp, GENERIC_IP_SUBJECT_ID);
        let generic_global_key = rate_limit_key(Subject::GenericGlobal, GENERIC_GLOBAL_SUBJECT_ID);

        let (user_active, user_day) = sm.get_rate_limit_usage(&user_key, day_epoch).await;
        assert_eq!(user_active, 0);
        assert_eq!(user_day, 2 * expected_multiplier);

        let (company_active, company_day) = sm.get_rate_limit_usage(&company_key, day_epoch).await;
        assert_eq!(company_active, 0);
        assert_eq!(company_day, 3 * expected_multiplier);

        let (generic_ip_active, generic_ip_day) =
            sm.get_rate_limit_usage(&generic_ip_key, day_epoch).await;
        assert_eq!(generic_ip_active, 0);
        assert_eq!(generic_ip_day, 4 * expected_multiplier);

        let (generic_global_active, generic_global_day) = sm
            .get_rate_limit_usage(&generic_global_key, day_epoch)
            .await;
        assert_eq!(generic_global_active, 0);
        assert_eq!(generic_global_day, 5 * expected_multiplier);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn test_rate_limit_sync_all_subjects_across_three_nodes() -> Result<()> {
    init_tracing();

    let cluster = setup_rate_limit_sync_cluster().await?;
    let RateLimitSyncCluster {
        node_configs,
        raft_nodes,
        state_machines,
        ..
    } = cluster;

    let (_leader_id, leader_index) =
        wait_for_consistent_leader_index(&raft_nodes, &node_configs, Duration::from_secs(10))
            .await?;

    let day_epoch = current_day_epoch();
    let deltas = all_subject_deltas(day_epoch);

    client_write_and_wait(
        &raft_nodes[leader_index],
        &raft_nodes,
        Request::RateLimitDeltas {
            request_id: Uuid::new_v4().as_u128(),
            deltas,
        },
    )
    .await?;

    assert_subject_counters(&state_machines, day_epoch, 1).await;

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn test_rate_limit_sync_accumulates_all_subjects_across_writes() -> Result<()> {
    init_tracing();

    let cluster = setup_rate_limit_sync_cluster().await?;
    let RateLimitSyncCluster {
        node_configs,
        raft_nodes,
        state_machines,
        ..
    } = cluster;

    let (_leader_id, leader_index) =
        wait_for_consistent_leader_index(&raft_nodes, &node_configs, Duration::from_secs(10))
            .await?;

    let day_epoch = current_day_epoch();

    for _ in 0..2 {
        let deltas = all_subject_deltas(day_epoch);
        client_write_and_wait(
            &raft_nodes[leader_index],
            &raft_nodes,
            Request::RateLimitDeltas {
                request_id: Uuid::new_v4().as_u128(),
                deltas,
            },
        )
        .await?;
    }

    assert_subject_counters(&state_machines, day_epoch, 2).await;

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn test_rate_limit_sync_converges_after_each_gateway_batch() -> Result<()> {
    init_tracing();

    let cluster = setup_rate_limit_sync_cluster().await?;
    let RateLimitSyncCluster {
        node_configs,
        raft_nodes,
        state_machines,
        ..
    } = cluster;

    let (_leader_id, leader_index) =
        wait_for_consistent_leader_index(&raft_nodes, &node_configs, Duration::from_secs(10))
            .await?;

    let day_epoch = current_day_epoch();

    for expected_multiplier in 1..=3_u64 {
        client_write_and_wait(
            &raft_nodes[leader_index],
            &raft_nodes,
            Request::RateLimitDeltas {
                request_id: Uuid::new_v4().as_u128(),
                deltas: all_subject_deltas(day_epoch),
            },
        )
        .await?;

        assert_subject_counters(&state_machines, day_epoch, expected_multiplier).await;
    }

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn test_rate_limit_sync_after_leader_failover() -> Result<()> {
    init_tracing();

    let cluster = setup_rate_limit_sync_cluster().await?;
    let RateLimitSyncCluster {
        mut node_configs,
        mut raft_nodes,
        mut state_machines,
        mut server_handles,
    } = cluster;

    let (old_leader, old_leader_index) =
        wait_for_consistent_leader_index(&raft_nodes, &node_configs, Duration::from_secs(10))
            .await?;

    let day_epoch = current_day_epoch();
    client_write_and_wait(
        &raft_nodes[old_leader_index],
        &raft_nodes,
        Request::RateLimitDeltas {
            request_id: Uuid::new_v4().as_u128(),
            deltas: all_subject_deltas(day_epoch),
        },
    )
    .await?;
    assert_subject_counters(&state_machines, day_epoch, 1).await;

    server_handles.remove(old_leader_index).abort();
    let _ = raft_nodes.remove(old_leader_index);
    let _ = state_machines.remove(old_leader_index);
    let _ = node_configs.remove(old_leader_index);

    let new_leader =
        wait_for_leader_consistent_excluding(&raft_nodes, old_leader, Duration::from_secs(30))
            .await?;
    let new_leader_index = node_index(&node_configs, new_leader);

    client_write_and_wait(
        &raft_nodes[new_leader_index],
        &raft_nodes,
        Request::RateLimitDeltas {
            request_id: Uuid::new_v4().as_u128(),
            deltas: all_subject_deltas(day_epoch),
        },
    )
    .await?;

    assert_subject_counters(&state_machines, day_epoch, 2).await;

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn test_rate_limit_sync_resets_on_epoch_rollover_across_nodes() -> Result<()> {
    init_tracing();

    let cluster = setup_rate_limit_sync_cluster().await?;
    let RateLimitSyncCluster {
        node_configs,
        raft_nodes,
        state_machines,
        ..
    } = cluster;

    let (_leader_id, leader_index) =
        wait_for_consistent_leader_index(&raft_nodes, &node_configs, Duration::from_secs(10))
            .await?;

    let day_epoch = current_day_epoch();
    let next_day = day_epoch + 1;

    client_write_and_wait(
        &raft_nodes[leader_index],
        &raft_nodes,
        Request::RateLimitDeltas {
            request_id: Uuid::new_v4().as_u128(),
            deltas: vec![RateLimitDelta {
                subject: Subject::Company,
                id: COMPANY_SUBJECT_ID,
                day_epoch,
                add_active: 0,
                add_day: 3,
            }],
        },
    )
    .await?;

    client_write_and_wait(
        &raft_nodes[leader_index],
        &raft_nodes,
        Request::RateLimitDeltas {
            request_id: Uuid::new_v4().as_u128(),
            deltas: vec![RateLimitDelta {
                subject: Subject::Company,
                id: COMPANY_SUBJECT_ID,
                day_epoch: next_day,
                add_active: 0,
                add_day: 1,
            }],
        },
    )
    .await?;

    let company_key = rate_limit_key(Subject::Company, COMPANY_SUBJECT_ID);
    for sm in &state_machines {
        let (new_active, new_day) = sm.get_rate_limit_usage(&company_key, next_day).await;
        assert_eq!(new_active, 0);
        assert_eq!(new_day, 1);

        let (old_active, old_day) = sm.get_rate_limit_usage(&company_key, day_epoch).await;
        assert_eq!(old_active, 0);
        assert_eq!(old_day, 0);
    }

    Ok(())
}
