use crate::raft::archive;

#[test]
fn parses_idx_from_full_snapshot_id_with_leader_parts() {
    // leader_id Display is "T{term}-N{node_id}", separated with '-'
    let id = "T3-N1-42-7"; // term=3, node=1, last_index=42, snapshot_idx=7
    assert_eq!(archive::parse_trailing_index(id), Some(7));
}

#[test]
fn parses_idx_from_minimal_snapshot_id_without_last_log() {
    let id = "--12"; // no last log, only snapshot_idx=12
    assert_eq!(archive::parse_trailing_index(id), Some(12));
}

#[test]
fn returns_none_when_no_trailing_number() {
    let id = "invalid-format-"; // trailing hyphen, no number
    assert_eq!(archive::parse_trailing_index(id), None);
}

#[test]
fn parses_idx_from_real_world_example() {
    let id = "T2-N1-10999-11";
    assert_eq!(archive::parse_trailing_index(id), Some(11));
}
