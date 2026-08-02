use raft::{
    sim::cluster::{PersistenceFaultPoint, SimCluster},
    sm::mem_kv::{MemKv, MemKvCommand, MemKvSnapshot},
    traits::state_machine::SnapshotableStateMachine,
    types::Role,
};

#[test]
fn crash_after_entries_does_not_publish_commit_or_apply() {
    let replica = raft::types::ReplicaId::must(1);
    let mut cluster = SimCluster::<MemKvCommand, MemKvSnapshot, MemKv>::new(vec![replica], 5, 2);

    cluster.tick(replica, 5);
    assert_eq!(cluster.node(replica).unwrap().role(), &Role::Leader);

    cluster.inject_persistence_fault(replica, PersistenceFaultPoint::AfterSnapshotAndEntries);
    let (_, messages) = cluster
        .propose(
            replica,
            MemKvCommand::Put {
                key: "order/7".to_string(),
                value: "paid".to_string(),
            },
        )
        .unwrap();

    assert!(messages.is_empty());
    assert!(cluster.is_crashed(replica));

    cluster.restart(replica);
    let restarted = cluster.node(replica).unwrap();
    assert_eq!(restarted.last_log_index(), 1);
    assert_eq!(restarted.commit_index(), 0);
    assert_eq!(cluster.state_machine(replica).unwrap().last_applied(), 0);
    assert_eq!(cluster.state_machine(replica).unwrap().get("order/7"), None);
}
