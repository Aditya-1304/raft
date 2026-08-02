use raft::{
    sim::cluster::{PersistenceFaultPoint, SimCluster},
    sm::mem_kv::{MemKv, MemKvCommand, MemKvSnapshot},
    traits::state_machine::SnapshotableStateMachine,
    types::Role,
};

#[test]
fn crash_after_entries_does_not_publish_commit_or_apply() {
    let mut cluster = SimCluster::<MemKvCommand, MemKvSnapshot, MemKv>::new(vec![1], 5, 2);

    cluster.tick(1, 5);
    assert_eq!(cluster.node(1).unwrap().role(), &Role::Leader);

    cluster.inject_persistence_fault(1, PersistenceFaultPoint::AfterSnapshotAndEntries);
    let (_, messages) = cluster
        .propose(
            1,
            MemKvCommand::Put {
                key: "order/7".to_string(),
                value: "paid".to_string(),
            },
        )
        .unwrap();

    assert!(messages.is_empty());
    assert!(cluster.is_crashed(1));

    cluster.restart(1);
    let restarted = cluster.node(1).unwrap();
    assert_eq!(restarted.last_log_index(), 1);
    assert_eq!(restarted.commit_index(), 0);
    assert_eq!(cluster.state_machine(1).unwrap().last_applied(), 0);
    assert_eq!(cluster.state_machine(1).unwrap().get("order/7"), None);
}
