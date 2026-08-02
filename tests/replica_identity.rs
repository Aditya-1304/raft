use raft::types::ReplicaId;

#[test]
fn replica_id_rejects_the_reserved_zero_value() {
    assert!(ReplicaId::new(0).is_none());
}

#[test]
fn replica_id_roundtrips_its_durable_scalar_value() {
    let replica_id = ReplicaId::new(42).expect("non-zero replica ID");

    assert_eq!(replica_id.get(), 42);
    assert_eq!(u64::from(replica_id), 42);
}
