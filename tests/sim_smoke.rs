use raft::{
    core::node::ProposeError,
    sim::{
        cluster::SimCluster,
        recorder::SimEvent,
        scheduler::{SimAction, SimScheduler, SimStepOutcome},
    },
    sm::mem_kv::{MemKv, MemKvCommand, MemKvSnapshot},
};

const NODE_IDS: [u64; 3] = [1, 2, 3];
const ELECTION_TIMEOUT: u64 = 5;
const HEARTBEAT_INTERVAL: u64 = 2;
const MAX_DELIVERY_STEPS: usize = 1024;

type TestCluster = SimCluster<MemKvCommand, MemKvSnapshot, MemKv>;
type TestScheduler = SimScheduler<MemKvCommand, MemKvSnapshot, MemKv>;

fn new_scheduler(seed: u64) -> TestScheduler {
    TestScheduler::with_seed(
        TestCluster::new(NODE_IDS.to_vec(), ELECTION_TIMEOUT, HEARTBEAT_INTERVAL),
        seed,
    )
}

fn wait_for_leader(sim: &mut TestScheduler) -> u64 {
    sim.wait_for_leader(12, ELECTION_TIMEOUT, MAX_DELIVERY_STEPS)
        .expect("cluster should elect a leader")
}

fn advance_rounds(sim: &mut TestScheduler, rounds: usize, ticks: u64) {
    for _ in 0..rounds {
        sim.advance_time_and_deliver(ticks, MAX_DELIVERY_STEPS);
    }
}

fn propose_put(
    sim: &mut TestScheduler,
    node_id: u64,
    key: &str,
    value: &str,
) -> Result<u64, ProposeError> {
    match sim.step(SimAction::Propose {
        node_id,
        command: MemKvCommand::Put {
            key: key.to_owned(),
            value: value.to_owned(),
        },
    }) {
        SimStepOutcome::Proposed { result } => result,
        _ => unreachable!("propose action must return SimStepOutcome::Proposed"),
    }
}

fn node_value(sim: &TestScheduler, node_id: u64, key: &str) -> Option<String> {
    sim.cluster()
        .state_machine(node_id)
        .expect("state machine should exist")
        .get(key)
        .cloned()
}

fn values_for_key(sim: &TestScheduler, key: &str) -> Vec<Option<String>> {
    NODE_IDS
        .iter()
        .map(|node_id| node_value(sim, *node_id, key))
        .collect()
}

fn assert_value_on_all_nodes(sim: &TestScheduler, key: &str, expected: Option<&str>) {
    for node_id in NODE_IDS {
        assert_eq!(node_value(sim, node_id, key).as_deref(), expected);
    }
}

fn run_seeded_script(seed: u64) -> (u64, String, String, Vec<Option<String>>) {
    let mut sim = new_scheduler(seed);
    let leader = wait_for_leader(&mut sim);

    sim.step(SimAction::SetDelayRange {
        min_ticks: 0,
        max_ticks: 2,
    });

    propose_put(&mut sim, leader, "seeded", "value").expect("leader should accept proposal");

    match sim.step(SimAction::DuplicateNext) {
        SimStepOutcome::Duplicated { duplicated } => assert!(duplicated),
        _ => unreachable!("duplicate action must return SimStepOutcome::Duplicated"),
    }

    advance_rounds(&mut sim, 8, 1);

    (
        leader,
        format!("{:?}", sim.recorder().events()),
        format!("{:?}", sim.trace()),
        values_for_key(&sim, "seeded"),
    )
}

#[test]
fn smoke_elects_leader_replicates_and_records_trace() {
    let mut sim = new_scheduler(7);

    let leader = wait_for_leader(&mut sim);
    assert_eq!(sim.leader(), Some(leader));

    let index = propose_put(&mut sim, leader, "smoke", "ok")
        .expect("leader should accept replicated command");
    assert!(index >= 1);

    sim.step(SimAction::DeliverAll {
        max_steps: MAX_DELIVERY_STEPS,
    });
    advance_rounds(&mut sim, 4, HEARTBEAT_INTERVAL);

    assert_value_on_all_nodes(&sim, "smoke", Some("ok"));
    assert_eq!(sim.pending_messages(), 0);
    assert_eq!(sim.due_messages(), 0);

    assert!(!sim.trace().is_empty());
    assert!(!sim.recorder().is_empty());
    assert!(
        sim.trace()
            .iter()
            .all(|entry| entry.invariant_report.is_empty())
    );

    assert!(
        sim.recorder()
            .events()
            .iter()
            .any(|event| matches!(event, SimEvent::ProposalResult { .. }))
    );
    assert!(
        sim.recorder()
            .events()
            .iter()
            .any(|event| matches!(event, SimEvent::MessageDelivered { .. }))
    );
}

#[test]
fn smoke_same_seed_replays_same_trace() {
    let left = run_seeded_script(0xabcddcba);
    let right = run_seeded_script(0xabcddcba);

    assert_eq!(left, right);
}

#[test]
fn smoke_delayed_and_duplicated_delivery_still_converges() {
    let mut sim = new_scheduler(99);
    let leader = wait_for_leader(&mut sim);

    sim.step(SimAction::SetDelayRange {
        min_ticks: 1,
        max_ticks: 3,
    });

    propose_put(&mut sim, leader, "dup", "1").expect("leader should accept proposal");

    match sim.step(SimAction::DuplicateNext) {
        SimStepOutcome::Duplicated { duplicated } => assert!(duplicated),
        _ => unreachable!("duplicate action must return SimStepOutcome::Duplicated"),
    }

    assert!(sim.pending_messages() > 0);
    assert_eq!(sim.due_messages(), 0);

    advance_rounds(&mut sim, 10, 1);

    assert_value_on_all_nodes(&sim, "dup", Some("1"));
    assert!(
        sim.trace()
            .iter()
            .all(|entry| entry.invariant_report.is_empty())
    );
    assert!(
        sim.recorder()
            .events()
            .iter()
            .any(|event| matches!(event, SimEvent::MessageDuplicated { .. }))
    );
    assert!(
        sim.recorder()
            .events()
            .iter()
            .any(|event| matches!(event, SimEvent::MessageScheduled { .. }))
    );
}
