use raft::{
    core::node::ProposeError,
    sim::{
        cluster::SimCluster,
        scheduler::{SimAction, SimScheduler, SimStepOutcome},
    },
    sm::mem_kv::{MemKv, MemKvCommand, MemKvSnapshot},
    types::Role,
};

const NODE_IDS: [u64; 5] = [1, 2, 3, 4, 5];
const ELECTION_TIMEOUT: u64 = 5;
const HEARTBEAT_INTERVAL: u64 = 2;
const MAX_DELIVERY_STEPS: usize = 512;

type TestCluster = SimCluster<MemKvCommand, MemKvSnapshot, MemKv>;
type TestScheduler = SimScheduler<MemKvCommand, MemKvSnapshot, MemKv>;

fn new_scheduler() -> TestScheduler {
    TestScheduler::new(TestCluster::new(
        NODE_IDS.to_vec(),
        ELECTION_TIMEOUT,
        HEARTBEAT_INTERVAL,
    ))
}

fn wait_for_leader(sim: &mut TestScheduler) -> u64 {
    sim.wait_for_leader(10, ELECTION_TIMEOUT, MAX_DELIVERY_STEPS)
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

fn assert_value(sim: &TestScheduler, node_id: u64, key: &str, expected: Option<&str>) {
    let sm = sim
        .cluster()
        .state_machine(node_id)
        .expect("state machine should exist");

    assert_eq!(sm.get(key).map(|value| value.as_str()), expected);
}

fn split_with_leader_in_majority(leader: u64) -> Vec<Vec<u64>> {
    let others = NODE_IDS
        .into_iter()
        .filter(|node_id| *node_id != leader)
        .collect::<Vec<_>>();

    vec![
        vec![leader, others[0], others[1]],
        vec![others[2], others[3]],
    ]
}

fn split_with_leader_in_minority(leader: u64) -> Vec<Vec<u64>> {
    let others = NODE_IDS
        .into_iter()
        .filter(|node_id| *node_id != leader)
        .collect::<Vec<_>>();

    vec![
        vec![others[1], others[2], others[3]],
        vec![leader, others[0]],
    ]
}

#[test]
fn leader_in_majority_partition_commits_while_minority_stays_stale() {
    let mut sim = new_scheduler();
    let leader = wait_for_leader(&mut sim);

    let groups = split_with_leader_in_majority(leader);
    let majority = groups[0].clone();
    let minority = groups[1].clone();

    sim.step(SimAction::Partition { groups });

    assert!(sim.cluster().is_partitioned(majority[0], minority[0]));

    propose_put(&mut sim, leader, "x", "1").expect("leader in majority should accept proposal");
    sim.step(SimAction::DeliverAll {
        max_steps: MAX_DELIVERY_STEPS,
    });
    advance_rounds(&mut sim, 3, HEARTBEAT_INTERVAL);

    for node_id in majority {
        assert_value(&sim, node_id, "x", Some("1"));
    }

    for node_id in minority {
        assert_value(&sim, node_id, "x", None);
    }

    assert!(sim.last_invariant_report().unwrap().is_empty());
}

#[test]
fn minority_leader_steps_down_and_healed_cluster_converges() {
    let mut sim = new_scheduler();
    let old_leader = wait_for_leader(&mut sim);

    let groups = split_with_leader_in_minority(old_leader);
    let majority = groups[0].clone();
    let minority = groups[1].clone();

    sim.step(SimAction::Partition { groups });

    assert!(sim.cluster().is_partitioned(majority[0], minority[0]));

    let mut new_leader = None;
    for _ in 0..10 {
        sim.advance_time_and_deliver(ELECTION_TIMEOUT, MAX_DELIVERY_STEPS);

        if let Some(leader) = sim.leader() {
            if leader != old_leader {
                new_leader = Some(leader);
                break;
            }
        }
    }

    let new_leader = new_leader.expect("majority side should elect a new leader");
    assert!(majority.contains(&new_leader));
    assert_eq!(
        sim.cluster().node(old_leader).unwrap().role(),
        &Role::Follower
    );

    assert_eq!(
        propose_put(&mut sim, old_leader, "stale", "nope"),
        Err(ProposeError::NotLeader)
    );

    propose_put(&mut sim, new_leader, "x", "2")
        .expect("new majority leader should accept proposal");
    sim.step(SimAction::DeliverAll {
        max_steps: MAX_DELIVERY_STEPS,
    });
    advance_rounds(&mut sim, 3, HEARTBEAT_INTERVAL);

    for node_id in majority.clone() {
        assert_value(&sim, node_id, "x", Some("2"));
    }

    for node_id in minority.clone() {
        assert_value(&sim, node_id, "x", None);
    }

    sim.step(SimAction::HealAll);
    advance_rounds(&mut sim, 6, HEARTBEAT_INTERVAL);

    for node_id in NODE_IDS {
        assert_value(&sim, node_id, "x", Some("2"));
    }

    assert!(sim.last_invariant_report().unwrap().is_empty());
}
