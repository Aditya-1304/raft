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
const MAX_DELIVERY_STEPS: usize = 1024;

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
    sim.wait_for_leader(12, ELECTION_TIMEOUT, MAX_DELIVERY_STEPS)
        .expect("cluster should elect a leader")
}

fn wait_for_group_leader(sim: &mut TestScheduler, group: &[u64]) -> u64 {
    for _ in 0..12 {
        advance_rounds(sim, 1, ELECTION_TIMEOUT);

        let leaders = group
            .iter()
            .copied()
            .filter(|node_id| !sim.cluster().is_crashed(*node_id))
            .filter(|node_id| {
                sim.cluster()
                    .node(*node_id)
                    .map(|node| node.role() == &Role::Leader)
                    .unwrap_or(false)
            })
            .collect::<Vec<_>>();

        if leaders.len() == 1 {
            return leaders[0];
        }
    }

    panic!("group should elect exactly one leader");
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

fn assert_value_on_nodes(sim: &TestScheduler, node_ids: &[u64], key: &str, expected: Option<&str>) {
    for &node_id in node_ids {
        assert_value(sim, node_id, key, expected);
    }
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
fn partition_crash_restart_and_heal_still_converges() {
    let mut sim = new_scheduler();
    let leader = wait_for_leader(&mut sim);

    propose_put(&mut sim, leader, "seed", "1").expect("initial leader should accept proposal");
    sim.step(SimAction::DeliverAll {
        max_steps: MAX_DELIVERY_STEPS,
    });
    advance_rounds(&mut sim, 3, HEARTBEAT_INTERVAL);

    assert_value_on_nodes(&sim, &NODE_IDS, "seed", Some("1"));

    let groups = split_with_leader_in_majority(leader);
    let majority = groups[0].clone();
    let minority = groups[1].clone();
    let restarted_node = minority[0];

    sim.step(SimAction::Partition { groups });

    propose_put(&mut sim, leader, "majority", "2")
        .expect("leader in majority should still accept proposal");
    sim.step(SimAction::DeliverAll {
        max_steps: MAX_DELIVERY_STEPS,
    });
    advance_rounds(&mut sim, 3, HEARTBEAT_INTERVAL);

    sim.step(SimAction::Crash {
        node_id: restarted_node,
    });
    advance_rounds(&mut sim, 2, HEARTBEAT_INTERVAL);

    sim.step(SimAction::Restart {
        node_id: restarted_node,
    });
    advance_rounds(&mut sim, 2, HEARTBEAT_INTERVAL);

    assert_value_on_nodes(&sim, &majority, "majority", Some("2"));
    assert_value_on_nodes(&sim, &minority, "majority", None);
    assert_value_on_nodes(&sim, &NODE_IDS, "seed", Some("1"));

    sim.step(SimAction::HealAll);
    let _ = wait_for_leader(&mut sim);
    advance_rounds(&mut sim, 8, HEARTBEAT_INTERVAL);

    assert_value_on_nodes(&sim, &NODE_IDS, "seed", Some("1"));
    assert_value_on_nodes(&sim, &NODE_IDS, "majority", Some("2"));
    assert!(!sim.cluster().is_crashed(restarted_node));
    assert!(sim.last_invariant_report().unwrap().is_empty());
}

#[test]
fn old_leader_crashes_in_minority_and_catches_up_after_new_term() {
    let mut sim = new_scheduler();
    let old_leader = wait_for_leader(&mut sim);

    propose_put(&mut sim, old_leader, "seed", "1").expect("initial leader should accept proposal");
    sim.step(SimAction::DeliverAll {
        max_steps: MAX_DELIVERY_STEPS,
    });
    advance_rounds(&mut sim, 3, HEARTBEAT_INTERVAL);

    let groups = split_with_leader_in_minority(old_leader);
    let majority = groups[0].clone();
    let minority = groups[1].clone();
    let minority_follower = minority
        .iter()
        .copied()
        .find(|node_id| *node_id != old_leader)
        .expect("minority should include a follower");

    sim.step(SimAction::Partition { groups });

    let new_leader = wait_for_group_leader(&mut sim, &majority);
    assert_ne!(new_leader, old_leader);

    sim.step(SimAction::Crash {
        node_id: old_leader,
    });
    advance_rounds(&mut sim, 2, HEARTBEAT_INTERVAL);

    propose_put(&mut sim, new_leader, "recovery", "3")
        .expect("majority-side leader should accept proposal");
    sim.step(SimAction::DeliverAll {
        max_steps: MAX_DELIVERY_STEPS,
    });
    advance_rounds(&mut sim, 3, HEARTBEAT_INTERVAL);

    assert_value_on_nodes(&sim, &majority, "recovery", Some("3"));
    assert_value(&sim, minority_follower, "recovery", None);

    sim.step(SimAction::Restart {
        node_id: old_leader,
    });
    advance_rounds(&mut sim, 2, HEARTBEAT_INTERVAL);

    sim.step(SimAction::HealAll);
    let healed_leader = wait_for_leader(&mut sim);
    advance_rounds(&mut sim, 8, HEARTBEAT_INTERVAL);

    assert_value_on_nodes(&sim, &NODE_IDS, "seed", Some("1"));
    assert_value_on_nodes(&sim, &NODE_IDS, "recovery", Some("3"));
    assert_eq!(
        sim.cluster().node(healed_leader).unwrap().role(),
        &Role::Leader
    );
    assert!(sim.last_invariant_report().unwrap().is_empty());
}
