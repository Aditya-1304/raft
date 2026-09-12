use raft::{
    core::{
        node::RaftNode,
        read_index::{ReadIndexError, ReadState},
    },
    message::{Envelope, Message, ReadIndexResponse},
    storage::mem::MemStorage,
    types::{NodeId, Role},
};

type TestStorage = MemStorage<(), ()>;
type TestNode = RaftNode<(), (), TestStorage, TestStorage>;

const ELECTION_TIMEOUT: u64 = 5;
const HEARTBEAT_INTERVAL: u64 = 2;

fn new_node(id: u64, peers: Vec<u64>) -> TestNode {
    RaftNode::new(
        id,
        peers,
        MemStorage::new(),
        MemStorage::new(),
        ELECTION_TIMEOUT,
        HEARTBEAT_INTERVAL,
    )
}

fn take_ready(node: &mut TestNode) -> raft::core::ready::Ready<(), ()> {
    let ready = node.ready().expect("the node should have Ready output");
    node.persist_ready_to_embedded_storage(&ready).unwrap();
    node.advance_persisted(ready.id).unwrap();
    ready
}

fn take_messages(node: &mut TestNode) -> Vec<Envelope<(), ()>> {
    node.ready()
        .map(|ready| {
            let messages = ready.messages.clone();
            node.persist_ready_to_embedded_storage(&ready).unwrap();
            node.advance_persisted(ready.id).unwrap();
            messages
        })
        .unwrap_or_default()
}

fn deliver(
    n1: &mut TestNode,
    n2: &mut TestNode,
    n3: &mut TestNode,
    messages: Vec<Envelope<(), ()>>,
) {
    for message in messages {
        match message.to.get() {
            1 => n1.step(message),
            2 => n2.step(message),
            3 => n3.step(message),
            _ => unreachable!(),
        }
    }
}

fn elect_leader(n1: &mut TestNode, n2: &mut TestNode, n3: &mut TestNode) {
    n1.tick(n1.current_election_timeout());
    let prevote_requests = take_messages(n1);
    deliver(n1, n2, n3, prevote_requests);
    let mut messages = take_messages(n2);
    messages.extend(take_messages(n3));
    deliver(n1, n2, n3, messages);
    let vote_requests = take_messages(n1);
    deliver(n1, n2, n3, vote_requests);
    let mut messages = take_messages(n2);
    messages.extend(take_messages(n3));
    deliver(n1, n2, n3, messages);
    assert_eq!(n1.role(), &Role::Leader);

    let heartbeats = take_messages(n1);
    deliver(n1, n2, n3, heartbeats);
    let mut responses = take_messages(n2);
    responses.extend(take_messages(n3));
    deliver(n1, n2, n3, responses);
}

fn activate_three_node_leader(
    leader: &mut TestNode,
    follower: &mut TestNode,
    dropped_follower: &mut TestNode,
) -> u64 {
    let index = leader.propose(()).unwrap();
    let append_entries = take_messages(leader);
    let to_follower = append_entries
        .into_iter()
        .find(|message| message.to == follower.id())
        .expect("leader should send activation entry to follower");
    follower.step(to_follower);
    let response = take_messages(follower);
    deliver(leader, follower, dropped_follower, response);
    let committed = take_ready(leader);
    leader
        .advance_applied(committed.apply_through().unwrap())
        .unwrap();
    leader
        .activate_read_index(leader.current_term())
        .expect("applied current-term activation entry should enable reads");
    index
}

fn response(from: u64, to: u64, term: u64, context: &[u8]) -> Envelope<(), ()> {
    Envelope {
        from: NodeId::must(from),
        to: NodeId::must(to),
        msg: Message::ReadIndexResponse(ReadIndexResponse {
            term,
            context: context.to_vec(),
        }),
    }
}

#[test]
/// Catches the safety bug where becoming leader alone permits a read before a
/// current-term activation entry has been applied.
fn leader_role_alone_does_not_activate_read_index() {
    let mut node = new_node(1, Vec::new());
    node.tick(node.current_election_timeout());
    take_ready(&mut node);

    assert_eq!(
        node.read_index(b"before-activation".to_vec()),
        Err(ReadIndexError::NotActivated {
            term: node.current_term()
        })
    );
    assert_eq!(
        node.activate_read_index(node.current_term()),
        Err(ReadIndexError::CurrentTermNotCommitted {
            term: node.current_term()
        })
    );
}

#[test]
/// Catches the routing bug where a follower accepts a local read barrier and
/// can produce a result without owning leadership.
fn read_index_is_admitted_only_by_leader() {
    let mut follower = new_node(1, vec![2]);

    assert_eq!(
        follower.read_index(b"follower".to_vec()),
        Err(ReadIndexError::NotLeader)
    );
    assert_eq!(
        follower.activate_read_index(0),
        Err(ReadIndexError::NotLeader)
    );
}

#[test]
/// Catches the liveness/safety bug where an incomplete quorum is reported as a
/// usable read state instead of remaining pending.
fn read_index_without_quorum_does_not_emit_a_read_state() {
    let mut leader = new_node(1, vec![2, 3]);
    let mut n2 = new_node(2, vec![1, 3]);
    let mut n3 = new_node(3, vec![1, 2]);
    elect_leader(&mut leader, &mut n2, &mut n3);
    activate_three_node_leader(&mut leader, &mut n2, &mut n3);

    leader.read_index(b"no-quorum".to_vec()).unwrap();
    let ready = take_ready(&mut leader);

    assert_eq!(ready.read_states, Vec::<ReadState>::new());
    assert_eq!(ready.messages.len(), 2);
    assert!(leader.ready().is_none());
}

#[test]
/// Catches context confusion and delayed-message bugs that could complete one
/// request using a different context or an acknowledgement from an old term.
fn stale_wrong_context_and_old_term_responses_cannot_complete_a_read() {
    let mut leader = new_node(1, vec![2, 3]);
    let mut n2 = new_node(2, vec![1, 3]);
    let mut n3 = new_node(3, vec![1, 2]);
    elect_leader(&mut leader, &mut n2, &mut n3);
    activate_three_node_leader(&mut leader, &mut n2, &mut n3);

    let context = b"matching-context";
    leader.read_index(context.to_vec()).unwrap();
    let request_ready = take_ready(&mut leader);
    assert_eq!(request_ready.read_states, Vec::<ReadState>::new());

    leader.step(response(2, 1, leader.current_term(), b"wrong-context"));
    assert!(leader.ready().is_none());
    leader.step(response(
        2,
        1,
        leader.current_term().saturating_sub(1),
        context,
    ));
    assert!(leader.ready().is_none());
    leader.step(response(2, 1, leader.current_term(), context));
    let ready = take_ready(&mut leader);
    assert_eq!(
        ready.read_states,
        vec![ReadState {
            request_ctx: context.to_vec(),
            index: leader.commit_index(),
            term: leader.current_term(),
        }]
    );
}

#[test]
/// Catches the stale-leader bug where a higher-term response leaves old reads
/// active after the core has lost leadership.
fn higher_term_response_invalidates_pending_reads_and_activation() {
    let mut leader = new_node(1, vec![2, 3]);
    let mut n2 = new_node(2, vec![1, 3]);
    let mut n3 = new_node(3, vec![1, 2]);
    elect_leader(&mut leader, &mut n2, &mut n3);
    activate_three_node_leader(&mut leader, &mut n2, &mut n3);

    leader.read_index(b"must-invalidate".to_vec()).unwrap();
    take_ready(&mut leader);
    let higher_term = leader.current_term() + 1;
    leader.step(response(2, 1, higher_term, b"must-invalidate"));

    assert_eq!(leader.role(), &Role::Follower);
    assert_eq!(leader.read_index_activation_term(), None);
    assert_eq!(
        leader.read_index(b"after-term-change".to_vec()),
        Err(ReadIndexError::NotLeader)
    );
    let ready = take_ready(&mut leader);
    assert!(ready.read_states.is_empty());
}

#[test]
/// Catches the unsafe-index bug where a read barrier returns the logical log
/// tail instead of the index proven committed by the quorum.
fn read_state_uses_safe_committed_index_not_uncommitted_log_tail() {
    let mut leader = new_node(1, vec![2, 3]);
    let mut n2 = new_node(2, vec![1, 3]);
    let mut n3 = new_node(3, vec![1, 2]);
    elect_leader(&mut leader, &mut n2, &mut n3);
    let activation_index = activate_three_node_leader(&mut leader, &mut n2, &mut n3);

    leader.propose(()).unwrap();
    take_ready(&mut leader);
    assert!(leader.last_log_index() > leader.commit_index());

    let context = b"safe-index";
    leader.read_index(context.to_vec()).unwrap();
    let requests = take_ready(&mut leader).messages;
    let to_follower = requests
        .into_iter()
        .find(|message| message.to == n2.id())
        .expect("read request should target follower");
    n2.step(to_follower);
    let responses = take_messages(&mut n2);
    deliver(&mut leader, &mut n2, &mut n3, responses);
    let ready = take_ready(&mut leader);

    assert_eq!(ready.read_states.len(), 1);
    assert_eq!(ready.read_states[0].index, activation_index);
    assert_eq!(ready.read_states[0].term, leader.current_term());
}

#[test]
/// Catches the Ready ownership bug where a confirmed read state disappears if
/// the host polls Ready more than once before acknowledging persistence.
fn confirmed_read_state_is_retained_until_ready_acknowledgement() {
    let mut node = new_node(1, Vec::new());
    node.tick(node.current_election_timeout());
    take_ready(&mut node);
    let activation_index = node.propose(()).unwrap();
    take_ready(&mut node);
    node.advance_applied(activation_index).unwrap();
    node.activate_read_index(node.current_term()).unwrap();

    node.read_index(b"retained".to_vec()).unwrap();
    let first = node.ready().expect("completed read should be in Ready");
    let repeated = node.ready().expect("unacknowledged read state must remain");
    assert_eq!(repeated, first);
    assert_eq!(first.read_states.len(), 1);

    node.persist_ready_to_embedded_storage(&first).unwrap();
    node.advance_persisted(first.id).unwrap();
    assert!(node.ready().is_none());
}
