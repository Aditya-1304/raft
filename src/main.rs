use std::{
    collections::BTreeMap,
    env, fs, io,
    io::{BufRead, BufReader, Write},
    net::{SocketAddr, TcpListener, TcpStream},
    path::PathBuf,
    sync::mpsc::{self, Receiver, RecvTimeoutError, Sender, TryRecvError},
    thread,
    time::Duration,
};

use raft::{
    core::node::{ProposeError, RaftNode},
    runtime::{
        RuntimeConfig, RuntimeError,
        server::{RuntimeServer, SnapshotPolicy},
        ticker_wall::WallTicker,
        transport_tcp::{TcpEndpoint, TcpTransport},
    },
    storage::{
        codec::{SnapshotCodec, U64Codec},
        file::{FileLogStore, FileSnapshotStore, FileStableStore},
    },
    traits::{
        snapshot_store::SnapshotStore,
        state_machine::{SnapshotableStateMachine, StateMachine},
    },
    types::{LogIndex, NodeId, Role},
};

const DEFAULT_TICK_MS: u64 = 100;
const DEFAULT_ELECTION_TIMEOUT_TICKS: u64 = 10;
const DEFAULT_HEARTBEAT_INTERVAL_TICKS: u64 = 3;
const DEFAULT_SNAPSHOT_EVERY: u64 = 5;

const ADMIN_REQUEST_TIMEOUT_MS: u64 = 5_000;
const DEFAULT_WATCH_INTERVAL_MS: u64 = 1_000;
const DEFAULT_PROPOSE_ATTEMPTS: usize = 20;
const DEFAULT_PROPOSE_RETRY_MS: u64 = 250;

type DemoCommand = u64;
type DemoLogStore = FileLogStore<DemoCommand, U64Codec>;
type DemoSnapshotStore = FileSnapshotStore<CounterSnapshot, CounterSnapshotCodec>;
type DemoTransport = TcpTransport<DemoCommand, CounterSnapshot, U64Codec, CounterSnapshotCodec>;
type DemoServer = RuntimeServer<
    DemoCommand,
    CounterSnapshot,
    DemoLogStore,
    FileStableStore,
    DemoSnapshotStore,
    CounterStateMachine,
    DemoTransport,
    WallTicker,
>;

#[derive(Debug, Clone, PartialEq, Eq)]
struct CounterSnapshot {
    total: u64,
    last_applied: LogIndex,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct CounterOutput {
    delta: u64,
    total: u64,
}

#[derive(Debug, Clone, Default)]
struct CounterStateMachine {
    total: u64,
    last_applied: LogIndex,
}

#[derive(Debug, Clone, Copy, Default)]
struct CounterSnapshotCodec;

#[derive(Debug, Clone)]
struct ClusterMember {
    node_id: NodeId,
    raft_addr: SocketAddr,
    admin_addr: SocketAddr,
}

#[derive(Debug, Clone)]
struct ClusterConfig {
    members: BTreeMap<NodeId, ClusterMember>,
}

#[derive(Debug, Clone)]
struct NodeOptions {
    id: NodeId,
    data_dir: PathBuf,
    cluster: ClusterConfig,
    tick_ms: u64,
    election_timeout_ticks: u64,
    heartbeat_interval_ticks: u64,
    snapshot_every: u64,
}

#[derive(Debug, Clone)]
struct ClusterOnlyOptions {
    cluster: ClusterConfig,
}

#[derive(Debug, Clone)]
struct WatchOptions {
    cluster: ClusterConfig,
    interval_ms: u64,
}

#[derive(Debug, Clone)]
struct ProposeOptions {
    cluster: ClusterConfig,
    command: DemoCommand,
    target_node: Option<NodeId>,
    attempts: usize,
    retry_ms: u64,
}

#[derive(Debug, Clone)]
struct NodeActionOptions {
    cluster: ClusterConfig,
    node_id: NodeId,
}

enum CliCommand {
    Node(NodeOptions),
    Status(ClusterOnlyOptions),
    Watch(WatchOptions),
    Propose(ProposeOptions),
    Snapshot(NodeActionOptions),
    Shutdown(NodeActionOptions),
    Help,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct NodeStatus {
    node_id: NodeId,
    raft_addr: SocketAddr,
    admin_addr: SocketAddr,
    role: Role,
    leader_id: Option<NodeId>,
    current_term: u64,
    voted_for: Option<NodeId>,
    commit_index: LogIndex,
    last_applied: LogIndex,
    first_log_index: LogIndex,
    last_log_index: LogIndex,
    last_log_term: u64,
    snapshot_index: LogIndex,
    snapshot_term: u64,
    current_election_timeout: u64,
    state_value: u64,
    ticks_observed: u64,
    inbound_messages_processed: u64,
    outbound_messages_sent: u64,
    committed_entries_applied: u64,
    snapshots_restored: u64,
    snapshot_policy: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ObservedState {
    role: Role,
    leader_id: Option<NodeId>,
    current_term: u64,
    commit_index: LogIndex,
    last_applied: LogIndex,
    first_log_index: LogIndex,
    last_log_index: LogIndex,
    snapshot_index: LogIndex,
    state_value: u64,
}

#[derive(Debug)]
enum AdminRequest {
    Status {
        reply_to: Sender<AdminResponse>,
    },
    Propose {
        command: DemoCommand,
        reply_to: Sender<AdminResponse>,
    },
    ForceSnapshot {
        reply_to: Sender<AdminResponse>,
    },
    Shutdown {
        reply_to: Sender<AdminResponse>,
    },
}

#[derive(Debug, Clone)]
enum AdminResponse {
    Status(NodeStatus),
    Accepted {
        node_id: NodeId,
        leader_id: Option<NodeId>,
        log_index: LogIndex,
    },
    NotLeader {
        node_id: NodeId,
        leader_id: Option<NodeId>,
    },
    Snapshot {
        node_id: NodeId,
        snapshot_index: Option<LogIndex>,
    },
    Shutdown {
        node_id: NodeId,
    },
    Error {
        node_id: Option<NodeId>,
        message: String,
    },
}

#[derive(Debug)]
struct StatusProbe {
    node_id: NodeId,
    admin_addr: SocketAddr,
    result: Result<NodeStatus, String>,
}

#[derive(Debug)]
enum ProposeOutcome {
    Accepted {
        node_id: NodeId,
        leader_id: Option<NodeId>,
        log_index: LogIndex,
    },
    NotLeader {
        node_id: NodeId,
        leader_id: Option<NodeId>,
    },
}

impl CounterStateMachine {
    fn total(&self) -> u64 {
        self.total
    }
}

impl StateMachine<DemoCommand> for CounterStateMachine {
    type Output = CounterOutput;

    fn apply(&mut self, index: LogIndex, cmd: &DemoCommand) -> Self::Output {
        self.last_applied = index;
        self.total = self.total.saturating_add(*cmd);

        CounterOutput {
            delta: *cmd,
            total: self.total,
        }
    }
}

impl SnapshotableStateMachine<DemoCommand> for CounterStateMachine {
    type Snapshot = CounterSnapshot;

    fn snapshot(&self) -> Self::Snapshot {
        CounterSnapshot {
            total: self.total,
            last_applied: self.last_applied,
        }
    }

    fn restore(&mut self, snapshot: Self::Snapshot) {
        self.total = snapshot.total;
        self.last_applied = snapshot.last_applied;
    }

    fn last_applied(&self) -> LogIndex {
        self.last_applied
    }
}

impl SnapshotCodec<CounterSnapshot> for CounterSnapshotCodec {
    fn encode(&self, snapshot: &CounterSnapshot) -> io::Result<Vec<u8>> {
        let mut bytes = Vec::with_capacity(16);
        bytes.extend_from_slice(&snapshot.total.to_le_bytes());
        bytes.extend_from_slice(&snapshot.last_applied.to_le_bytes());
        Ok(bytes)
    }

    fn decode(&self, bytes: &[u8]) -> io::Result<CounterSnapshot> {
        if bytes.len() != 16 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("expected 16 bytes for CounterSnapshot, got {}", bytes.len()),
            ));
        }

        let total = u64::from_le_bytes(bytes[0..8].try_into().expect("slice length checked"));
        let last_applied =
            u64::from_le_bytes(bytes[8..16].try_into().expect("slice length checked"));

        Ok(CounterSnapshot {
            total,
            last_applied,
        })
    }
}

impl ClusterConfig {
    fn new(entries: Vec<ClusterMember>) -> io::Result<Self> {
        let mut members = BTreeMap::new();

        for entry in entries {
            if members.insert(entry.node_id, entry).is_some() {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "duplicate node id in cluster config",
                ));
            }
        }

        if members.is_empty() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "cluster config must contain at least one node",
            ));
        }

        Ok(Self { members })
    }

    fn member(&self, node_id: NodeId) -> io::Result<&ClusterMember> {
        self.members.get(&node_id).ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("cluster config missing node {node_id}"),
            )
        })
    }

    fn peer_raft_addrs(&self, local_id: NodeId) -> BTreeMap<NodeId, SocketAddr> {
        self.members
            .iter()
            .filter(|(node_id, _)| **node_id != local_id)
            .map(|(node_id, member)| (*node_id, member.raft_addr))
            .collect()
    }

    fn peer_ids(&self, local_id: NodeId) -> Vec<NodeId> {
        self.members
            .keys()
            .copied()
            .filter(|node_id| *node_id != local_id)
            .collect()
    }

    fn ordered_ids(&self) -> Vec<NodeId> {
        self.members.keys().copied().collect()
    }
}

fn main() -> io::Result<()> {
    match parse_cli()? {
        CliCommand::Node(options) => run_node(options),
        CliCommand::Status(options) => run_status(options),
        CliCommand::Watch(options) => run_watch(options),
        CliCommand::Propose(options) => run_propose(options),
        CliCommand::Snapshot(options) => run_snapshot(options),
        CliCommand::Shutdown(options) => run_shutdown(options),
        CliCommand::Help => {
            print_usage();
            Ok(())
        }
    }
}

fn run_node(options: NodeOptions) -> io::Result<()> {
    fs::create_dir_all(&options.data_dir)?;

    let member = options.cluster.member(options.id)?.clone();
    let (admin_tx, admin_rx) = mpsc::channel();

    spawn_admin_listener(member.admin_addr, admin_tx.clone())?;

    let mut server = open_runtime_server(&options)?;
    let mut previous = None;

    println!(
        "node {} starting | raft={} admin={} data_dir={} tick={}ms election={} heartbeat={} snapshot_every={}",
        member.node_id,
        member.raft_addr,
        member.admin_addr,
        options.data_dir.display(),
        options.tick_ms,
        options.election_timeout_ticks,
        options.heartbeat_interval_ticks,
        options.snapshot_every
    );

    println!("node {} peers:", member.node_id);
    for peer_id in options.cluster.ordered_ids() {
        let peer = options.cluster.member(peer_id)?;
        println!(
            "  node {} -> raft={} admin={}",
            peer.node_id, peer.raft_addr, peer.admin_addr
        );
    }

    loop {
        if drain_admin_requests(&member, &mut server, &admin_rx)? {
            println!("node {} shutting down", member.node_id);
            return Ok(());
        }

        match server.run_once() {
            Ok(_) => {}
            Err(err) => return Err(runtime_error_to_io(err, member.node_id)),
        }

        for (index, output) in server.take_applied_outputs() {
            println!(
                "node {} applied index {}: +{} => total {}",
                member.node_id, index, output.delta, output.total
            );
        }

        let status = snapshot_local_status(&member, &server);
        maybe_log_runtime_change(&mut previous, &status);
    }
}

fn run_status(options: ClusterOnlyOptions) -> io::Result<()> {
    let probes = query_cluster_statuses(&options.cluster);
    print_status_table(&probes);
    Ok(())
}

fn run_watch(options: WatchOptions) -> io::Result<()> {
    loop {
        let probes = query_cluster_statuses(&options.cluster);
        print_status_table(&probes);
        println!();
        thread::sleep(Duration::from_millis(options.interval_ms));
    }
}

fn run_propose(options: ProposeOptions) -> io::Result<()> {
    let mut leader_hint = options.target_node;

    if leader_hint.is_none() {
        let probes = query_cluster_statuses(&options.cluster);
        leader_hint = probes.iter().find_map(|probe| {
            probe.result.as_ref().ok().and_then(|status| {
                if status.role == Role::Leader {
                    Some(status.node_id)
                } else {
                    None
                }
            })
        });
    }

    for _ in 0..options.attempts {
        let ordered = candidate_order(&options.cluster, leader_hint, options.target_node);

        for node_id in ordered {
            let member = options.cluster.member(node_id)?;
            match send_propose_request(member.admin_addr, options.command) {
                Ok(ProposeOutcome::Accepted {
                    node_id,
                    leader_id,
                    log_index,
                }) => {
                    println!(
                        "accepted by node {} at log index {} (leader={:?})",
                        node_id, log_index, leader_id
                    );
                    return Ok(());
                }
                Ok(ProposeOutcome::NotLeader { node_id, leader_id }) => {
                    println!(
                        "node {} rejected proposal as not leader (leader_hint={:?})",
                        node_id, leader_id
                    );
                    leader_hint = leader_id;
                }
                Err(err) => {
                    eprintln!("proposal attempt via node {} failed: {}", node_id, err);
                }
            }
        }

        thread::sleep(Duration::from_millis(options.retry_ms));
    }

    Err(io::Error::new(
        io::ErrorKind::TimedOut,
        "proposal was not accepted by any node",
    ))
}

fn run_snapshot(options: NodeActionOptions) -> io::Result<()> {
    let member = options.cluster.member(options.node_id)?;
    let response = send_simple_admin_command(member.admin_addr, "snapshot")?;
    match require_kind(&response, "snapshot")? {
        "snapshot" => {
            println!(
                "node {} snapshot result: snapshot_index={}",
                options.node_id,
                response
                    .get("snapshot_index")
                    .map(String::as_str)
                    .unwrap_or("none")
            );
            Ok(())
        }
        _ => unreachable!(),
    }
}

fn run_shutdown(options: NodeActionOptions) -> io::Result<()> {
    let member = options.cluster.member(options.node_id)?;
    let response = send_simple_admin_command(member.admin_addr, "shutdown")?;
    match require_kind(&response, "shutdown")? {
        "shutdown" => {
            println!("node {} acknowledged shutdown", options.node_id);
            Ok(())
        }
        _ => unreachable!(),
    }
}

fn open_runtime_server(options: &NodeOptions) -> io::Result<DemoServer> {
    let member = options.cluster.member(options.id)?;
    let stable_store = FileStableStore::open(options.data_dir.join("hard_state.txt"))?;
    let log_store = DemoLogStore::open(options.data_dir.join("log.txt"), U64Codec)?;
    let snapshot_store =
        DemoSnapshotStore::open(options.data_dir.join("snapshot.txt"), CounterSnapshotCodec)?;

    let raft = RaftNode::new(
        member.node_id,
        options.cluster.peer_ids(member.node_id),
        log_store,
        stable_store,
        options.election_timeout_ticks,
        options.heartbeat_interval_ticks,
    );

    let TcpEndpoint {
        transport,
        inbound,
        local_addr: _,
    } = TcpTransport::bind(
        member.node_id,
        member.raft_addr,
        options.cluster.peer_raft_addrs(member.node_id),
        U64Codec,
        CounterSnapshotCodec,
    )?;

    let ticker = WallTicker::new(Duration::from_millis(options.tick_ms));
    let config = RuntimeConfig::new(64, Duration::from_millis(1));

    let snapshot_policy = if options.snapshot_every == 0 {
        SnapshotPolicy::Never
    } else {
        SnapshotPolicy::EveryAppliedEntries(options.snapshot_every)
    };

    RuntimeServer::with_snapshot_policy(
        raft,
        snapshot_store,
        CounterStateMachine::default(),
        transport,
        ticker,
        inbound,
        config,
        snapshot_policy,
    )
    .map_err(|err| runtime_error_to_io(err, member.node_id))
}

fn spawn_admin_listener(bind_addr: SocketAddr, tx: Sender<AdminRequest>) -> io::Result<()> {
    let listener = TcpListener::bind(bind_addr)?;

    thread::spawn(move || {
        for stream in listener.incoming() {
            match stream {
                Ok(stream) => {
                    let tx = tx.clone();
                    thread::spawn(move || {
                        if let Err(err) = handle_admin_connection(stream, tx) {
                            eprintln!("admin connection error: {}", err);
                        }
                    });
                }
                Err(err) => {
                    eprintln!("admin listener accept error: {}", err);
                }
            }
        }
    });

    Ok(())
}

fn handle_admin_connection(stream: TcpStream, tx: Sender<AdminRequest>) -> io::Result<()> {
    stream.set_read_timeout(Some(Duration::from_millis(ADMIN_REQUEST_TIMEOUT_MS)))?;
    stream.set_write_timeout(Some(Duration::from_millis(ADMIN_REQUEST_TIMEOUT_MS)))?;

    let mut reader = BufReader::new(stream.try_clone()?);
    let mut line = String::new();
    reader.read_line(&mut line)?;

    let request = parse_admin_request_line(line.trim())?;
    let (reply_tx, reply_rx) = mpsc::channel();

    let request = match request {
        ParsedAdminRequest::Status => AdminRequest::Status { reply_to: reply_tx },
        ParsedAdminRequest::Propose { command } => AdminRequest::Propose {
            command,
            reply_to: reply_tx,
        },
        ParsedAdminRequest::Snapshot => AdminRequest::ForceSnapshot { reply_to: reply_tx },
        ParsedAdminRequest::Shutdown => AdminRequest::Shutdown { reply_to: reply_tx },
    };

    tx.send(request)
        .map_err(|_| io::Error::new(io::ErrorKind::BrokenPipe, "node control loop is gone"))?;

    let response = match reply_rx.recv_timeout(Duration::from_millis(ADMIN_REQUEST_TIMEOUT_MS)) {
        Ok(response) => response,
        Err(RecvTimeoutError::Timeout) => AdminResponse::Error {
            node_id: None,
            message: "admin request timed out".to_string(),
        },
        Err(RecvTimeoutError::Disconnected) => AdminResponse::Error {
            node_id: None,
            message: "node control loop disconnected".to_string(),
        },
    };

    write_admin_response(stream, &response)
}

fn drain_admin_requests(
    member: &ClusterMember,
    server: &mut DemoServer,
    admin_rx: &Receiver<AdminRequest>,
) -> io::Result<bool> {
    loop {
        match admin_rx.try_recv() {
            Ok(request) => {
                if handle_admin_request(member, server, request)? {
                    return Ok(true);
                }
            }
            Err(TryRecvError::Empty) => return Ok(false),
            Err(TryRecvError::Disconnected) => return Ok(false),
        }
    }
}

fn handle_admin_request(
    member: &ClusterMember,
    server: &mut DemoServer,
    request: AdminRequest,
) -> io::Result<bool> {
    match request {
        AdminRequest::Status { reply_to } => {
            let _ = reply_to.send(AdminResponse::Status(snapshot_local_status(member, server)));
            Ok(false)
        }
        AdminRequest::Propose { command, reply_to } => {
            let response = if server.raft().role() != &Role::Leader {
                AdminResponse::NotLeader {
                    node_id: member.node_id,
                    leader_id: server.raft().leader_id(),
                }
            } else {
                match server.raft_mut().propose(command) {
                    Ok(log_index) => {
                        server
                            .run_until_idle(64)
                            .map_err(|err| runtime_error_to_io(err, member.node_id))?;

                        AdminResponse::Accepted {
                            node_id: member.node_id,
                            leader_id: server.raft().leader_id(),
                            log_index,
                        }
                    }
                    Err(ProposeError::NotLeader) => AdminResponse::NotLeader {
                        node_id: member.node_id,
                        leader_id: server.raft().leader_id(),
                    },
                }
            };

            let _ = reply_to.send(response);
            Ok(false)
        }
        AdminRequest::ForceSnapshot { reply_to } => {
            let snapshot_index = server
                .force_snapshot()
                .map_err(|err| runtime_error_to_io(err, member.node_id))?;

            let _ = reply_to.send(AdminResponse::Snapshot {
                node_id: member.node_id,
                snapshot_index,
            });
            Ok(false)
        }
        AdminRequest::Shutdown { reply_to } => {
            let _ = reply_to.send(AdminResponse::Shutdown {
                node_id: member.node_id,
            });
            Ok(true)
        }
    }
}

fn snapshot_local_status(member: &ClusterMember, server: &DemoServer) -> NodeStatus {
    let hard_state = server.raft().hard_state();

    NodeStatus {
        node_id: member.node_id,
        raft_addr: member.raft_addr,
        admin_addr: member.admin_addr,
        role: *server.raft().role(),
        leader_id: server.raft().leader_id(),
        current_term: hard_state.current_term,
        voted_for: server.raft().voted_for(),
        commit_index: server.raft().commit_index(),
        last_applied: server.state_machine().last_applied(),
        first_log_index: server.raft().first_log_index(),
        last_log_index: server.raft().last_log_index(),
        last_log_term: server.raft().last_log_term(),
        snapshot_index: server.snapshot_store().last_included_index(),
        snapshot_term: server.snapshot_store().last_included_term(),
        current_election_timeout: server.raft().current_election_timeout(),
        state_value: server.state_machine().total(),
        ticks_observed: server.stats().ticks_observed,
        inbound_messages_processed: server.stats().inbound_messages_processed,
        outbound_messages_sent: server.stats().outbound_messages_sent,
        committed_entries_applied: server.stats().committed_entries_applied,
        snapshots_restored: server.stats().snapshots_restored,
        snapshot_policy: format_snapshot_policy(server.snapshot_policy()),
    }
}

fn maybe_log_runtime_change(previous: &mut Option<ObservedState>, status: &NodeStatus) {
    let current = ObservedState {
        role: status.role,
        leader_id: status.leader_id,
        current_term: status.current_term,
        commit_index: status.commit_index,
        last_applied: status.last_applied,
        first_log_index: status.first_log_index,
        last_log_index: status.last_log_index,
        snapshot_index: status.snapshot_index,
        state_value: status.state_value,
    };

    if previous.as_ref() != Some(&current) {
        println!(
            "node {} status | role={:?} leader={:?} term={} commit={} applied={} log=[{}..{}] snap={} value={}",
            status.node_id,
            status.role,
            status.leader_id,
            status.current_term,
            status.commit_index,
            status.last_applied,
            status.first_log_index,
            status.last_log_index,
            status.snapshot_index,
            status.state_value,
        );
        *previous = Some(current);
    }
}

fn query_cluster_statuses(cluster: &ClusterConfig) -> Vec<StatusProbe> {
    cluster
        .ordered_ids()
        .into_iter()
        .map(|node_id| {
            let member = cluster
                .member(node_id)
                .expect("ordered_ids and member lookup should agree");

            let result = send_simple_admin_command(member.admin_addr, "status")
                .and_then(|fields| parse_status_response(&fields))
                .map_err(|err| err.to_string());

            StatusProbe {
                node_id,
                admin_addr: member.admin_addr,
                result,
            }
        })
        .collect()
}

fn print_status_table(probes: &[StatusProbe]) {
    for probe in probes {
        match &probe.result {
            Ok(status) => {
                println!(
                    "node {} | role={:?} leader={:?} term={} commit={} applied={} log=[{}..{}] last_term={} snap={}@{} value={} timeout={} ticks={} in={} out={} restored={} policy={} raft={} admin={}",
                    status.node_id,
                    status.role,
                    status.leader_id,
                    status.current_term,
                    status.commit_index,
                    status.last_applied,
                    status.first_log_index,
                    status.last_log_index,
                    status.last_log_term,
                    status.snapshot_index,
                    status.snapshot_term,
                    status.state_value,
                    status.current_election_timeout,
                    status.ticks_observed,
                    status.inbound_messages_processed,
                    status.outbound_messages_sent,
                    status.snapshots_restored,
                    status.snapshot_policy,
                    status.raft_addr,
                    status.admin_addr,
                );
            }
            Err(err) => {
                println!(
                    "node {} | admin={} | unreachable: {}",
                    probe.node_id, probe.admin_addr, err
                );
            }
        }
    }
}

fn send_propose_request(
    admin_addr: SocketAddr,
    command: DemoCommand,
) -> io::Result<ProposeOutcome> {
    let fields = send_simple_admin_command(admin_addr, &format!("propose {command}"))?;
    match require_kind(&fields, "accepted or not_leader")? {
        "accepted" => Ok(ProposeOutcome::Accepted {
            node_id: parse_required_u64_field(&fields, "node_id")?,
            leader_id: parse_optional_u64_field(&fields, "leader_id")?,
            log_index: parse_required_u64_field(&fields, "log_index")?,
        }),
        "not_leader" => Ok(ProposeOutcome::NotLeader {
            node_id: parse_required_u64_field(&fields, "node_id")?,
            leader_id: parse_optional_u64_field(&fields, "leader_id")?,
        }),
        "error" => Err(io::Error::other(
            fields
                .get("message")
                .cloned()
                .unwrap_or_else(|| "unknown admin error".to_string()),
        )),
        other => Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("unexpected propose response kind: {other}"),
        )),
    }
}

fn send_simple_admin_command(
    admin_addr: SocketAddr,
    command: &str,
) -> io::Result<BTreeMap<String, String>> {
    let mut stream = TcpStream::connect(admin_addr)?;
    stream.set_read_timeout(Some(Duration::from_millis(ADMIN_REQUEST_TIMEOUT_MS)))?;
    stream.set_write_timeout(Some(Duration::from_millis(ADMIN_REQUEST_TIMEOUT_MS)))?;

    stream.write_all(command.as_bytes())?;
    stream.write_all(b"\n")?;
    stream.flush()?;

    read_response_fields(stream)
}

fn read_response_fields(stream: TcpStream) -> io::Result<BTreeMap<String, String>> {
    let mut fields = BTreeMap::new();
    let mut reader = BufReader::new(stream);

    loop {
        let mut line = String::new();
        let n = reader.read_line(&mut line)?;

        if n == 0 {
            break;
        }

        let line = line.trim();
        if line.is_empty() {
            break;
        }

        let (key, value) = line.split_once('=').ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("invalid admin response line: {line}"),
            )
        })?;

        fields.insert(key.to_string(), value.to_string());
    }

    if fields.is_empty() {
        return Err(io::Error::new(
            io::ErrorKind::UnexpectedEof,
            "admin response was empty",
        ));
    }

    Ok(fields)
}

fn write_admin_response(mut stream: TcpStream, response: &AdminResponse) -> io::Result<()> {
    let mut fields = BTreeMap::new();

    match response {
        AdminResponse::Status(status) => {
            fields.insert("ok".to_string(), "true".to_string());
            fields.insert("kind".to_string(), "status".to_string());
            fields.insert("node_id".to_string(), status.node_id.to_string());
            fields.insert("raft_addr".to_string(), status.raft_addr.to_string());
            fields.insert("admin_addr".to_string(), status.admin_addr.to_string());
            fields.insert("role".to_string(), role_to_wire(status.role).to_string());
            fields.insert(
                "leader_id".to_string(),
                option_u64_to_wire(status.leader_id),
            );
            fields.insert("current_term".to_string(), status.current_term.to_string());
            fields.insert(
                "voted_for".to_string(),
                option_u64_to_wire(status.voted_for),
            );
            fields.insert("commit_index".to_string(), status.commit_index.to_string());
            fields.insert("last_applied".to_string(), status.last_applied.to_string());
            fields.insert(
                "first_log_index".to_string(),
                status.first_log_index.to_string(),
            );
            fields.insert(
                "last_log_index".to_string(),
                status.last_log_index.to_string(),
            );
            fields.insert(
                "last_log_term".to_string(),
                status.last_log_term.to_string(),
            );
            fields.insert(
                "snapshot_index".to_string(),
                status.snapshot_index.to_string(),
            );
            fields.insert(
                "snapshot_term".to_string(),
                status.snapshot_term.to_string(),
            );
            fields.insert(
                "current_election_timeout".to_string(),
                status.current_election_timeout.to_string(),
            );
            fields.insert("state_value".to_string(), status.state_value.to_string());
            fields.insert(
                "ticks_observed".to_string(),
                status.ticks_observed.to_string(),
            );
            fields.insert(
                "inbound_messages_processed".to_string(),
                status.inbound_messages_processed.to_string(),
            );
            fields.insert(
                "outbound_messages_sent".to_string(),
                status.outbound_messages_sent.to_string(),
            );
            fields.insert(
                "committed_entries_applied".to_string(),
                status.committed_entries_applied.to_string(),
            );
            fields.insert(
                "snapshots_restored".to_string(),
                status.snapshots_restored.to_string(),
            );
            fields.insert(
                "snapshot_policy".to_string(),
                status.snapshot_policy.clone(),
            );
        }
        AdminResponse::Accepted {
            node_id,
            leader_id,
            log_index,
        } => {
            fields.insert("ok".to_string(), "true".to_string());
            fields.insert("kind".to_string(), "accepted".to_string());
            fields.insert("node_id".to_string(), node_id.to_string());
            fields.insert("leader_id".to_string(), option_u64_to_wire(*leader_id));
            fields.insert("log_index".to_string(), log_index.to_string());
        }
        AdminResponse::NotLeader { node_id, leader_id } => {
            fields.insert("ok".to_string(), "false".to_string());
            fields.insert("kind".to_string(), "not_leader".to_string());
            fields.insert("node_id".to_string(), node_id.to_string());
            fields.insert("leader_id".to_string(), option_u64_to_wire(*leader_id));
        }
        AdminResponse::Snapshot {
            node_id,
            snapshot_index,
        } => {
            fields.insert("ok".to_string(), "true".to_string());
            fields.insert("kind".to_string(), "snapshot".to_string());
            fields.insert("node_id".to_string(), node_id.to_string());
            fields.insert(
                "snapshot_index".to_string(),
                option_u64_to_wire(*snapshot_index),
            );
        }
        AdminResponse::Shutdown { node_id } => {
            fields.insert("ok".to_string(), "true".to_string());
            fields.insert("kind".to_string(), "shutdown".to_string());
            fields.insert("node_id".to_string(), node_id.to_string());
        }
        AdminResponse::Error { node_id, message } => {
            fields.insert("ok".to_string(), "false".to_string());
            fields.insert("kind".to_string(), "error".to_string());
            fields.insert("node_id".to_string(), option_u64_to_wire(*node_id));
            fields.insert("message".to_string(), message.clone());
        }
    }

    for (key, value) in fields {
        writeln!(stream, "{key}={value}")?;
    }
    writeln!(stream)?;
    stream.flush()
}

fn parse_status_response(fields: &BTreeMap<String, String>) -> io::Result<NodeStatus> {
    let kind = require_kind(fields, "status")?;
    if kind != "status" {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("expected kind=status, got {kind}"),
        ));
    }

    Ok(NodeStatus {
        node_id: parse_required_u64_field(fields, "node_id")?,
        raft_addr: parse_required_socketaddr_field(fields, "raft_addr")?,
        admin_addr: parse_required_socketaddr_field(fields, "admin_addr")?,
        role: parse_role_field(fields, "role")?,
        leader_id: parse_optional_u64_field(fields, "leader_id")?,
        current_term: parse_required_u64_field(fields, "current_term")?,
        voted_for: parse_optional_u64_field(fields, "voted_for")?,
        commit_index: parse_required_u64_field(fields, "commit_index")?,
        last_applied: parse_required_u64_field(fields, "last_applied")?,
        first_log_index: parse_required_u64_field(fields, "first_log_index")?,
        last_log_index: parse_required_u64_field(fields, "last_log_index")?,
        last_log_term: parse_required_u64_field(fields, "last_log_term")?,
        snapshot_index: parse_required_u64_field(fields, "snapshot_index")?,
        snapshot_term: parse_required_u64_field(fields, "snapshot_term")?,
        current_election_timeout: parse_required_u64_field(fields, "current_election_timeout")?,
        state_value: parse_required_u64_field(fields, "state_value")?,
        ticks_observed: parse_required_u64_field(fields, "ticks_observed")?,
        inbound_messages_processed: parse_required_u64_field(fields, "inbound_messages_processed")?,
        outbound_messages_sent: parse_required_u64_field(fields, "outbound_messages_sent")?,
        committed_entries_applied: parse_required_u64_field(fields, "committed_entries_applied")?,
        snapshots_restored: parse_required_u64_field(fields, "snapshots_restored")?,
        snapshot_policy: parse_required_string_field(fields, "snapshot_policy")?,
    })
}

fn candidate_order(
    cluster: &ClusterConfig,
    leader_hint: Option<NodeId>,
    target_node: Option<NodeId>,
) -> Vec<NodeId> {
    let mut ordered = Vec::new();

    if let Some(target_node) = target_node {
        ordered.push(target_node);
    }

    if let Some(leader_hint) = leader_hint {
        if !ordered.contains(&leader_hint) {
            ordered.push(leader_hint);
        }
    }

    for node_id in cluster.ordered_ids() {
        if !ordered.contains(&node_id) {
            ordered.push(node_id);
        }
    }

    ordered
}

fn parse_cli() -> io::Result<CliCommand> {
    let args = env::args().skip(1).collect::<Vec<_>>();

    if args.is_empty() {
        return Ok(CliCommand::Help);
    }

    match args[0].as_str() {
        "node" => Ok(CliCommand::Node(parse_node_args(&args[1..])?)),
        "status" => Ok(CliCommand::Status(parse_cluster_only_args(&args[1..])?)),
        "watch" => Ok(CliCommand::Watch(parse_watch_args(&args[1..])?)),
        "propose" => Ok(CliCommand::Propose(parse_propose_args(&args[1..])?)),
        "snapshot" => Ok(CliCommand::Snapshot(parse_node_action_args(&args[1..])?)),
        "shutdown" => Ok(CliCommand::Shutdown(parse_node_action_args(&args[1..])?)),
        "--help" | "-h" | "help" => Ok(CliCommand::Help),
        other => Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("unknown subcommand: {other}"),
        )),
    }
}

fn parse_node_args(args: &[String]) -> io::Result<NodeOptions> {
    let mut id = None;
    let mut data_dir = None;
    let mut cluster = Vec::new();
    let mut tick_ms = DEFAULT_TICK_MS;
    let mut election_timeout_ticks = DEFAULT_ELECTION_TIMEOUT_TICKS;
    let mut heartbeat_interval_ticks = DEFAULT_HEARTBEAT_INTERVAL_TICKS;
    let mut snapshot_every = DEFAULT_SNAPSHOT_EVERY;

    let mut i = 0;
    while i < args.len() {
        match args[i].as_str() {
            "--id" => {
                i += 1;
                id = Some(parse_u64_text(require_arg(args, i, "--id")?, "--id")?);
            }
            "--data-dir" => {
                i += 1;
                data_dir = Some(PathBuf::from(require_arg(args, i, "--data-dir")?));
            }
            "--cluster" => {
                i += 1;
                cluster.push(parse_cluster_member(require_arg(args, i, "--cluster")?)?);
            }
            "--tick-ms" => {
                i += 1;
                tick_ms = parse_u64_text(require_arg(args, i, "--tick-ms")?, "--tick-ms")?;
            }
            "--election-timeout" => {
                i += 1;
                election_timeout_ticks = parse_u64_text(
                    require_arg(args, i, "--election-timeout")?,
                    "--election-timeout",
                )?;
            }
            "--heartbeat-interval" => {
                i += 1;
                heartbeat_interval_ticks = parse_u64_text(
                    require_arg(args, i, "--heartbeat-interval")?,
                    "--heartbeat-interval",
                )?;
            }
            "--snapshot-every" => {
                i += 1;
                snapshot_every = parse_u64_text(
                    require_arg(args, i, "--snapshot-every")?,
                    "--snapshot-every",
                )?;
            }
            other => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    format!("unknown node arg: {other}"),
                ));
            }
        }

        i += 1;
    }

    let id = id.ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            "`node` requires --id <node_id>",
        )
    })?;
    let data_dir = data_dir.ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            "`node` requires --data-dir <path>",
        )
    })?;
    let cluster = ClusterConfig::new(cluster)?;
    let _ = cluster.member(id)?;

    Ok(NodeOptions {
        id,
        data_dir,
        cluster,
        tick_ms,
        election_timeout_ticks,
        heartbeat_interval_ticks,
        snapshot_every,
    })
}

fn parse_cluster_only_args(args: &[String]) -> io::Result<ClusterOnlyOptions> {
    Ok(ClusterOnlyOptions {
        cluster: parse_cluster_config_from_args(args)?,
    })
}

fn parse_watch_args(args: &[String]) -> io::Result<WatchOptions> {
    let mut cluster = Vec::new();
    let mut interval_ms = DEFAULT_WATCH_INTERVAL_MS;

    let mut i = 0;
    while i < args.len() {
        match args[i].as_str() {
            "--cluster" => {
                i += 1;
                cluster.push(parse_cluster_member(require_arg(args, i, "--cluster")?)?);
            }
            "--interval-ms" => {
                i += 1;
                interval_ms =
                    parse_u64_text(require_arg(args, i, "--interval-ms")?, "--interval-ms")?;
            }
            other => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    format!("unknown watch arg: {other}"),
                ));
            }
        }
        i += 1;
    }

    Ok(WatchOptions {
        cluster: ClusterConfig::new(cluster)?,
        interval_ms,
    })
}

fn parse_propose_args(args: &[String]) -> io::Result<ProposeOptions> {
    let mut cluster = Vec::new();
    let mut command = None;
    let mut target_node = None;
    let mut attempts = DEFAULT_PROPOSE_ATTEMPTS;
    let mut retry_ms = DEFAULT_PROPOSE_RETRY_MS;

    let mut i = 0;
    while i < args.len() {
        match args[i].as_str() {
            "--cluster" => {
                i += 1;
                cluster.push(parse_cluster_member(require_arg(args, i, "--cluster")?)?);
            }
            "--command" => {
                i += 1;
                command = Some(parse_u64_text(
                    require_arg(args, i, "--command")?,
                    "--command",
                )?);
            }
            "--target-node" => {
                i += 1;
                target_node = Some(parse_u64_text(
                    require_arg(args, i, "--target-node")?,
                    "--target-node",
                )?);
            }
            "--attempts" => {
                i += 1;
                attempts = parse_usize_text(require_arg(args, i, "--attempts")?, "--attempts")?;
            }
            "--retry-ms" => {
                i += 1;
                retry_ms = parse_u64_text(require_arg(args, i, "--retry-ms")?, "--retry-ms")?;
            }
            other => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    format!("unknown propose arg: {other}"),
                ));
            }
        }
        i += 1;
    }

    Ok(ProposeOptions {
        cluster: ClusterConfig::new(cluster)?,
        command: command.ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                "`propose` requires --command <u64>",
            )
        })?,
        target_node,
        attempts,
        retry_ms,
    })
}

fn parse_node_action_args(args: &[String]) -> io::Result<NodeActionOptions> {
    let mut cluster = Vec::new();
    let mut node_id = None;

    let mut i = 0;
    while i < args.len() {
        match args[i].as_str() {
            "--cluster" => {
                i += 1;
                cluster.push(parse_cluster_member(require_arg(args, i, "--cluster")?)?);
            }
            "--node-id" => {
                i += 1;
                node_id = Some(parse_u64_text(
                    require_arg(args, i, "--node-id")?,
                    "--node-id",
                )?);
            }
            other => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    format!("unknown action arg: {other}"),
                ));
            }
        }
        i += 1;
    }

    Ok(NodeActionOptions {
        cluster: ClusterConfig::new(cluster)?,
        node_id: node_id.ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                "action requires --node-id <node_id>",
            )
        })?,
    })
}

fn parse_cluster_config_from_args(args: &[String]) -> io::Result<ClusterConfig> {
    let mut cluster = Vec::new();
    let mut i = 0;

    while i < args.len() {
        match args[i].as_str() {
            "--cluster" => {
                i += 1;
                cluster.push(parse_cluster_member(require_arg(args, i, "--cluster")?)?);
            }
            other => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    format!("unknown arg: {other}"),
                ));
            }
        }

        i += 1;
    }

    ClusterConfig::new(cluster)
}

fn parse_cluster_member(text: &str) -> io::Result<ClusterMember> {
    let (node_text, rest) = text.split_once('=').ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("invalid cluster member `{text}`, expected id=raft_addr@admin_addr"),
        )
    })?;

    let (raft_text, admin_text) = rest.split_once('@').ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("invalid cluster member `{text}`, expected id=raft_addr@admin_addr"),
        )
    })?;

    Ok(ClusterMember {
        node_id: parse_u64_text(node_text, "cluster node id")?,
        raft_addr: parse_socketaddr_text(raft_text, "cluster raft addr")?,
        admin_addr: parse_socketaddr_text(admin_text, "cluster admin addr")?,
    })
}

fn parse_admin_request_line(line: &str) -> io::Result<ParsedAdminRequest> {
    let mut parts = line.split_whitespace();

    match parts.next() {
        Some("status") => Ok(ParsedAdminRequest::Status),
        Some("propose") => {
            let command = parts.next().ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "propose command requires a u64 payload",
                )
            })?;

            Ok(ParsedAdminRequest::Propose {
                command: parse_u64_text(command, "propose payload")?,
            })
        }
        Some("snapshot") => Ok(ParsedAdminRequest::Snapshot),
        Some("shutdown") => Ok(ParsedAdminRequest::Shutdown),
        Some(other) => Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("unknown admin command `{other}`"),
        )),
        None => Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "empty admin command",
        )),
    }
}

fn print_usage() {
    println!("Raft runtime node and control CLI");
    println!();
    println!("Subcommands:");
    println!("  node");
    println!("  status");
    println!("  watch");
    println!("  propose");
    println!("  snapshot");
    println!("  shutdown");
    println!();
    println!("Cluster member format:");
    println!("  --cluster <id=raft_addr@admin_addr>");
    println!("  example: --cluster 1=127.0.0.1:7001@127.0.0.1:8001");
    println!();
    println!("Examples:");
    println!("  cargo run -- node --id 1 --data-dir ./data/node1 \\");
    println!("    --cluster 1=127.0.0.1:7001@127.0.0.1:8001 \\");
    println!("    --cluster 2=127.0.0.1:7002@127.0.0.1:8002 \\");
    println!("    --cluster 3=127.0.0.1:7003@127.0.0.1:8003");
    println!();
    println!("  cargo run -- status \\");
    println!("    --cluster 1=127.0.0.1:7001@127.0.0.1:8001 \\");
    println!("    --cluster 2=127.0.0.1:7002@127.0.0.1:8002 \\");
    println!("    --cluster 3=127.0.0.1:7003@127.0.0.1:8003");
    println!();
    println!("  cargo run -- watch \\");
    println!("    --cluster 1=127.0.0.1:7001@127.0.0.1:8001 \\");
    println!("    --cluster 2=127.0.0.1:7002@127.0.0.1:8002 \\");
    println!("    --cluster 3=127.0.0.1:7003@127.0.0.1:8003");
    println!();
    println!("  cargo run -- propose --command 5 \\");
    println!("    --cluster 1=127.0.0.1:7001@127.0.0.1:8001 \\");
    println!("    --cluster 2=127.0.0.1:7002@127.0.0.1:8002 \\");
    println!("    --cluster 3=127.0.0.1:7003@127.0.0.1:8003");
    println!();
    println!("  cargo run -- snapshot --node-id 1 \\");
    println!("    --cluster 1=127.0.0.1:7001@127.0.0.1:8001 \\");
    println!("    --cluster 2=127.0.0.1:7002@127.0.0.1:8002 \\");
    println!("    --cluster 3=127.0.0.1:7003@127.0.0.1:8003");
}

fn require_arg<'a>(args: &'a [String], index: usize, flag: &str) -> io::Result<&'a str> {
    args.get(index).map(String::as_str).ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("{flag} requires a following value"),
        )
    })
}

fn parse_u64_text(text: &str, what: &str) -> io::Result<u64> {
    text.parse().map_err(|err| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("invalid {what} `{text}`: {err}"),
        )
    })
}

fn parse_usize_text(text: &str, what: &str) -> io::Result<usize> {
    text.parse().map_err(|err| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("invalid {what} `{text}`: {err}"),
        )
    })
}

fn parse_socketaddr_text(text: &str, what: &str) -> io::Result<SocketAddr> {
    text.parse().map_err(|err| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("invalid {what} `{text}`: {err}"),
        )
    })
}

fn role_to_wire(role: Role) -> &'static str {
    match role {
        Role::Leader => "leader",
        Role::Follower => "follower",
        Role::Candidate => "candidate",
    }
}

fn parse_role_field(fields: &BTreeMap<String, String>, key: &str) -> io::Result<Role> {
    match parse_required_string_field(fields, key)?.as_str() {
        "leader" => Ok(Role::Leader),
        "follower" => Ok(Role::Follower),
        "candidate" => Ok(Role::Candidate),
        other => Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("invalid role `{other}`"),
        )),
    }
}

fn option_u64_to_wire(value: Option<u64>) -> String {
    value
        .map(|value| value.to_string())
        .unwrap_or_else(|| "none".to_string())
}

fn parse_optional_u64_field(
    fields: &BTreeMap<String, String>,
    key: &str,
) -> io::Result<Option<u64>> {
    match parse_required_string_field(fields, key)?.as_str() {
        "none" => Ok(None),
        text => Ok(Some(parse_u64_text(text, key)?)),
    }
}

fn parse_required_u64_field(fields: &BTreeMap<String, String>, key: &str) -> io::Result<u64> {
    parse_u64_text(&parse_required_string_field(fields, key)?, key)
}

fn parse_required_socketaddr_field(
    fields: &BTreeMap<String, String>,
    key: &str,
) -> io::Result<SocketAddr> {
    parse_socketaddr_text(&parse_required_string_field(fields, key)?, key)
}

fn parse_required_string_field(fields: &BTreeMap<String, String>, key: &str) -> io::Result<String> {
    fields.get(key).cloned().ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            format!("response missing required field `{key}`"),
        )
    })
}

fn require_kind<'a>(fields: &'a BTreeMap<String, String>, expected: &str) -> io::Result<&'a str> {
    fields.get("kind").map(String::as_str).ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            format!("response missing kind field, expected {expected}"),
        )
    })
}

fn format_snapshot_policy(policy: SnapshotPolicy) -> String {
    match policy {
        SnapshotPolicy::Never => "never".to_string(),
        SnapshotPolicy::EveryAppliedEntries(value) => format!("every:{value}"),
    }
}

fn runtime_error_to_io(err: RuntimeError, node_id: NodeId) -> io::Error {
    match err {
        RuntimeError::Io(err) => io::Error::new(
            err.kind(),
            format!("node {node_id} runtime I/O error: {err}"),
        ),
        RuntimeError::InboundClosed => io::Error::new(
            io::ErrorKind::BrokenPipe,
            format!("node {node_id} inbound transport channel closed"),
        ),
    }
}

enum ParsedAdminRequest {
    Status,
    Propose { command: DemoCommand },
    Snapshot,
    Shutdown,
}
