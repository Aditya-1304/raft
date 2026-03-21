use std::{
    collections::BTreeMap,
    io::{self, Cursor, Read, Write},
    marker::PhantomData,
    net::{SocketAddr, TcpListener, TcpStream},
    sync::mpsc::{self, Receiver, Sender},
    thread,
    time::Duration,
};

use crate::{
    entry::LogEntry,
    message::{
        AppendEntriesRequest, AppendEntriesResponse, Envelope, InstallSnapshotRequest,
        InstallSnapshotResponse, Message, PreVoteRequest, PreVoteResponse, RequestVoteRequest,
        RequestVoteResponse,
    },
    storage::codec::{CommandCodec, SnapshotCodec},
    traits::transport::Transport,
    types::{NodeId, Snapshot},
};

const TAG_PREVOTE_REQUEST: u8 = 1;
const TAG_PREVOTE_RESPONSE: u8 = 2;
const TAG_REQUEST_VOTE_REQUEST: u8 = 3;
const TAG_REQUEST_VOTE_RESPONSE: u8 = 4;
const TAG_APPEND_ENTRIES_REQUEST: u8 = 5;
const TAG_APPEND_ENTRIES_RESPONSE: u8 = 6;
const TAG_INSTALL_SNAPSHOT_REQUEST: u8 = 7;
const TAG_INSTALL_SNAPSHOT_RESPONSE: u8 = 8;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TcpTransportConfig {
    pub connect_timeout: Duration,
    pub nodelay: bool,
}

impl Default for TcpTransportConfig {
    fn default() -> Self {
        Self {
            connect_timeout: Duration::from_millis(250),
            nodelay: true,
        }
    }
}

pub struct TcpEndpoint<C, S, CC, SC>
where
    CC: CommandCodec<C>,
    SC: SnapshotCodec<S>,
{
    pub transport: TcpTransport<C, S, CC, SC>,
    pub inbound: Receiver<Envelope<C, S>>,
    pub local_addr: SocketAddr,
}

#[derive(Debug, Clone)]
pub struct TcpTransport<C, S, CC, SC>
where
    CC: CommandCodec<C>,
    SC: SnapshotCodec<S>,
{
    local_id: NodeId,
    peers: BTreeMap<NodeId, SocketAddr>,
    codec: TcpEnvelopeCodec<C, S, CC, SC>,
    config: TcpTransportConfig,
}

#[derive(Debug, Clone)]
pub struct TcpEnvelopeCodec<C, S, CC, SC>
where
    CC: CommandCodec<C>,
    SC: SnapshotCodec<S>,
{
    command_codec: CC,
    snapshot_codec: SC,
    _marker: PhantomData<(C, S)>,
}

impl<C, S, CC, SC> TcpTransport<C, S, CC, SC>
where
    C: Clone + Send + 'static,
    S: Clone + Send + 'static,
    CC: CommandCodec<C> + Clone + Send + Sync + 'static,
    SC: SnapshotCodec<S> + Clone + Send + Sync + 'static,
{
    pub fn bind(
        local_id: NodeId,
        bind_addr: SocketAddr,
        peers: BTreeMap<NodeId, SocketAddr>,
        command_codec: CC,
        snapshot_codec: SC,
    ) -> io::Result<TcpEndpoint<C, S, CC, SC>> {
        Self::bind_with_config(
            local_id,
            bind_addr,
            peers,
            command_codec,
            snapshot_codec,
            TcpTransportConfig::default(),
        )
    }

    pub fn bind_with_config(
        local_id: NodeId,
        bind_addr: SocketAddr,
        peers: BTreeMap<NodeId, SocketAddr>,
        command_codec: CC,
        snapshot_codec: SC,
        config: TcpTransportConfig,
    ) -> io::Result<TcpEndpoint<C, S, CC, SC>> {
        let listener = TcpListener::bind(bind_addr)?;
        let local_addr = listener.local_addr()?;

        let codec = TcpEnvelopeCodec::new(command_codec, snapshot_codec);
        let transport = Self {
            local_id,
            peers,
            codec: codec.clone(),
            config,
        };

        let (tx, rx) = mpsc::channel();
        spawn_listener_thread(listener, tx, codec);

        Ok(TcpEndpoint {
            transport,
            inbound: rx,
            local_addr,
        })
    }

    pub fn local_id(&self) -> NodeId {
        self.local_id
    }

    pub fn peers(&self) -> &BTreeMap<NodeId, SocketAddr> {
        &self.peers
    }

    pub fn config(&self) -> TcpTransportConfig {
        self.config
    }

    pub fn try_send(&self, msg: Envelope<C, S>) -> io::Result<()> {
        let Some(addr) = self.peers.get(&msg.to).copied() else {
            return Err(io::Error::new(
                io::ErrorKind::NotFound,
                format!("no TCP peer configured for node {}", msg.to),
            ));
        };

        self.send_batch_to(addr, vec![msg])
    }

    pub fn try_send_batch(&self, messages: Vec<Envelope<C, S>>) -> io::Result<()> {
        let mut by_target: BTreeMap<NodeId, Vec<Envelope<C, S>>> = BTreeMap::new();

        for message in messages {
            by_target.entry(message.to).or_default().push(message);
        }

        let mut last_error = None;

        for (target, batch) in by_target {
            let Some(addr) = self.peers.get(&target).copied() else {
                last_error = Some(io::Error::new(
                    io::ErrorKind::NotFound,
                    format!("no TCP peer configured for node {target}"),
                ));
                continue;
            };

            if let Err(err) = self.send_batch_to(addr, batch) {
                last_error = Some(err);
            }
        }

        if let Some(err) = last_error {
            Err(err)
        } else {
            Ok(())
        }
    }

    fn send_batch_to(&self, addr: SocketAddr, batch: Vec<Envelope<C, S>>) -> io::Result<()> {
        let mut stream = TcpStream::connect_timeout(&addr, self.config.connect_timeout)?;
        stream.set_nodelay(self.config.nodelay)?;

        for message in batch {
            let payload = self.codec.encode_envelope(&message)?;
            write_frame(&mut stream, &payload)?;
        }

        stream.flush()?;
        Ok(())
    }
}

impl<C, S, CC, SC> Transport<C, S> for TcpTransport<C, S, CC, SC>
where
    C: Clone + Send + 'static,
    S: Clone + Send + 'static,
    CC: CommandCodec<C> + Clone + Send + Sync + 'static,
    SC: SnapshotCodec<S> + Clone + Send + Sync + 'static,
{
    fn send(&self, msg: Envelope<C, S>) {
        if let Err(err) = self.try_send(msg) {
            eprintln!("tcp transport send failed: {err}");
        }
    }

    fn send_batch(&self, msg: Vec<Envelope<C, S>>) {
        if let Err(err) = self.try_send_batch(msg) {
            eprintln!("tcp transport batch send failed: {err}");
        }
    }
}

impl<C, S, CC, SC> TcpEnvelopeCodec<C, S, CC, SC>
where
    CC: CommandCodec<C>,
    SC: SnapshotCodec<S>,
{
    pub fn new(command_codec: CC, snapshot_codec: SC) -> Self {
        Self {
            command_codec,
            snapshot_codec,
            _marker: PhantomData,
        }
    }

    pub fn encode_envelope(&self, envelope: &Envelope<C, S>) -> io::Result<Vec<u8>> {
        let mut buf = Vec::new();

        push_u64(&mut buf, envelope.from);
        push_u64(&mut buf, envelope.to);

        match &envelope.msg {
            Message::PreVote(request) => {
                push_u8(&mut buf, TAG_PREVOTE_REQUEST);
                self.encode_prevote_request(&mut buf, request);
            }
            Message::PreVoteResponse(response) => {
                push_u8(&mut buf, TAG_PREVOTE_RESPONSE);
                self.encode_prevote_response(&mut buf, response);
            }
            Message::RequestVote(request) => {
                push_u8(&mut buf, TAG_REQUEST_VOTE_REQUEST);
                self.encode_request_vote_request(&mut buf, request);
            }
            Message::RequestVoteResponse(response) => {
                push_u8(&mut buf, TAG_REQUEST_VOTE_RESPONSE);
                self.encode_request_vote_response(&mut buf, response);
            }
            Message::AppendEntries(request) => {
                push_u8(&mut buf, TAG_APPEND_ENTRIES_REQUEST);
                self.encode_append_entries_request(&mut buf, request)?;
            }
            Message::AppendEntriesResponse(response) => {
                push_u8(&mut buf, TAG_APPEND_ENTRIES_RESPONSE);
                self.encode_append_entries_response(&mut buf, response);
            }
            Message::InstallSnapshot(request) => {
                push_u8(&mut buf, TAG_INSTALL_SNAPSHOT_REQUEST);
                self.encode_install_snapshot_request(&mut buf, request)?;
            }
            Message::InstallSnapshotResponse(response) => {
                push_u8(&mut buf, TAG_INSTALL_SNAPSHOT_RESPONSE);
                self.encode_install_snapshot_response(&mut buf, response);
            }
        }

        Ok(buf)
    }

    pub fn decode_envelope(&self, bytes: &[u8]) -> io::Result<Envelope<C, S>> {
        let mut cursor = Cursor::new(bytes);

        let from = read_u64(&mut cursor)?;
        let to = read_u64(&mut cursor)?;
        let tag = read_u8(&mut cursor)?;

        let msg = match tag {
            TAG_PREVOTE_REQUEST => Message::PreVote(self.decode_prevote_request(&mut cursor)?),
            TAG_PREVOTE_RESPONSE => {
                Message::PreVoteResponse(self.decode_prevote_response(&mut cursor)?)
            }
            TAG_REQUEST_VOTE_REQUEST => {
                Message::RequestVote(self.decode_request_vote_request(&mut cursor)?)
            }
            TAG_REQUEST_VOTE_RESPONSE => {
                Message::RequestVoteResponse(self.decode_request_vote_response(&mut cursor)?)
            }
            TAG_APPEND_ENTRIES_REQUEST => {
                Message::AppendEntries(self.decode_append_entries_request(&mut cursor)?)
            }
            TAG_APPEND_ENTRIES_RESPONSE => {
                Message::AppendEntriesResponse(self.decode_append_entries_response(&mut cursor)?)
            }
            TAG_INSTALL_SNAPSHOT_REQUEST => {
                Message::InstallSnapshot(self.decode_install_snapshot_request(&mut cursor)?)
            }
            TAG_INSTALL_SNAPSHOT_RESPONSE => Message::InstallSnapshotResponse(
                self.decode_install_snapshot_response(&mut cursor)?,
            ),
            other => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("unknown message tag: {other}"),
                ));
            }
        };

        Ok(Envelope { from, to, msg })
    }

    fn encode_prevote_request(&self, buf: &mut Vec<u8>, request: &PreVoteRequest) {
        push_u64(buf, request.term);
        push_u64(buf, request.candidate_id);
        push_u64(buf, request.last_log_index);
        push_u64(buf, request.last_log_term);
    }

    fn decode_prevote_request(&self, cursor: &mut Cursor<&[u8]>) -> io::Result<PreVoteRequest> {
        Ok(PreVoteRequest {
            term: read_u64(cursor)?,
            candidate_id: read_u64(cursor)?,
            last_log_index: read_u64(cursor)?,
            last_log_term: read_u64(cursor)?,
        })
    }

    fn encode_prevote_response(&self, buf: &mut Vec<u8>, response: &PreVoteResponse) {
        push_u64(buf, response.term);
        push_bool(buf, response.vote_granted);
    }

    fn decode_prevote_response(&self, cursor: &mut Cursor<&[u8]>) -> io::Result<PreVoteResponse> {
        Ok(PreVoteResponse {
            term: read_u64(cursor)?,
            vote_granted: read_bool(cursor)?,
        })
    }

    fn encode_request_vote_request(&self, buf: &mut Vec<u8>, request: &RequestVoteRequest) {
        push_u64(buf, request.term);
        push_u64(buf, request.candidate_id);
        push_u64(buf, request.last_log_index);
        push_u64(buf, request.last_log_term);
    }

    fn decode_request_vote_request(
        &self,
        cursor: &mut Cursor<&[u8]>,
    ) -> io::Result<RequestVoteRequest> {
        Ok(RequestVoteRequest {
            term: read_u64(cursor)?,
            candidate_id: read_u64(cursor)?,
            last_log_index: read_u64(cursor)?,
            last_log_term: read_u64(cursor)?,
        })
    }

    fn encode_request_vote_response(&self, buf: &mut Vec<u8>, response: &RequestVoteResponse) {
        push_u64(buf, response.term);
        push_bool(buf, response.vote_granted);
    }

    fn decode_request_vote_response(
        &self,
        cursor: &mut Cursor<&[u8]>,
    ) -> io::Result<RequestVoteResponse> {
        Ok(RequestVoteResponse {
            term: read_u64(cursor)?,
            vote_granted: read_bool(cursor)?,
        })
    }

    fn encode_append_entries_request(
        &self,
        buf: &mut Vec<u8>,
        request: &AppendEntriesRequest<C>,
    ) -> io::Result<()> {
        push_u64(buf, request.term);
        push_u64(buf, request.leader_id);
        push_u64(buf, request.prev_log_index);
        push_u64(buf, request.prev_log_term);
        push_u64(buf, request.leader_commit);
        push_u64(buf, request.entries.len() as u64);

        for entry in &request.entries {
            push_u64(buf, entry.index);
            push_u64(buf, entry.term);
            push_bytes(buf, &self.command_codec.encode(&entry.command)?);
        }

        Ok(())
    }

    fn decode_append_entries_request(
        &self,
        cursor: &mut Cursor<&[u8]>,
    ) -> io::Result<AppendEntriesRequest<C>> {
        let term = read_u64(cursor)?;
        let leader_id = read_u64(cursor)?;
        let prev_log_index = read_u64(cursor)?;
        let prev_log_term = read_u64(cursor)?;
        let leader_commit = read_u64(cursor)?;
        let entry_count = read_u64(cursor)? as usize;

        let mut entries = Vec::with_capacity(entry_count);

        for _ in 0..entry_count {
            let index = read_u64(cursor)?;
            let term = read_u64(cursor)?;
            let command = self.command_codec.decode(&read_bytes(cursor)?)?;

            entries.push(LogEntry {
                index,
                term,
                command,
            });
        }

        Ok(AppendEntriesRequest {
            term,
            leader_id,
            prev_log_index,
            prev_log_term,
            entries,
            leader_commit,
        })
    }

    fn encode_append_entries_response(&self, buf: &mut Vec<u8>, response: &AppendEntriesResponse) {
        push_u64(buf, response.term);
        push_bool(buf, response.success);
        push_option_u64(buf, response.match_index);
        push_option_u64(buf, response.conflict_term);
        push_option_u64(buf, response.conflict_index);
    }

    fn decode_append_entries_response(
        &self,
        cursor: &mut Cursor<&[u8]>,
    ) -> io::Result<AppendEntriesResponse> {
        Ok(AppendEntriesResponse {
            term: read_u64(cursor)?,
            success: read_bool(cursor)?,
            match_index: read_option_u64(cursor)?,
            conflict_term: read_option_u64(cursor)?,
            conflict_index: read_option_u64(cursor)?,
        })
    }

    fn encode_install_snapshot_request(
        &self,
        buf: &mut Vec<u8>,
        request: &InstallSnapshotRequest<S>,
    ) -> io::Result<()> {
        push_u64(buf, request.term);
        push_u64(buf, request.leader_id);
        push_u64(buf, request.snapshot.last_included_index);
        push_u64(buf, request.snapshot.last_included_term);
        push_bytes(buf, &self.snapshot_codec.encode(&request.snapshot.data)?);
        Ok(())
    }

    fn decode_install_snapshot_request(
        &self,
        cursor: &mut Cursor<&[u8]>,
    ) -> io::Result<InstallSnapshotRequest<S>> {
        let term = read_u64(cursor)?;
        let leader_id = read_u64(cursor)?;
        let last_included_index = read_u64(cursor)?;
        let last_included_term = read_u64(cursor)?;
        let data = self.snapshot_codec.decode(&read_bytes(cursor)?)?;

        Ok(InstallSnapshotRequest {
            term,
            leader_id,
            snapshot: Snapshot {
                last_included_index,
                last_included_term,
                data,
            },
        })
    }

    fn encode_install_snapshot_response(
        &self,
        buf: &mut Vec<u8>,
        response: &InstallSnapshotResponse,
    ) {
        push_u64(buf, response.term);
        push_bool(buf, response.success);
        push_u64(buf, response.last_included_index);
    }

    fn decode_install_snapshot_response(
        &self,
        cursor: &mut Cursor<&[u8]>,
    ) -> io::Result<InstallSnapshotResponse> {
        Ok(InstallSnapshotResponse {
            term: read_u64(cursor)?,
            success: read_bool(cursor)?,
            last_included_index: read_u64(cursor)?,
        })
    }
}

fn spawn_listener_thread<C, S, CC, SC>(
    listener: TcpListener,
    tx: Sender<Envelope<C, S>>,
    codec: TcpEnvelopeCodec<C, S, CC, SC>,
) where
    C: Clone + Send + 'static,
    S: Clone + Send + 'static,
    CC: CommandCodec<C> + Clone + Send + Sync + 'static,
    SC: SnapshotCodec<S> + Clone + Send + Sync + 'static,
{
    thread::spawn(move || {
        for stream in listener.incoming() {
            match stream {
                Ok(stream) => {
                    let tx = tx.clone();
                    let codec = codec.clone();

                    thread::spawn(move || {
                        if let Err(err) = handle_inbound_stream(stream, tx, codec) {
                            eprintln!("tcp transport inbound stream failed: {err}");
                        }
                    });
                }
                Err(err) => {
                    eprintln!("tcp transport listener accept failed: {err}");
                }
            }
        }
    });
}

fn handle_inbound_stream<C, S, CC, SC>(
    mut stream: TcpStream,
    tx: Sender<Envelope<C, S>>,
    codec: TcpEnvelopeCodec<C, S, CC, SC>,
) -> io::Result<()>
where
    C: Clone + Send + 'static,
    S: Clone + Send + 'static,
    CC: CommandCodec<C> + Clone + Send + Sync + 'static,
    SC: SnapshotCodec<S> + Clone + Send + Sync + 'static,
{
    while let Some(frame) = read_frame(&mut stream)? {
        let envelope = codec.decode_envelope(&frame)?;
        if tx.send(envelope).is_err() {
            break;
        }
    }

    Ok(())
}

fn write_frame(stream: &mut TcpStream, payload: &[u8]) -> io::Result<()> {
    let len = payload.len() as u64;
    stream.write_all(&len.to_be_bytes())?;
    stream.write_all(payload)?;
    Ok(())
}

fn read_frame(stream: &mut TcpStream) -> io::Result<Option<Vec<u8>>> {
    let mut len_buf = [0_u8; 8];
    let mut read = 0;

    while read < len_buf.len() {
        let n = stream.read(&mut len_buf[read..])?;

        if n == 0 {
            if read == 0 {
                return Ok(None);
            }

            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "truncated frame length",
            ));
        }

        read += n;
    }

    let len = u64::from_be_bytes(len_buf) as usize;
    let mut payload = vec![0_u8; len];
    stream.read_exact(&mut payload)?;
    Ok(Some(payload))
}

fn push_u8(buf: &mut Vec<u8>, value: u8) {
    buf.push(value);
}

fn push_bool(buf: &mut Vec<u8>, value: bool) {
    push_u8(buf, if value { 1 } else { 0 });
}

fn push_u64(buf: &mut Vec<u8>, value: u64) {
    buf.extend_from_slice(&value.to_be_bytes());
}

fn push_bytes(buf: &mut Vec<u8>, bytes: &[u8]) {
    push_u64(buf, bytes.len() as u64);
    buf.extend_from_slice(bytes);
}

fn push_option_u64(buf: &mut Vec<u8>, value: Option<u64>) {
    match value {
        Some(value) => {
            push_u8(buf, 1);
            push_u64(buf, value);
        }
        None => push_u8(buf, 0),
    }
}

fn read_u8(cursor: &mut Cursor<&[u8]>) -> io::Result<u8> {
    let mut buf = [0_u8; 1];
    cursor.read_exact(&mut buf)?;
    Ok(buf[0])
}

fn read_bool(cursor: &mut Cursor<&[u8]>) -> io::Result<bool> {
    match read_u8(cursor)? {
        0 => Ok(false),
        1 => Ok(true),
        other => Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("invalid bool tag: {other}"),
        )),
    }
}

fn read_u64(cursor: &mut Cursor<&[u8]>) -> io::Result<u64> {
    let mut buf = [0_u8; 8];
    cursor.read_exact(&mut buf)?;
    Ok(u64::from_be_bytes(buf))
}

fn read_bytes(cursor: &mut Cursor<&[u8]>) -> io::Result<Vec<u8>> {
    let len = read_u64(cursor)? as usize;
    let mut buf = vec![0_u8; len];
    cursor.read_exact(&mut buf)?;
    Ok(buf)
}

fn read_option_u64(cursor: &mut Cursor<&[u8]>) -> io::Result<Option<u64>> {
    match read_u8(cursor)? {
        0 => Ok(None),
        1 => Ok(Some(read_u64(cursor)?)),
        other => Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("invalid option tag: {other}"),
        )),
    }
}
