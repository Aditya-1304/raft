use std::{
    collections::{BTreeMap, BTreeSet, VecDeque},
    fmt,
    sync::mpsc::{self, RecvTimeoutError, Sender},
    time::Duration,
};

use crate::types::{LogIndex, NodeId};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClientRequest<C> {
    pub client_id: u64,
    pub request_id: u64,
    pub command: C,
}

#[derive(Debug)]
pub struct ClientEnvelope<C> {
    pub request: ClientRequest<C>,
    pub reply_to: Sender<ClientReply>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ClientReply {
    Accepted {
        request_id: u64,
        node_id: NodeId,
        log_index: LogIndex,
    },
    NotLeader {
        request_id: u64,
        node_id: NodeId,
        leader_hint: Option<NodeId>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClientAccepted {
    pub client_id: u64,
    pub request_id: u64,
    pub node_id: NodeId,
    pub log_index: LogIndex,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ClientError {
    NoPeers,
    DuplicateRequest {
        request_id: u64,
    },
    MismatchedResponse {
        expected_request_id: u64,
        actual_request_id: u64,
    },
    AllNodesRejected {
        request_id: u64,
        leader_hint: Option<NodeId>,
    },
}

impl fmt::Display for ClientError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ClientError::NoPeers => write!(f, "client has no known peers"),
            ClientError::DuplicateRequest { request_id } => {
                write!(f, "duplicate client request id: {request_id}")
            }
            ClientError::MismatchedResponse {
                expected_request_id,
                actual_request_id,
            } => write!(
                f,
                "client received mismatched response: expected request {}, got {}",
                expected_request_id, actual_request_id
            ),
            ClientError::AllNodesRejected {
                request_id,
                leader_hint,
            } => write!(
                f,
                "all nodes rejected request {} (latest leader hint: {:?})",
                request_id, leader_hint
            ),
        }
    }
}

impl std::error::Error for ClientError {}

pub struct RuntimeClient<C> {
    client_id: u64,
    next_request_id: u64,
    leader_hint: Option<NodeId>,
    peers: BTreeMap<NodeId, Sender<ClientEnvelope<C>>>,
    pending_requests: BTreeSet<u64>,
}

impl<C> RuntimeClient<C> {
    pub fn new(client_id: u64, peers: BTreeMap<NodeId, Sender<ClientEnvelope<C>>>) -> Self {
        Self {
            client_id,
            next_request_id: 1,
            leader_hint: None,
            peers,
            pending_requests: BTreeSet::new(),
        }
    }

    pub fn client_id(&self) -> u64 {
        self.client_id
    }

    pub fn leader_hint(&self) -> Option<NodeId> {
        self.leader_hint
    }

    pub fn set_leader_hint(&mut self, leader_hint: Option<NodeId>) {
        self.leader_hint = leader_hint.filter(|node_id| self.peers.contains_key(node_id));
    }

    pub fn peer_count(&self) -> usize {
        self.peers.len()
    }

    pub fn pending_request_count(&self) -> usize {
        self.pending_requests.len()
    }

    pub fn register_peer(&mut self, node_id: NodeId, sender: Sender<ClientEnvelope<C>>) {
        self.peers.insert(node_id, sender);
    }

    pub fn remove_peer(&mut self, node_id: NodeId) -> Option<Sender<ClientEnvelope<C>>> {
        if self.leader_hint == Some(node_id) {
            self.leader_hint = None;
        }

        self.peers.remove(&node_id)
    }
}

impl<C> RuntimeClient<C>
where
    C: Clone + Send + 'static,
{
    pub fn submit(&mut self, command: C, timeout: Duration) -> Result<ClientAccepted, ClientError> {
        let request_id = self.next_request_id;
        self.next_request_id = self.next_request_id.saturating_add(1);

        self.submit_with_id(request_id, command, timeout)
    }

    pub fn submit_with_id(
        &mut self,
        request_id: u64,
        command: C,
        timeout: Duration,
    ) -> Result<ClientAccepted, ClientError> {
        if self.peers.is_empty() {
            return Err(ClientError::NoPeers);
        }

        if !self.pending_requests.insert(request_id) {
            return Err(ClientError::DuplicateRequest { request_id });
        }

        let request = ClientRequest {
            client_id: self.client_id,
            request_id,
            command,
        };

        let result = self.submit_inner(request, timeout);
        self.pending_requests.remove(&request_id);
        result
    }

    fn submit_inner(
        &mut self,
        request: ClientRequest<C>,
        timeout: Duration,
    ) -> Result<ClientAccepted, ClientError> {
        let mut candidates = self.initial_candidates();
        let mut tried = BTreeSet::new();

        while let Some(node_id) = candidates.pop_front() {
            if !tried.insert(node_id) {
                continue;
            }

            let Some(peer) = self.peers.get(&node_id) else {
                continue;
            };

            let (reply_tx, reply_rx) = mpsc::channel();
            let envelope = ClientEnvelope {
                request: request.clone(),
                reply_to: reply_tx,
            };

            if peer.send(envelope).is_err() {
                continue;
            }

            match reply_rx.recv_timeout(timeout) {
                Ok(ClientReply::Accepted {
                    request_id,
                    node_id,
                    log_index,
                }) => {
                    self.ensure_request_match(request.request_id, request_id)?;
                    self.leader_hint = Some(node_id);

                    return Ok(ClientAccepted {
                        client_id: request.client_id,
                        request_id,
                        node_id,
                        log_index,
                    });
                }
                Ok(ClientReply::NotLeader {
                    request_id,
                    node_id: _,
                    leader_hint,
                }) => {
                    self.ensure_request_match(request.request_id, request_id)?;

                    if let Some(leader_hint) = leader_hint {
                        self.record_leader_hint(leader_hint);

                        if self.peers.contains_key(&leader_hint) && !tried.contains(&leader_hint) {
                            candidates.push_front(leader_hint);
                        }
                    }
                }
                Err(RecvTimeoutError::Timeout) => {}
                Err(RecvTimeoutError::Disconnected) => {}
            }
        }

        Err(ClientError::AllNodesRejected {
            request_id: request.request_id,
            leader_hint: self.leader_hint,
        })
    }

    fn ensure_request_match(
        &self,
        expected_request_id: u64,
        actual_request_id: u64,
    ) -> Result<(), ClientError> {
        if expected_request_id == actual_request_id {
            Ok(())
        } else {
            Err(ClientError::MismatchedResponse {
                expected_request_id,
                actual_request_id,
            })
        }
    }

    fn record_leader_hint(&mut self, leader_hint: NodeId) {
        if self.peers.contains_key(&leader_hint) {
            self.leader_hint = Some(leader_hint);
        }
    }

    fn initial_candidates(&self) -> VecDeque<NodeId> {
        let mut ordered = VecDeque::new();

        if let Some(leader_hint) = self.leader_hint {
            if self.peers.contains_key(&leader_hint) {
                ordered.push_back(leader_hint);
            }
        }

        for &node_id in self.peers.keys() {
            if Some(node_id) != self.leader_hint {
                ordered.push_back(node_id);
            }
        }

        ordered
    }
}
