use std::{
    collections::BTreeSet,
    fs::{self, File},
    io::{self, Write},
    path::{Path, PathBuf},
};

use crate::{
    entry::{EntryPayload, LogEntry},
    traits::{log_store::LogStore, snapshot_store::SnapshotStore, stable_store::StableStore},
    types::{ConfChange, ConfChangeKind, ConfState, HardState, LogIndex, NodeId, Snapshot, Term},
};

use super::codec::{CommandCodec, SnapshotCodec};

#[derive(Debug, Clone)]
pub struct FileStableStore {
    path: PathBuf,
    hard_state: HardState,
    conf_state: Option<ConfState>,
}

pub struct FileSnapshotStore<S, Codec> {
    path: PathBuf,
    codec: Codec,
    snapshot: Option<Snapshot<S>>,
}

pub struct FileLogStore<C, Codec> {
    path: PathBuf,
    codec: Codec,
    entries: Vec<LogEntry<C>>,
    snapshot_index: LogIndex,
    snapshot_term: Term,
}

impl FileStableStore {
    pub fn open(path: impl Into<PathBuf>) -> io::Result<Self> {
        let path = path.into();

        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }

        let (hard_state, conf_state) = if path.exists() {
            Self::read_stable_state(&path)?
        } else {
            (HardState::default(), None)
        };

        Ok(Self {
            path,
            hard_state,
            conf_state,
        })
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    fn read_stable_state(path: &Path) -> io::Result<(HardState, Option<ConfState>)> {
        let contents = fs::read_to_string(path)?;

        if contents.trim().is_empty() {
            return Ok((HardState::default(), None));
        }

        let mut current_term = None;
        let mut voted_for = None;
        let mut commit = None;
        let mut conf_version = None;
        let mut voters = None;
        let mut learners = None;
        let mut outgoing_voters = None;

        for line in contents.lines().filter(|line| !line.trim().is_empty()) {
            let (key, value) = line.split_once('=').ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("invalid hard state line: {line}"),
                )
            })?;

            match key.trim() {
                "current_term" => current_term = Some(parse_u64("current_term", value.trim())?),
                "voted_for" => voted_for = Some(parse_optional_node_id(value.trim())?),
                "commit" => commit = Some(parse_u64("commit", value.trim())?),
                "conf_version" => conf_version = Some(parse_u64("conf_version", value.trim())?),
                "voters" => voters = Some(parse_replica_set("voters", value.trim())?),
                "learners" => learners = Some(parse_replica_set("learners", value.trim())?),
                "outgoing_voters" => {
                    outgoing_voters = Some(parse_replica_set("outgoing_voters", value.trim())?)
                }
                other => {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        format!("unknown hard state field: {other}"),
                    ));
                }
            }
        }

        let hard_state = HardState {
            current_term: required_field(current_term, "current_term")?,
            voted_for: required_field(voted_for, "voted_for")?,
            commit: required_field(commit, "commit")?,
        };

        let conf_state = match conf_version {
            Some(version) => {
                let state = ConfState {
                    version,
                    voters: required_field(voters, "voters")?,
                    learners: required_field(learners, "learners")?,
                    outgoing_voters: required_field(outgoing_voters, "outgoing_voters")?,
                };
                state.validate().map_err(|err| {
                    io::Error::new(
                        io::ErrorKind::InvalidData,
                        format!("invalid durable ConfState: {err:?}"),
                    )
                })?;
                Some(state)
            }
            None if voters.is_none() && learners.is_none() && outgoing_voters.is_none() => None,
            None => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "stable state contains ConfState sets without conf_version",
                ));
            }
        };

        Ok((hard_state, conf_state))
    }

    fn persist_stable_state(
        &self,
        hs: &HardState,
        conf_state: Option<&ConfState>,
    ) -> io::Result<()> {
        let tmp_path = self.path.with_extension("tmp");
        let voted_for = hs
            .voted_for
            .map(|id| id.to_string())
            .unwrap_or_else(|| "none".to_string());

        let mut encoded = format!(
            "current_term={}\nvoted_for={}\ncommit={}\n",
            hs.current_term, voted_for, hs.commit
        );
        if let Some(conf_state) = conf_state {
            encoded.push_str(&format!(
                "conf_version={}\nvoters={}\nlearners={}\noutgoing_voters={}\n",
                conf_state.version,
                encode_replica_set(&conf_state.voters),
                encode_replica_set(&conf_state.learners),
                encode_replica_set(&conf_state.outgoing_voters),
            ));
        }

        let mut tmp = File::create(&tmp_path)?;
        tmp.write_all(encoded.as_bytes())?;
        tmp.sync_all()?;
        drop(tmp);

        fs::rename(&tmp_path, &self.path)?;
        Ok(())
    }
}

impl StableStore for FileStableStore {
    fn hard_state(&self) -> HardState {
        self.hard_state.clone()
    }

    fn set_hard_state(&mut self, hs: HardState) {
        self.persist_stable_state(&hs, self.conf_state.as_ref())
            .expect("failed to persist HardState to FileStableStore");
        self.hard_state = hs;
    }

    fn conf_state(&self) -> Option<ConfState> {
        self.conf_state.clone()
    }

    fn set_conf_state(&mut self, conf_state: ConfState) {
        self.persist_stable_state(&self.hard_state, Some(&conf_state))
            .expect("failed to persist ConfState to FileStableStore");
        self.conf_state = Some(conf_state);
    }
}

impl<S, Codec> FileSnapshotStore<S, Codec>
where
    Codec: SnapshotCodec<S>,
{
    pub fn open(path: impl Into<PathBuf>, codec: Codec) -> io::Result<Self> {
        let path = path.into();

        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }

        let snapshot = if path.exists() {
            Self::read_snapshot(&path, &codec)?
        } else {
            None
        };

        Ok(Self {
            path,
            codec,
            snapshot,
        })
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    fn read_snapshot(path: &Path, codec: &Codec) -> io::Result<Option<Snapshot<S>>> {
        let contents = fs::read_to_string(path)?;

        if contents.trim().is_empty() {
            return Ok(None);
        }

        let mut last_included_index = None;
        let mut last_included_term = None;
        let mut snapshot_id = None;
        let mut size_bytes = None;
        let mut checksum_hex = None;
        let mut conf_version = None;
        let mut voters = None;
        let mut learners = None;
        let mut outgoing_voters = None;
        let mut data_hex = None;

        for line in contents.lines().filter(|line| !line.trim().is_empty()) {
            let (key, value) = line.split_once('=').ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("invalid snapshot line: {line}"),
                )
            })?;

            match key.trim() {
                "snapshot_id" => snapshot_id = Some(parse_u64("snapshot_id", value.trim())?),
                "last_included_index" => {
                    last_included_index = Some(parse_u64("last_included_index", value.trim())?)
                }
                "last_included_term" => {
                    last_included_term = Some(parse_u64("last_included_term", value.trim())?)
                }
                "conf_version" => conf_version = Some(parse_u64("conf_version", value.trim())?),
                "voters" => voters = Some(parse_replica_set("voters", value.trim())?),
                "learners" => learners = Some(parse_replica_set("learners", value.trim())?),
                "outgoing_voters" => {
                    outgoing_voters = Some(parse_replica_set("outgoing_voters", value.trim())?)
                }
                "size_bytes" => size_bytes = Some(parse_u64("size_bytes", value.trim())?),
                "checksum" => checksum_hex = Some(value.trim().to_string()),
                "data" => data_hex = Some(value.trim().to_string()),
                other => {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        format!("unknown snapshot field: {other}"),
                    ));
                }
            }
        }

        let last_included_index = last_included_index.ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                "snapshot file missing last_included_index",
            )
        })?;
        let last_included_term = last_included_term.ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                "snapshot file missing last_included_term",
            )
        })?;
        let data_hex = data_hex.ok_or_else(|| {
            io::Error::new(io::ErrorKind::InvalidData, "snapshot file missing data")
        })?;

        let data_bytes = decode_hex(&data_hex)?;
        let data = codec.decode(&data_bytes)?;
        let conf_state = ConfState {
            version: required_field(conf_version, "snapshot.conf_version")?,
            voters: required_field(voters, "snapshot.voters")?,
            learners: required_field(learners, "snapshot.learners")?,
            outgoing_voters: required_field(outgoing_voters, "snapshot.outgoing_voters")?,
        };
        conf_state.validate().map_err(|err| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("invalid snapshot ConfState: {err:?}"),
            )
        })?;
        let checksum_bytes = decode_hex(&required_field(checksum_hex, "snapshot.checksum")?)?;
        let checksum: [u8; 32] = checksum_bytes.try_into().map_err(|bytes: Vec<u8>| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("snapshot checksum must be 32 bytes, got {}", bytes.len()),
            )
        })?;

        Ok(Some(Snapshot {
            snapshot_id: required_field(snapshot_id, "snapshot.snapshot_id")?,
            last_included_index,
            last_included_term,
            conf_state,
            size_bytes: required_field(size_bytes, "snapshot.size_bytes")?,
            checksum,
            data,
        }))
    }

    fn persist_snapshot(&self, snapshot: &Snapshot<S>) -> io::Result<()> {
        let tmp_path = self.path.with_extension("tmp");
        let data_bytes = self.codec.encode(&snapshot.data)?;
        let data_hex = encode_hex(&data_bytes);
        let checksum_hex = encode_hex(&snapshot.checksum);
        let encoded = format!(
            "snapshot_id={}\nlast_included_index={}\nlast_included_term={}\nconf_version={}\nvoters={}\nlearners={}\noutgoing_voters={}\nsize_bytes={}\nchecksum={}\ndata={}\n",
            snapshot.snapshot_id,
            snapshot.last_included_index,
            snapshot.last_included_term,
            snapshot.conf_state.version,
            encode_replica_set(&snapshot.conf_state.voters),
            encode_replica_set(&snapshot.conf_state.learners),
            encode_replica_set(&snapshot.conf_state.outgoing_voters),
            snapshot.size_bytes,
            checksum_hex,
            data_hex
        );

        let mut tmp = File::create(&tmp_path)?;
        tmp.write_all(encoded.as_bytes())?;
        tmp.sync_all()?;
        drop(tmp);

        fs::rename(&tmp_path, &self.path)?;
        Ok(())
    }
}

impl<S, Codec> SnapshotStore<S> for FileSnapshotStore<S, Codec>
where
    Codec: SnapshotCodec<S>,
{
    fn latest(&self) -> Option<&Snapshot<S>> {
        self.snapshot.as_ref()
    }

    fn save(&mut self, snapshot: Snapshot<S>) {
        self.persist_snapshot(&snapshot)
            .expect("failed to persist snapshot to FileSnapshotStore");
        self.snapshot = Some(snapshot);
    }
}

impl<C, Codec> FileLogStore<C, Codec>
where
    C: Clone,
    Codec: CommandCodec<C>,
{
    pub fn open(path: impl Into<PathBuf>, codec: Codec) -> io::Result<Self> {
        let path = path.into();

        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }

        let (snapshot_index, snapshot_term, entries) = if path.exists() {
            Self::read_log_state(&path, &codec)?
        } else {
            (0, 0, Vec::new())
        };

        Ok(Self {
            path,
            codec,
            entries,
            snapshot_index,
            snapshot_term,
        })
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    fn read_log_state(
        path: &Path,
        codec: &Codec,
    ) -> io::Result<(LogIndex, Term, Vec<LogEntry<C>>)> {
        let contents = fs::read_to_string(path)?;

        if contents.trim().is_empty() {
            return Ok((0, 0, Vec::new()));
        }

        let mut snapshot_index = 0;
        let mut snapshot_term = 0;
        let mut entries = Vec::new();

        for line in contents.lines().filter(|line| !line.trim().is_empty()) {
            let (key, value) = line.split_once('=').ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("invalid log-state line: {line}"),
                )
            })?;

            match key.trim() {
                "snapshot_index" => snapshot_index = parse_u64("snapshot_index", value.trim())?,
                "snapshot_term" => snapshot_term = parse_u64("snapshot_term", value.trim())?,
                "entry" => {
                    let parts: Vec<&str> = value.trim().splitn(4, ',').collect();
                    if parts.len() < 3 {
                        return Err(io::Error::new(
                            io::ErrorKind::InvalidData,
                            "log entry is missing required fields",
                        ));
                    }
                    let index_text = parts[0];
                    let term_text = parts[1];
                    let (payload, encoded_len) = if parts.len() == 3 {
                        // Read the pre-ConfState standalone demo format as a
                        // normal command so existing local demos remain usable.
                        let command_bytes = decode_hex(parts[2])?;
                        let encoded_len = command_bytes.len();
                        (
                            EntryPayload::Normal(codec.decode(&command_bytes)?),
                            encoded_len,
                        )
                    } else {
                        match parts[2] {
                            "normal" => {
                                let command_bytes = decode_hex(parts[3])?;
                                let encoded_len = command_bytes.len();
                                (
                                    EntryPayload::Normal(codec.decode(&command_bytes)?),
                                    encoded_len,
                                )
                            }
                            "configuration" => (
                                EntryPayload::Configuration(decode_conf_change(parts[3])?),
                                24,
                            ),
                            kind => {
                                return Err(io::Error::new(
                                    io::ErrorKind::InvalidData,
                                    format!("unknown log entry payload kind: {kind}"),
                                ));
                            }
                        }
                    };

                    entries.push(LogEntry {
                        index: parse_u64("entry.index", index_text)?,
                        term: parse_u64("entry.term", term_text)?,
                        encoded_len,
                        payload,
                    });
                }
                other => {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        format!("unknown log-state field: {other}"),
                    ));
                }
            }
        }

        validate_loaded_entries(snapshot_index, &entries)?;
        Ok((snapshot_index, snapshot_term, entries))
    }

    fn persist_log_state(&self) -> io::Result<()> {
        let tmp_path = self.path.with_extension("tmp");
        let mut encoded = format!(
            "snapshot_index={}\nsnapshot_term={}\n",
            self.snapshot_index, self.snapshot_term
        );

        for entry in &self.entries {
            match &entry.payload {
                EntryPayload::Normal(command) => {
                    let command_bytes = self.codec.encode(command)?;
                    let command_hex = encode_hex(&command_bytes);
                    encoded.push_str(&format!(
                        "entry={},{},normal,{}\n",
                        entry.index, entry.term, command_hex
                    ));
                }
                EntryPayload::Configuration(change) => {
                    encoded.push_str(&format!(
                        "entry={},{},configuration,{}\n",
                        entry.index,
                        entry.term,
                        encode_conf_change(change)
                    ));
                }
            }
        }

        let mut tmp = File::create(&tmp_path)?;
        tmp.write_all(encoded.as_bytes())?;
        tmp.sync_all()?;
        drop(tmp);

        fs::rename(&tmp_path, &self.path)?;
        Ok(())
    }

    fn set_snapshot_boundary(&mut self, index: LogIndex, term: Term) {
        self.snapshot_index = index;
        self.snapshot_term = term;
    }

    fn first_log_index(&self) -> LogIndex {
        self.entries
            .first()
            .map(|entry| entry.index)
            .unwrap_or(self.snapshot_index.saturating_add(1))
    }

    fn last_log_index(&self) -> LogIndex {
        self.entries
            .last()
            .map(|entry| entry.index)
            .unwrap_or(self.snapshot_index)
    }

    fn offset(&self, index: LogIndex) -> Option<usize> {
        let first = self.first_log_index();
        let last = self.last_log_index();

        if index < first || index > last {
            None
        } else {
            Some((index - first) as usize)
        }
    }

    fn truncate_suffix_in_memory(&mut self, from: LogIndex) {
        let first = self.first_log_index();
        let last = self.last_log_index();

        if from > last || from < first {
            return;
        }

        if from == first {
            self.entries.clear();
            return;
        }

        if let Some(offset) = self.offset(from) {
            self.entries.truncate(offset);
        }
    }
}

impl<C, Codec> LogStore<C> for FileLogStore<C, Codec>
where
    C: Clone,
    Codec: CommandCodec<C>,
{
    fn first_index(&self) -> LogIndex {
        self.first_log_index()
    }

    fn last_index(&self) -> LogIndex {
        self.last_log_index()
    }

    fn term(&self, index: LogIndex) -> Option<Term> {
        if self.snapshot_index != 0 && index == self.snapshot_index {
            return Some(self.snapshot_term);
        }

        self.offset(index)
            .and_then(|offset| self.entries.get(offset))
            .map(|entry| entry.term)
    }

    fn entry(&self, index: LogIndex) -> Option<LogEntry<C>> {
        self.offset(index)
            .and_then(|offset| self.entries.get(offset))
            .cloned()
    }

    fn entries(&self, from: LogIndex, max: usize) -> Vec<LogEntry<C>> {
        if max == 0 {
            return Vec::new();
        }

        let start = from.max(self.first_log_index());
        let Some(offset) = self.offset(start) else {
            return Vec::new();
        };

        self.entries
            .iter()
            .skip(offset)
            .take(max)
            .cloned()
            .collect()
    }

    fn append(&mut self, entries: &[LogEntry<C>]) {
        if entries.is_empty() {
            return;
        }

        let first_new_index = entries[0].index;
        let expected_next = self.last_log_index().saturating_add(1);

        if first_new_index > expected_next {
            panic!(
                "attempted to append non-contiguous entries: first_new_index={}, expected_next={}",
                first_new_index, expected_next
            );
        }

        self.truncate_suffix_in_memory(first_new_index);
        self.entries.extend_from_slice(entries);
        self.persist_log_state()
            .expect("failed to persist log state during append");
    }

    fn truncate_suffix(&mut self, from: LogIndex) {
        let old_len = self.entries.len();
        self.truncate_suffix_in_memory(from);

        if self.entries.len() != old_len {
            self.persist_log_state()
                .expect("failed to persist log state during truncate_suffix");
        }
    }

    fn compact(&mut self, through: LogIndex) {
        if through <= self.snapshot_index {
            return;
        }

        let Some(term) = self.term(through) else {
            return;
        };

        let remaining = self.entries(through.saturating_add(1), usize::MAX);
        self.entries = remaining;
        self.set_snapshot_boundary(through, term);

        self.persist_log_state()
            .expect("failed to persist log state during compact");
    }

    fn install_snapshot(&mut self, last_included_index: LogIndex, last_included_term: Term) {
        if last_included_index < self.snapshot_index {
            return;
        }

        if last_included_index == self.snapshot_index && last_included_term == self.snapshot_term {
            return;
        }

        let keep_suffix = matches!(
            self.term(last_included_index),
            Some(term) if term == last_included_term
        );

        self.entries = if keep_suffix {
            self.entries(last_included_index.saturating_add(1), usize::MAX)
        } else {
            Vec::new()
        };

        self.set_snapshot_boundary(last_included_index, last_included_term);

        self.persist_log_state()
            .expect("failed to persist log state during install_snapshot");
    }
}

fn parse_u64(field: &str, value: &str) -> io::Result<u64> {
    value.parse().map_err(|err| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            format!("invalid {field} value `{value}`: {err}"),
        )
    })
}

fn parse_optional_node_id(value: &str) -> io::Result<Option<NodeId>> {
    if value == "none" {
        Ok(None)
    } else {
        parse_u64("voted_for", value).map(Some)
    }
}

fn parse_replica_set(field: &str, value: &str) -> io::Result<BTreeSet<NodeId>> {
    if value.is_empty() {
        return Ok(BTreeSet::new());
    }

    value
        .split(',')
        .map(|part| parse_u64(field, part))
        .collect()
}

fn encode_replica_set(values: &BTreeSet<NodeId>) -> String {
    values
        .iter()
        .map(u64::to_string)
        .collect::<Vec<_>>()
        .join(",")
}

fn required_field<T>(value: Option<T>, field: &str) -> io::Result<T> {
    value.ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            format!("stable state file missing {field}"),
        )
    })
}

fn encode_conf_change(change: &ConfChange) -> String {
    let (kind, replica_id) = match change.kind {
        ConfChangeKind::AddLearner(replica_id) => ("add_learner", replica_id),
        ConfChangeKind::PromoteLearner(replica_id) => ("promote_learner", replica_id),
        ConfChangeKind::RemoveReplica(replica_id) => ("remove_replica", replica_id),
    };
    format!("{}:{}:{}", change.expected_version, kind, replica_id)
}

fn decode_conf_change(value: &str) -> io::Result<ConfChange> {
    let mut parts = value.split(':');
    let expected_version = parse_u64(
        "configuration.expected_version",
        parts.next().ok_or_else(|| {
            io::Error::new(io::ErrorKind::InvalidData, "missing configuration version")
        })?,
    )?;
    let kind = parts
        .next()
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "missing configuration kind"))?;
    let replica_id = parse_u64(
        "configuration.replica_id",
        parts.next().ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                "missing configuration replica ID",
            )
        })?,
    )?;
    if parts.next().is_some() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "configuration payload contains trailing fields",
        ));
    }

    let kind = match kind {
        "add_learner" => ConfChangeKind::AddLearner(replica_id),
        "promote_learner" => ConfChangeKind::PromoteLearner(replica_id),
        "remove_replica" => ConfChangeKind::RemoveReplica(replica_id),
        other => {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("unknown configuration change kind: {other}"),
            ));
        }
    };
    Ok(ConfChange {
        expected_version,
        kind,
    })
}

fn validate_loaded_entries<C>(snapshot_index: LogIndex, entries: &[LogEntry<C>]) -> io::Result<()> {
    if let Some(first) = entries.first()
        && first.index != snapshot_index.saturating_add(1)
    {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!(
                "first entry index {} does not follow snapshot index {}",
                first.index, snapshot_index
            ),
        ));
    }

    for pair in entries.windows(2) {
        let prev = &pair[0];
        let next = &pair[1];

        if next.index != prev.index.saturating_add(1) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "non-contiguous log entries loaded from disk: {} then {}",
                    prev.index, next.index
                ),
            ));
        }
    }

    Ok(())
}

fn encode_hex(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";

    let mut out = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        out.push(HEX[(byte >> 4) as usize] as char);
        out.push(HEX[(byte & 0x0f) as usize] as char);
    }
    out
}

fn decode_hex(input: &str) -> io::Result<Vec<u8>> {
    let bytes = input.as_bytes();

    if !bytes.len().is_multiple_of(2) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("hex payload must have even length, got {}", bytes.len()),
        ));
    }

    let mut out = Vec::with_capacity(bytes.len() / 2);
    let mut i = 0;

    while i < bytes.len() {
        let high = hex_value(bytes[i])?;
        let low = hex_value(bytes[i + 1])?;
        out.push((high << 4) | low);
        i += 2;
    }

    Ok(out)
}

fn hex_value(ch: u8) -> io::Result<u8> {
    match ch {
        b'0'..=b'9' => Ok(ch - b'0'),
        b'a'..=b'f' => Ok(ch - b'a' + 10),
        b'A'..=b'F' => Ok(ch - b'A' + 10),
        _ => Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("invalid hex digit `{}`", ch as char),
        )),
    }
}
