use std::{
    fs::{self, File},
    io::{self, Write},
    path::{Path, PathBuf},
};

use crate::{
    entry::LogEntry,
    traits::{log_store::LogStore, snapshot_store::SnapshotStore, stable_store::StableStore},
    types::{HardState, LogIndex, NodeId, Snapshot, Term},
};

use super::codec::{CommandCodec, SnapshotCodec};

#[derive(Debug, Clone)]
pub struct FileStableStore {
    path: PathBuf,
    hard_state: HardState,
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

        let hard_state = if path.exists() {
            Self::read_hard_state(&path)?
        } else {
            HardState::default()
        };

        Ok(Self { path, hard_state })
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    fn read_hard_state(path: &Path) -> io::Result<HardState> {
        let contents = fs::read_to_string(path)?;

        if contents.trim().is_empty() {
            return Ok(HardState::default());
        }

        let mut current_term = None;
        let mut voted_for = None;
        let mut commit = None;

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
                other => {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        format!("unknown hard state field: {other}"),
                    ));
                }
            }
        }

        Ok(HardState {
            current_term: current_term.unwrap_or(0),
            voted_for: voted_for.unwrap_or(None),
            commit: commit.unwrap_or(0),
        })
    }

    fn persist_hard_state(&self, hs: &HardState) -> io::Result<()> {
        let tmp_path = self.path.with_extension("tmp");
        let voted_for = hs
            .voted_for
            .map(|id| id.to_string())
            .unwrap_or_else(|| "none".to_string());

        let encoded = format!(
            "current_term={}\nvoted_for={}\ncommit={}\n",
            hs.current_term, voted_for, hs.commit
        );

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
        self.persist_hard_state(&hs)
            .expect("failed to persist HardState to FileStableStore");
        self.hard_state = hs;
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
        let mut data_hex = None;

        for line in contents.lines().filter(|line| !line.trim().is_empty()) {
            let (key, value) = line.split_once('=').ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("invalid snapshot line: {line}"),
                )
            })?;

            match key.trim() {
                "last_included_index" => {
                    last_included_index = Some(parse_u64("last_included_index", value.trim())?)
                }
                "last_included_term" => {
                    last_included_term = Some(parse_u64("last_included_term", value.trim())?)
                }
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

        Ok(Some(Snapshot {
            last_included_index,
            last_included_term,
            data,
        }))
    }

    fn persist_snapshot(&self, snapshot: &Snapshot<S>) -> io::Result<()> {
        let tmp_path = self.path.with_extension("tmp");
        let data_bytes = self.codec.encode(&snapshot.data)?;
        let data_hex = encode_hex(&data_bytes);
        let encoded = format!(
            "last_included_index={}\nlast_included_term={}\ndata={}\n",
            snapshot.last_included_index, snapshot.last_included_term, data_hex
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
                    let mut parts = value.trim().splitn(3, ',');
                    let index_text = parts.next().ok_or_else(|| {
                        io::Error::new(io::ErrorKind::InvalidData, "missing entry index")
                    })?;
                    let term_text = parts.next().ok_or_else(|| {
                        io::Error::new(io::ErrorKind::InvalidData, "missing entry term")
                    })?;
                    let command_hex = parts.next().ok_or_else(|| {
                        io::Error::new(io::ErrorKind::InvalidData, "missing entry command bytes")
                    })?;

                    let command_bytes = decode_hex(command_hex)?;
                    let command = codec.decode(&command_bytes)?;

                    entries.push(LogEntry {
                        index: parse_u64("entry.index", index_text)?,
                        term: parse_u64("entry.term", term_text)?,
                        command,
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
            let command_bytes = self.codec.encode(&entry.command)?;
            let command_hex = encode_hex(&command_bytes);

            encoded.push_str(&format!(
                "entry={},{},{}\n",
                entry.index, entry.term, command_hex
            ));
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
            .unwrap_or(self.snapshot_index + 1)
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
        let expected_next = self.last_log_index() + 1;

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

        let remaining = self.entries(through + 1, usize::MAX);
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
            self.entries(last_included_index + 1, usize::MAX)
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

fn validate_loaded_entries<C>(snapshot_index: LogIndex, entries: &[LogEntry<C>]) -> io::Result<()> {
    if let Some(first) = entries.first() {
        if first.index != snapshot_index + 1 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "first entry index {} does not follow snapshot index {}",
                    first.index, snapshot_index
                ),
            ));
        }
    }

    for pair in entries.windows(2) {
        let prev = &pair[0];
        let next = &pair[1];

        if next.index != prev.index + 1 {
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

    if bytes.len() % 2 != 0 {
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
