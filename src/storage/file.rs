use std::{
    fs::{self, File},
    io::{self, Write},
    path::{Path, PathBuf},
};

use crate::{
    traits::stable_store::StableStore,
    types::{HardState, NodeId},
};

#[derive(Debug, Clone)]
pub struct FileStableStore {
    path: PathBuf,
    hard_state: HardState,
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

fn parse_u64(field: &str, value: &str) -> io::Result<u64> {
    value.parse().map_err(|err| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            format!("invalid {field} value `{value}` : {err}"),
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
