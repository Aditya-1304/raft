use std::io;

pub trait CommandCodec<C> {
    fn encode(&self, command: &C) -> io::Result<Vec<u8>>;
    fn decode(&self, bytes: &[u8]) -> io::Result<C>;
}

pub trait SnapshotCodec<S> {
    fn encode(&self, snapshot: &S) -> io::Result<Vec<u8>>;
    fn decode(&self, bytes: &[u8]) -> io::Result<S>;
}

#[derive(Debug, Clone, Copy, Default)]
pub struct U64Codec;

impl CommandCodec<u64> for U64Codec {
    fn encode(&self, command: &u64) -> io::Result<Vec<u8>> {
        Ok(command.to_le_bytes().to_vec())
    }

    fn decode(&self, bytes: &[u8]) -> io::Result<u64> {
        let array: [u8; 8] = bytes.try_into().map_err(|_| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("expected 8 bytes for u64 command, got {}", bytes.len()),
            )
        })?;

        Ok(u64::from_le_bytes(array))
    }
}

impl SnapshotCodec<u64> for U64Codec {
    fn encode(&self, snapshot: &u64) -> io::Result<Vec<u8>> {
        Ok(snapshot.to_le_bytes().to_vec())
    }

    fn decode(&self, bytes: &[u8]) -> io::Result<u64> {
        decode_u64(bytes, "snapshot")
    }
}

#[derive(Debug, Clone, Copy, Default)]
pub struct UnitCodec;

impl CommandCodec<()> for UnitCodec {
    fn encode(&self, _command: &()) -> io::Result<Vec<u8>> {
        Ok(Vec::new())
    }

    fn decode(&self, bytes: &[u8]) -> io::Result<()> {
        if bytes.is_empty() {
            Ok(())
        } else {
            Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("expected empty bytes for unit command got {}", bytes.len()),
            ))
        }
    }
}

impl SnapshotCodec<()> for UnitCodec {
    fn encode(&self, _snapshot: &()) -> io::Result<Vec<u8>> {
        Ok(Vec::new())
    }

    fn decode(&self, bytes: &[u8]) -> io::Result<()> {
        decode_unit(bytes, "snapshot")
    }
}

fn decode_u64(bytes: &[u8], kind: &str) -> io::Result<u64> {
    let array: [u8; 8] = bytes.try_into().map_err(|_| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            format!("expected 8 bytes for u64 {kind}, got {}", bytes.len()),
        )
    })?;

    Ok(u64::from_le_bytes(array))
}

fn decode_unit(bytes: &[u8], kind: &str) -> io::Result<()> {
    if bytes.is_empty() {
        Ok(())
    } else {
        Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("expected empty bytes for unit {kind}, got {}", bytes.len()),
        ))
    }
}
