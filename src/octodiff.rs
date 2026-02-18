//! OctoDiff binary delta reader
//!
//! OctoDiff is a binary patching format from Octopus Deploy.
//! It stores instructions to transform a "basis" file into a "target" file
//! using two commands:
//! - Copy: read bytes from the basis file
//! - Write: insert new bytes from the delta
//!
//! Reference: https://github.com/OctopusDeploy/Octodiff

// Module not yet integrated into main installation pipeline
#![allow(dead_code)]

use anyhow::{bail, Context, Result};
use binrw::prelude::*;
use std::io::{Read, Seek, SeekFrom};

/// Magic bytes at start of OctoDiff files
const MAGIC: &[u8; 9] = b"OCTODELTA";

/// End of metadata marker
const END_MARKER: &[u8; 3] = b">>>";

/// Command byte for Copy instruction
const CMD_COPY: u8 = 0x60;

/// Command byte for Write instruction
const CMD_WRITE: u8 = 0x80;

/// OctoDiff file header
#[derive(Debug, BinRead)]
#[br(little, magic = b"OCTODELTA")]
struct Header {
    /// Version (should be 0x01)
    version: u8,

    /// Hash algorithm name length
    hash_algo_len: u8,

    /// Hash algorithm name bytes (typically "SHA1")
    #[br(count = hash_algo_len)]
    hash_algo: Vec<u8>,

    /// Hash length in bytes
    hash_len: i32,

    /// Hash of the expected output file
    #[br(count = hash_len)]
    hash: Vec<u8>,

    /// End of metadata marker
    #[br(assert(end_marker == *END_MARKER, "invalid end marker"))]
    end_marker: [u8; 3],
}

/// A delta command
#[derive(Debug)]
enum Command {
    /// Copy bytes from the basis file
    Copy { offset: u64, length: usize },
    /// Write new bytes from the delta file
    Write { length: usize },
}

/// Read the next command from the delta stream
fn read_command<R: Read>(reader: &mut R) -> Result<Option<Command>> {
    let mut cmd_byte = [0u8; 1];
    match reader.read_exact(&mut cmd_byte) {
        Ok(()) => {}
        Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => return Ok(None),
        Err(e) => return Err(e).context("reading command byte"),
    }

    match cmd_byte[0] {
        CMD_COPY => {
            let mut buf = [0u8; 16];
            reader.read_exact(&mut buf).context("reading copy params")?;
            let offset = i64::from_le_bytes(buf[0..8].try_into().unwrap());
            let length = i64::from_le_bytes(buf[8..16].try_into().unwrap());
            Ok(Some(Command::Copy {
                offset: offset as u64,
                length: length as usize,
            }))
        }
        CMD_WRITE => {
            let mut buf = [0u8; 8];
            reader
                .read_exact(&mut buf)
                .context("reading write length")?;
            let length = i64::from_le_bytes(buf);
            Ok(Some(Command::Write {
                length: length as usize,
            }))
        }
        unknown => bail!("unknown command byte: 0x{:02x}", unknown),
    }
}

/// Applies a delta to produce the target file
///
/// Implements `Read` so you can stream the output without buffering.
pub struct DeltaReader<B: Read + Seek, D: Read + Seek> {
    /// The basis (original) file
    basis: B,
    /// The delta (patch) file
    delta: D,
    /// Current command being processed
    current: Option<CommandState>,
    /// Expected output hash (for verification)
    pub expected_hash: Vec<u8>,
}

/// State for partially-processed command
enum CommandState {
    Copy { remaining: usize },
    Write { remaining: usize },
}

impl<B: Read + Seek, D: Read + Seek> DeltaReader<B, D> {
    /// Create a new delta reader
    ///
    /// - `basis`: The original file to patch
    /// - `delta`: The delta/patch file
    pub fn new(basis: B, mut delta: D) -> Result<Self> {
        // Read and validate header
        let header = Header::read(&mut delta).context("reading OctoDiff header")?;

        if header.version != 0x01 {
            bail!("unsupported OctoDiff version: {}", header.version);
        }

        Ok(Self {
            basis,
            delta,
            current: None,
            expected_hash: header.hash,
        })
    }

    /// Read and set up the next command
    fn advance(&mut self) -> Result<bool> {
        match read_command(&mut self.delta)? {
            Some(Command::Copy { offset, length }) => {
                self.basis
                    .seek(SeekFrom::Start(offset))
                    .context("seeking in basis file")?;
                self.current = Some(CommandState::Copy { remaining: length });
                Ok(true)
            }
            Some(Command::Write { length }) => {
                self.current = Some(CommandState::Write { remaining: length });
                Ok(true)
            }
            None => {
                self.current = None;
                Ok(false)
            }
        }
    }
}

impl<B: Read + Seek, D: Read + Seek> Read for DeltaReader<B, D> {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        loop {
            // If no current command, try to get the next one
            if self.current.is_none() {
                match self.advance() {
                    Ok(true) => {}
                    Ok(false) => return Ok(0), // EOF
                    Err(e) => return Err(std::io::Error::other(e)),
                }
            }

            // Process current command
            match &mut self.current {
                Some(CommandState::Copy { remaining }) => {
                    let to_read = buf.len().min(*remaining);
                    let n = self.basis.read(&mut buf[..to_read])?;
                    if n == 0 {
                        return Err(std::io::Error::new(
                            std::io::ErrorKind::UnexpectedEof,
                            "basis file ended early",
                        ));
                    }
                    *remaining -= n;
                    if *remaining == 0 {
                        self.current = None;
                    }
                    return Ok(n);
                }
                Some(CommandState::Write { remaining }) => {
                    let to_read = buf.len().min(*remaining);
                    let n = self.delta.read(&mut buf[..to_read])?;
                    if n == 0 {
                        return Err(std::io::Error::new(
                            std::io::ErrorKind::UnexpectedEof,
                            "delta file ended early",
                        ));
                    }
                    *remaining -= n;
                    if *remaining == 0 {
                        self.current = None;
                    }
                    return Ok(n);
                }
                None => continue, // Loop to get next command
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    #[test]
    fn test_header_parsing() {
        // Minimal valid OctoDiff header
        let mut data = Vec::new();
        data.extend_from_slice(MAGIC); // Magic
        data.push(0x01); // Version
        data.push(4); // Hash algo name length
        data.extend_from_slice(b"SHA1"); // Hash algo name
        data.extend_from_slice(&20i32.to_le_bytes()); // Hash length
        data.extend_from_slice(&[0u8; 20]); // Hash (zeros)
        data.extend_from_slice(END_MARKER); // End marker

        let header = Header::read(&mut Cursor::new(&data)).unwrap();
        assert_eq!(header.version, 0x01);
        assert_eq!(&header.hash_algo, b"SHA1");
        assert_eq!(header.hash.len(), 20);
    }

    #[test]
    #[ignore] // Run with: cargo test -- --ignored
    fn test_real_tuxborn_patch() {
        use std::fs::File;
        use std::path::Path;

        let patch_path = Path::new("/home/luke/Documents/Wabbajack Rust Update/Tuxborn-Wabbajack/Tuxborn/00025e30-0d5f-4482-99f6-3ecf59d73160");
        if !patch_path.exists() {
            return;
        }

        let mut file = File::open(patch_path).unwrap();
        let header = Header::read(&mut file).unwrap();

        println!("Version: {}", header.version);
        println!("Hash algo: {}", String::from_utf8_lossy(&header.hash_algo));
        println!("Hash: {:?}", header.hash);

        assert_eq!(header.version, 0x01);
        assert_eq!(&header.hash_algo, b"SHA1");
        assert_eq!(header.hash.len(), 20);
    }
}
