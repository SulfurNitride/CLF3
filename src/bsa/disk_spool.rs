//! Disk-backed storage for archive payloads produced by parallel compressors.
//!
//! The archive crates model compressed files as owned byte buffers. Keeping all
//! of those buffers alive until the final archive is written makes peak RSS
//! proportional to the complete archive size. `DiskSpool` lets workers release
//! their buffers immediately while retaining a cheap range into one temporary
//! file. The completed spool is memory-mapped read-only when the archive's
//! headers and payload are assembled.

use anyhow::{Context, Result};
use memmap2::{Mmap, MmapOptions};
use std::fs::File;
use std::io::{Seek, SeekFrom, Write};
use std::ops::Range;
use std::path::Path;
use std::sync::{Arc, Mutex};

#[derive(Clone)]
pub(crate) struct DiskSpoolWriter {
    file: Arc<Mutex<File>>,
}

pub(crate) struct DiskSpool {
    temp: File,
    writer: DiskSpoolWriter,
}

impl DiskSpool {
    pub(crate) fn new_near(output_path: &Path) -> Result<Self> {
        let parent = output_path.parent().unwrap_or_else(|| Path::new("."));
        std::fs::create_dir_all(parent)
            .with_context(|| format!("Failed to create archive directory: {}", parent.display()))?;

        // Keep the spool beside the destination. Using /tmp can silently put a
        // multi-gigabyte archive on tmpfs, recreating the very RAM pressure this
        // type is intended to avoid.
        // `tempfile_in` unlinks the directory entry immediately on Unix. A
        // killed/OOM process therefore cannot strand a 30+ GB named spool.
        let temp = tempfile::tempfile_in(parent)
            .with_context(|| format!("Failed to create archive spool in {}", parent.display()))?;
        let file = temp
            .try_clone()
            .context("Failed to open archive spool for parallel writers")?;

        Ok(Self {
            temp,
            writer: DiskSpoolWriter {
                file: Arc::new(Mutex::new(file)),
            },
        })
    }

    pub(crate) fn writer(&self) -> DiskSpoolWriter {
        self.writer.clone()
    }

    pub(crate) fn map(&self) -> Result<Mmap> {
        {
            let mut file = self
                .writer
                .file
                .lock()
                .map_err(|_| anyhow::anyhow!("Archive spool lock was poisoned"))?;
            file.flush().context("Failed to flush archive spool")?;
        }

        // SAFETY: the parallel writers have completed before map() is called,
        // and the mapping is read-only for the remainder of the spool's life.
        let mapping = unsafe { MmapOptions::new().map(&self.temp) }
            .context("Failed to memory-map archive spool")?;
        #[cfg(unix)]
        {
            // The archive writer consumes the mapping once, in order. This
            // hint lets Linux reclaim already-consumed file-backed pages
            // aggressively instead of allowing mapped RSS to resemble another
            // copy of the archive.
            let _ = mapping.advise(memmap2::Advice::Sequential);
        }
        Ok(mapping)
    }
}

impl DiskSpoolWriter {
    /// Append all buffers while holding the file lock once. Returned ranges
    /// refer to the eventual read-only mapping produced by `DiskSpool::map`.
    pub(crate) fn append<'a, I>(&self, buffers: I) -> Result<Vec<Range<usize>>>
    where
        I: IntoIterator<Item = &'a [u8]>,
    {
        let mut file = self
            .file
            .lock()
            .map_err(|_| anyhow::anyhow!("Archive spool lock was poisoned"))?;
        let mut position = file
            .seek(SeekFrom::End(0))
            .context("Failed to seek archive spool")?;
        let mut ranges = Vec::new();

        for bytes in buffers {
            let start: usize = position
                .try_into()
                .context("Archive spool exceeds addressable memory-map size")?;
            file.write_all(bytes)
                .context("Failed to append compressed archive data to spool")?;
            position = position
                .checked_add(bytes.len() as u64)
                .context("Archive spool size overflow")?;
            let end: usize = position
                .try_into()
                .context("Archive spool exceeds addressable memory-map size")?;
            ranges.push(start..end);
        }

        Ok(ranges)
    }
}
