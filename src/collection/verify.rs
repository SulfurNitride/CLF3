//! Archive hash helpers for collection installs (md5).

use std::fs::File;
use std::io::{BufReader, Read, Write};
use std::path::Path;

use anyhow::{Context, Result};

/// Compute MD5 hash of a file as a lowercase hex string.
pub fn compute_md5(path: &Path) -> Result<String> {
    let file = File::open(path).with_context(|| format!("Failed to open {}", path.display()))?;
    let mut reader = BufReader::with_capacity(1024 * 1024, file);
    let mut context = md5::Context::new();

    let mut buffer = [0u8; 65536];
    loop {
        let bytes_read = reader
            .read(&mut buffer)
            .with_context(|| format!("Failed to read {}", path.display()))?;
        if bytes_read == 0 {
            break;
        }
        context.write_all(&buffer[..bytes_read])?;
    }

    Ok(format!("{:x}", context.compute()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use tempfile::TempDir;

    #[test]
    fn test_compute_md5() {
        let temp_dir = TempDir::new().unwrap();
        let file_path = temp_dir.path().join("test.txt");
        let mut file = File::create(&file_path).unwrap();
        file.write_all(b"test").unwrap();
        let md5 = compute_md5(&file_path).unwrap();
        assert_eq!(md5, "098f6bcd4621d373cade4e832627b4f6");
    }
}
