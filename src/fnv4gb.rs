//! FNV 4GB patch — set IMAGE_FILE_LARGE_ADDRESS_AWARE on FNV executables.
//!
//! Fallout New Vegas is a 32-bit binary. On 64-bit systems (including Proton)
//! it can access more than 2 GB of RAM if the LAA flag is set in the PE
//! Characteristics field. This is a one-byte-range in-place patch; the binary
//! on disk is otherwise identical to the stock exe.
//!
//! This replaces the FNV4GB / FNV4GB_Proton tools with a native Rust
//! implementation: same result, no download required.

use anyhow::{Context, Result};
use std::fs::OpenOptions;
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::Path;

const IMAGE_FILE_LARGE_ADDRESS_AWARE: u16 = 0x0020;

const FNV_EXES: &[&str] = &["FalloutNV.exe", "FalloutNVLauncher.exe"];

/// Locate the PE Characteristics field inside `data`.
///
/// Returns `(characteristics_value, byte_offset_in_file)` or `None` if the
/// buffer is not a valid PE image.
fn pe_characteristics(data: &[u8]) -> Option<(u16, usize)> {
    if data.len() < 0x40 || data[0] != b'M' || data[1] != b'Z' {
        return None;
    }
    let pe_offset =
        u32::from_le_bytes(data[0x3C..0x40].try_into().ok()?) as usize;

    // PE signature "PE\0\0" + 4 (sig) + 18 (COFF fields before Characteristics)
    let chars_offset = pe_offset.checked_add(22)?;
    if data.len() < chars_offset + 2 {
        return None;
    }
    if &data[pe_offset..pe_offset + 4] != b"PE\0\0" {
        return None;
    }
    let val = u16::from_le_bytes(data[chars_offset..chars_offset + 2].try_into().ok()?);
    Some((val, chars_offset))
}

/// Return true if the executable already has IMAGE_FILE_LARGE_ADDRESS_AWARE set.
pub fn is_large_address_aware(exe_path: &Path) -> bool {
    let mut data = Vec::new();
    if std::fs::File::open(exe_path)
        .and_then(|mut f| f.read_to_end(&mut data))
        .is_err()
    {
        return false;
    }
    pe_characteristics(&data)
        .map(|(chars, _)| chars & IMAGE_FILE_LARGE_ADDRESS_AWARE != 0)
        .unwrap_or(false)
}

/// Set IMAGE_FILE_LARGE_ADDRESS_AWARE in the PE Characteristics field in-place.
///
/// Returns `Ok(true)` if the patch was applied, `Ok(false)` if already set.
fn patch_exe(exe_path: &Path) -> Result<bool> {
    let mut data = Vec::new();
    std::fs::File::open(exe_path)
        .and_then(|mut f| f.read_to_end(&mut data))
        .with_context(|| format!("Failed to read {}", exe_path.display()))?;

    let (chars, offset) = pe_characteristics(&data)
        .with_context(|| format!("Not a valid PE executable: {}", exe_path.display()))?;

    if chars & IMAGE_FILE_LARGE_ADDRESS_AWARE != 0 {
        return Ok(false);
    }

    let new_chars = chars | IMAGE_FILE_LARGE_ADDRESS_AWARE;
    let mut file = OpenOptions::new()
        .write(true)
        .open(exe_path)
        .with_context(|| format!("Failed to open for writing: {}", exe_path.display()))?;
    file.seek(SeekFrom::Start(offset as u64))
        .context("Failed to seek to PE Characteristics")?;
    file.write_all(&new_chars.to_le_bytes())
        .context("Failed to write PE Characteristics")?;

    Ok(true)
}

/// Ensure every FNV executable in `game_dir` has the 4GB LAA flag set.
///
/// Silently skips exes that are already patched or not present. Logs progress
/// via `reporter`. Errors from one exe do not stop patching of the others, but
/// all errors are returned as a combined error at the end.
pub fn ensure_fnv_4gb_patched(
    game_dir: &Path,
    reporter: &dyn crate::installer::progress::ProgressReporter,
) -> Result<()> {
    let mut errors: Vec<String> = Vec::new();

    for &exe_name in FNV_EXES {
        let exe_path = game_dir.join(exe_name);
        if !exe_path.exists() {
            continue;
        }

        match patch_exe(&exe_path) {
            Ok(true) => reporter.log(&format!("4GB patch applied to {}", exe_name)),
            Ok(false) => reporter.log(&format!("{} already 4GB patched", exe_name)),
            Err(e) => {
                let msg = format!("Failed to 4GB patch {}: {:#}", exe_name, e);
                reporter.log(&format!("Warning: {}", msg));
                errors.push(msg);
            }
        }
    }

    if errors.is_empty() {
        Ok(())
    } else {
        anyhow::bail!("FNV 4GB patch errors:\n{}", errors.join("\n"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use tempfile::NamedTempFile;

    fn fake_pe(laa: bool) -> Vec<u8> {
        // Minimal PE stub: DOS header (0x40 bytes) + PE header
        let mut data = vec![0u8; 0x80];
        // MZ magic
        data[0] = b'M';
        data[1] = b'Z';
        // e_lfanew at 0x3C = 0x40 (right after DOS stub)
        data[0x3C] = 0x40;
        // PE signature
        data[0x40] = b'P';
        data[0x41] = b'E';
        data[0x42] = 0;
        data[0x43] = 0;
        // Characteristics at 0x40 + 22 = 0x56
        let chars: u16 = if laa { 0x0122 } else { 0x0102 };
        data[0x56] = (chars & 0xFF) as u8;
        data[0x57] = (chars >> 8) as u8;
        data
    }

    #[test]
    fn detects_laa_flag() {
        let mut f = NamedTempFile::new().unwrap();
        f.write_all(&fake_pe(true)).unwrap();
        assert!(is_large_address_aware(f.path()));
    }

    #[test]
    fn detects_missing_laa_flag() {
        let mut f = NamedTempFile::new().unwrap();
        f.write_all(&fake_pe(false)).unwrap();
        assert!(!is_large_address_aware(f.path()));
    }

    #[test]
    fn patch_sets_laa_flag() {
        let mut f = NamedTempFile::new().unwrap();
        f.write_all(&fake_pe(false)).unwrap();
        let path = f.path().to_path_buf();

        let newly_patched = patch_exe(&path).unwrap();
        assert!(newly_patched, "should report newly patched");
        assert!(is_large_address_aware(&path), "flag should now be set");
    }

    #[test]
    fn patch_is_idempotent() {
        let mut f = NamedTempFile::new().unwrap();
        f.write_all(&fake_pe(true)).unwrap();
        let path = f.path().to_path_buf();

        let newly_patched = patch_exe(&path).unwrap();
        assert!(!newly_patched, "should report already patched");
    }
}
