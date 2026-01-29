# CLF3 Installer Revamp Plan

Based on analysis of the working Collections Manager (MO2) reference implementation.

## Progress Status

- [x] **Phase 1: Extraction Rewrite** - COMPLETED
- [x] **Phase 2: FOMOD Handler** - COMPLETED
- [x] **Phase 3: Root Builder** - COMPLETED
- [ ] **Phase 4: Testing** - TODO

---

## 1. Archive Extraction - COMPLETED ✓

### Changes Made:
- Rewrote `detect_wrapper_folder` with simple reference logic
- **Rule:** Only unwrap if EXACTLY ONE folder AND NO real files
- Removed `select_variant_folder` entirely (reference doesn't do this)
- Removed complex content folder detection heuristics

### New Logic (from reference):
```rust
fn detect_wrapper_folder(extracted_path: &Path) -> PathBuf {
    // Ignorable files: readme.txt, license.txt, meta.ini, preview images
    // Only unwrap if: exactly 1 dir AND no real files
    // Always unwrap "Data" folder
    // Maximum 3 levels of unwrapping
}
```

---

## 2. FOMOD Handler - COMPLETED ✓

### Changes Made:
- Step+group matching uses case-insensitive comparison
- Added "Data/" prefix stripping (MO2 mods are implicitly in Data context)
- Verified empty destination handling (files use just filename)
- Verified folder installation (is_folder flag from `<folder>` XML element)

### File Destination Rules (verified):

| Source | Destination | Result |
|--------|-------------|--------|
| `file.esp` | `""` (empty) | `file.esp` at root ✓ |
| `folder/file.esp` | `""` (empty) | `file.esp` at root (just filename) ✓ |
| `file.esp` | `"/"` | `file.esp` at root ✓ |
| `file.esp` | `"Data/"` | `file.esp` at root (Data is implicit) ✓ FIXED |
| `file.esp` | `"scripts/"` | `scripts/file.esp` ✓ |
| `file.esp` | `"scripts/file.esp"` | `scripts/file.esp` ✓ |
| `folder/` (is_folder) | `""` | Copy folder CONTENTS to root ✓ |
| `folder/` (is_folder) | `"dest/"` | Copy folder contents to `dest/` ✓ |

### Code Fix Applied:
```rust
// Strip "Data/" or "Data" prefix - MO2 mods are implicitly in Data folder context
let dest_lower = dest_normalized.to_lowercase();
if dest_lower == "data" || dest_lower == "data/" || dest_lower == "data\\" {
    dest_normalized = String::new();
} else if dest_lower.starts_with("data/") {
    dest_normalized = dest_normalized[5..].to_string();
}
```

---

## 3. Root Builder - COMPLETED ✓

### Changes Made:
- Expanded `is_root_level_file` to detect ENB configs and enbseries/ folder
- Added `get_root_dest_path` to preserve enbseries/ folder structure
- Fixed SKSE/F4SE plugin detection (plugins/ folder goes to Data, not root)

### Root File Detection (updated):
```rust
// ENB config files at root
const ENB_ROOT_FILES: &[&str] = &[
    "enbseries.ini", "enblocal.ini", "enbbloom.fx",
    "enbeffect.fx", "enbeffectprepass.fx", "enbadaptation.fx",
    "enblens.fx", "enbdepthoffield.fx", "enbpalette.png",
    "d3dcompiler_46e.dll",
];

// enbseries/ folder contents → preserve structure in game root
// DLLs/EXEs at root (not in data/, skse/plugins/, etc.)
```

### Root Destination Logic:
```rust
fn get_root_dest_path(stock_game_dir: &Path, rel_path: &str) -> PathBuf {
    // enbseries/ folder → preserve structure (game_root/enbseries/...)
    // Regular root files → flatten to filename (game_root/d3d11.dll)
}
```

### File Routing Summary:
| File Type | Destination |
|-----------|-------------|
| `d3d11.dll`, `dinput8.dll` | `game_root/` (flattened) |
| `enbseries.ini` | `game_root/` (flattened) |
| `enbseries/effect.txt` | `game_root/enbseries/effect.txt` (preserved) |
| `SKSE/Plugins/*.dll` | `mod_folder/SKSE/Plugins/` (MO2 virtualizes) |
| `skse64_loader.exe` | `game_root/` (flattened) |

---

## 4. Testing - TODO

### Test Cases:
1. Simple texture mod (meshes + textures folders)
2. Plugin mod (.esp at root)
3. SKSE plugin (SKSE/Plugins structure)
4. Root mod (ENB with DLLs)
5. FOMOD mod with choices
6. Wrapper folder mod (ModName v1.0/...)
7. Data folder mod (Data/meshes/...)

### Success Criteria:
- [ ] No red X mods in MO2
- [ ] Correct folder structure preserved
- [ ] FOMOD choices apply correctly
- [ ] Root files in Stock Game folder

---

## Files Modified

| File | Status | Changes |
|------|--------|---------|
| `src/collection/extract.rs` | ✓ Done | Simplified wrapper detection, expanded root file detection, ENB folder handling |
| `src/collection/fomod/executor.rs` | ✓ Done | Added Data/ prefix stripping for implicit Data context |
| `src/collection/installer.rs` | OK | Uses new extraction |

---

## Reference Implementation Key Insights

From `/home/luke/Downloads/Collections Manager (MO2)-1594-2-5-1769312217/`:

1. **Simple wrapper detection** - Only unwrap if 1 folder + no files
2. **No variant selection** - Extract everything, let FOMOD handle it
3. **Sequential processing** - One mod at a time, strict order
4. **Marker files** - Track what's installed with JSON markers
5. **FOMOD via dialog automation** - Uses Python/Qt to click through MO2's FOMOD dialog (we parse XML directly)
