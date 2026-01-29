# CLF3 Development Gameplan

## Project Overview

**CLF3 (Chlorine Trifluoride)** - A Linux-native modlist installer supporting:
1. **Wabbajack modlists** (current - working)
2. **Nexus/Vortex Collections** (planned)
3. **Windows support** (future)

**Target Game**: Skyrim Special Edition (Steam) - expanding to other games later

---

## Architecture

```
clf3/
├── src/
│   ├── lib.rs                     # Library exports
│   ├── main.rs                    # CLI: `wabbajack` and `collection` commands
│   │
│   ├── collection/                # Nexus Collection support
│   │   ├── mod.rs
│   │   ├── types.rs               # Collection JSON types ✅
│   │   ├── installer.rs           # Collection installation orchestrator
│   │   └── fomod/
│   │       ├── mod.rs
│   │       ├── parser.rs          # XML parsing (UTF-16, BOM handling)
│   │       └── executor.rs        # Apply FOMOD choices
│   │
│   ├── file_router/               # Smart file placement ✅
│   │   ├── mod.rs
│   │   ├── mod_types.rs           # ModType enum
│   │   └── patterns.rs            # Detection patterns
│   │
│   ├── games/                     # Game type definitions ✅
│   │   └── mod.rs                 # GameType enum, validation
│   │
│   ├── mo2/                       # MO2 portable instance ✅
│   │   ├── mod.rs                 # Mo2Instance struct
│   │   ├── downloader.rs          # Download MO2 from GitHub
│   │   ├── stock_game.rs          # Stock Game folder creation
│   │   └── ini.rs                 # INI + profile generation
│   │
│   ├── loot/                      # Plugin sorting (future)
│   │   └── mod.rs                 # libloot crate integration
│   │
│   └── (existing modules)         # bsa, downloaders, installer, modlist, etc.
```

---

## Reference Code Locations

| Component | Reference | Location |
|-----------|-----------|----------|
| Collection JSON parsing | C++ | `/home/luke/Documents/Wabbajack Rust Update/Nexus-Collection-To-MO2-Bridge-main/src/nexus_bridge.cpp` |
| FOMOD installer | C++ | `/home/luke/Documents/Wabbajack Rust Update/Nexus-Collection-To-MO2-Bridge-main/src/fomod_installer.cpp` |
| modlist.txt sorting | C++ | `nexus_bridge.cpp:1583-2086` (ModListGenerator class) |
| plugins.txt / LOOT | C++ | `nexus_bridge.cpp:2089-2280` (PluginListGenerator class) |
| File routing logic | TypeScript | `/home/luke/Documents/Wabbajack Rust Update/Vortex-master/extensions/` |
| MO2 downloader | Rust | `/home/luke/Documents/NaK-main/src/installers/mo2.rs` |

---

## Phase 1: Foundation (Collection Support Core) ✅

### 1.1 File Router Module ✅
- [x] Create `src/file_router/mod.rs`
- [x] Implement `ModType` enum (Default, Root, BepInExRoot, BepInExPlugin)
- [x] Implement root file detection (dinput8.dll, dxgi.dll, etc.)
- [x] Implement data folder detection (Meshes/, Textures/, *.esp, etc.)
- [x] Add tests for mixed-content archives (like SKSE)

### 1.2 Game Type Module ✅
- [x] Create `src/games/mod.rs`
- [x] Define `GameType` enum with properties (name, steam_app_id, nexus_domain, etc.)
- [x] Implement `from_nexus_domain()` for auto-detection from collection JSON
- [x] Add game path validation
- [x] Document GameFinder for future auto-detection (see Future Phases)

### 1.3 Collection JSON Parser ✅
- [x] Create `src/collection/types.rs`
- [x] Define `Collection` struct (name, author, domain, mods, modRules, plugins)
- [x] Define `CollectionMod` struct (modId, fileId, logicalFilename, md5, choices, etc.)
- [x] Define `ModRule` struct (type, source, reference)
- [x] Define `PluginInfo` struct (name, enabled)
- [x] Define `FomodChoices` structs for installer choices
- [x] Implement JSON deserialization with serde
- [x] Add comprehensive unit tests

---

## Phase 2: MO2 Instance Creation ✅

### 2.1 MO2 Downloader ✅
- [x] Create `src/mo2/downloader.rs`
- [x] Find MO2 portable download URL (GitHub releases API)
- [x] Download MO2 portable 7z with progress callback
- [x] Extract to output directory using sevenz_rust
- [x] Verify extraction success (check ModOrganizer.exe exists)

### 2.2 Stock Game Creator ✅
- [x] Create `src/mo2/stock_game.rs`
- [x] Copy entire game folder to "Stock Game"
- [x] Handle large file copies with progress callback
- [x] Count files/bytes for progress tracking
- [x] Verify copy integrity

### 2.3 MO2 INI Configuration ✅
- [x] Create `src/mo2/ini.rs`
- [x] Generate ModOrganizer.ini with proper Qt format (double backslashes)
- [x] Set game path to Stock Game folder (Wine Z: format)
- [x] Configure custom executables (game + launcher)
- [x] Create default profile with plugins.txt/loadorder.txt
- [x] Mo2Instance struct for path management
- [x] **TESTED**: Full MO2 setup verified working

---

## Phase 3: Mod Installation Pipeline (CURRENT)

### 3.1 Collection Installer Orchestrator ✅
- [x] Create `src/collection/installer.rs`
- [x] Create `src/collection/db.rs` - SQLite database for tracking
- [x] Create `src/collection/verify.rs` - Archive verification with MD5 hashing
- [x] Create `src/collection/extract.rs` - Parallel extraction with file routing
- [x] Define `CollectionInstaller` struct with configuration
- [x] Implement installation phases:
  - [x] Phase A: Parse collection JSON, determine game type
  - [x] Phase B: Setup MO2 instance (download MO2, create Stock Game)
  - [x] Phase C: Download all mod archives (stub - needs Nexus integration)
  - [x] Phase D: Validate archives (size/hash check with auto-fix retry loop)
  - [x] Phase E: Extract and route files per mod (parallel, Wabbajack-style)
  - [x] Phase F: Process FOMOD installers with saved choices
  - [x] Phase G: Generate modlist.txt (mod load order)
  - [x] Phase H: Generate plugins.txt (plugin load order)
- [x] Progress reporting via callbacks
- [x] Resumable installation (track completed steps)
- [x] Auto-fix corrupted downloads (like Wabbajack) - delete & re-download up to 3 times

### 3.2 FOMOD Installer ✅
- [x] Create `src/collection/fomod/mod.rs`
- [x] Create `src/collection/fomod/encoding.rs`
  - [x] UTF-16 LE/BE BOM detection
  - [x] UTF-8 BOM detection
  - [x] Encoding conversion using encoding_rs
- [x] Create `src/collection/fomod/parser.rs`
  - [x] Parse ModuleConfig.xml
  - [x] Handle UTF-16 LE encoding
  - [x] Handle UTF-8 BOM
  - [x] Handle case-insensitive file lookups
  - [x] Parse install steps, groups, plugins, files
  - [x] Handle self-closing XML tags (Event::Empty)
- [x] Create `src/collection/fomod/executor.rs`
  - [x] Apply choices from collection JSON's `FomodChoices`
  - [x] Handle `<file>` installations (source -> destination)
  - [x] Handle `<folder>` installations
  - [x] Handle conditional flags with And/Or operators
  - [x] Case-insensitive path matching for archive contents
- [x] Preflight validation
  - [x] Validate FOMOD configs before extraction
  - [x] Store validation status in database
  - [x] Detailed error reporting with stack traces
  - [x] Option to continue on errors (continue_on_fomod_error)

### 3.3 File Extraction with Routing ✅
- [x] Create `src/collection/extract.rs` - extraction helper with FileRouter
- [x] Parallel archive indexing (Wabbajack-style with rayon + MultiProgress)
- [x] Parallel extraction (Wabbajack-style with rayon + MultiProgress)
- [x] For each file in archive:
  - [x] Determine ModType via FileRouter
  - [x] Route Root files → Stock Game folder
  - [x] Route Default files → mods/<mod_name>/ folder
  - [x] Route BepInEx files appropriately
- [x] Handle mods with mixed content (e.g., SKSE has root + data files)
- [x] **Hash-based FOMOD fallback**: For mods with FOMOD but no recorded choices:
  - [x] Use `hashes` array from collection JSON to determine expected files
  - [x] Build case-insensitive map of archive contents
  - [x] Match expected paths with suffix matching (handles FOMOD subfolders)
  - [x] Copy only matching files with expected structure
  - [x] Fall back to standard extraction if no matches
- [x] Case-insensitive folder merging during extraction (Windows compat)
- [x] Data folder flattening (move Data/ contents to root)
- [x] Wrapper folder detection and unwrapping (version folders, etc.)
- [ ] **TODO**: Support nested archives (archive inside archive) - deferred to later

---

## Phase 4: Load Order Generation

> **NOTE**: Based on C++ reference code in `Nexus-Collection-To-MO2-Bridge-main/src/nexus_bridge.cpp`
> - PluginListGenerator: lines 2092-2246
> - ModListGenerator: lines 1586-2086
> These contain custom algorithms we developed for proper sorting.

### 4.1 Plugin Sorting (plugins.txt) ✅ DONE

**Status**: Implemented in `src/loot/mod.rs` and integrated into `phase_generate_plugins()`

- [x] Create `src/loot/mod.rs` with PluginSorter struct
- [x] Add `libloot = "0.28.4"` to Cargo.toml
- [x] Initialize libloot Game handle for Skyrim SE
- [x] `find_local_app_data()` - Steam/Proton prefix detection for local app data
  - Standard Steam: `~/.steam/steam/steamapps/compatdata/489830/pfx/...`
  - Alternative: `~/.local/share/Steam/steamapps/compatdata/489830/pfx/...`
  - Flatpak: `~/.var/app/com.valvesoftware.Steam/.local/share/Steam/...`
- [x] `set_mod_paths()` - Set additional data paths (MO2 virtual filesystem simulation)
- [x] `load_plugins()` - Find and load plugin headers
  - Search mods directory first (MO2 priority - first match wins)
  - Fall back to game Data folder
  - Skip already-found plugins (deduplication)
- [x] `sort_plugins()` - Call libloot sorting
- [x] `sort_all()` - Convenience method combining all steps
- [x] Integration: `sort_plugins_with_loot()` in installer.rs with fallback to collection order
- [x] Write plugins.txt with `*plugin.esp` format (enabled)
- [x] Write loadorder.txt for MO2 compatibility
- [x] Base game plugins always first (Skyrim.esm, Update.esm, DLCs)
- [x] Unit tests (4 tests passing)

### 4.2 Mod Sorting (modlist.txt) ✅ COMPLETE

**Status**: Implemented full ensemble sorting matching C++ reference code
**Implementation**: `src/collection/modlist.rs`

**Reference**: C++ ModListGenerator class (nexus_bridge.cpp:1586-2086)

#### 4.2.1 Core Topological Sort ✅
- [x] Create `ModListGenerator` struct in `src/collection/modlist.rs`
- [x] Build lookup maps:
  - `logicalNameToIdx`: logicalFilename -> mod index (rules use logicalFileName)
  - `md5ToLogicalName`: md5 -> logicalFilename (for MD5-based rule lookups)
  - `modFolders`: index -> folderName (output uses folderName)
- [x] Process modRules to build constraint graph:
  - "before": source must come before reference (edge: source -> reference)
  - "after": source must come after reference (edge: reference -> source)
  - NOTE: Rule lookup by logicalFileName OR fileMD5 (MD5 is fallback)
- [x] Build adjacency lists using petgraph crate

#### 4.2.2 DFS-Based Sort ✅
- [x] Implement iterative DFS to avoid stack overflow on large graphs
- [x] Start from SINKS (nodes with no successors)
- [x] Walk backwards via predecessors, add in post-order
- [x] Track visited states: 0=unvisited, 1=in-progress, 2=done
- [x] Detect cycles (when we hit an in-progress node)
- [x] Sort sinks alphabetically for deterministic tie-breaking
- [x] Handle disconnected components (remaining unvisited nodes)
- [x] Reverse output (MO2: Top = Winner, so sinks/highest priority at top)

#### 4.2.3 Kahn's Algorithm with Tie-Breaking ✅
- [x] `kahn_sort()` function with priority queue (BinaryHeap)
- [x] In-degree tracking for each node
- [x] Priority queue sorted by tie-breaker (lower value = earlier in output)
- [x] Process nodes when in-degree reaches 0
- [x] Handle cycles gracefully (add remaining nodes sorted by tie-breaker)

#### 4.2.4 Plugin Position Integration ✅
- [x] `build_plugin_position_map()`: sorted plugin name -> position
- [x] `get_mod_plugin_position()`: find earliest plugin position for a mod folder
  - Scan mod folder for .esp/.esm/.esl files
  - Return minimum position from LOOT-sorted order
  - Return usize::MAX if mod has no plugins

#### 4.2.5 Ensemble Sorting ✅
> **Implemented**: Full 4-method ensemble matching C++ code
> 1. DFS Sort (weight 2.0) - respects before/after via depth-first
> 2. Kahn's Algorithm (weight 2.0) - topological with plugin tie-breaking
> 3. Plugin Order (weight 1.5) - sort purely by plugin position
> 4. Collection Order (weight 0.5) - original order as baseline
>
> Combined score = weighted average of ranks from each method
> Final sort uses Kahn's with combined rank as tie-breaker

- [x] `generate_mod_order_combined()` with full ensemble
- [x] Weight constants: DFS=2.0, Kahn=2.0, Plugin=1.5, Collection=0.5
- [x] Logging for diagnostics

#### 4.2.6 Integration ✅
- [x] `phase_generate_modlist()` uses ensemble sorting
- [x] Write modlist.txt with `+ModName` format
- [x] Top = Winner, Bottom = Loser format
- [x] Unit tests for sorting algorithms (3 tests)

#### 4.2.7 Edge Cases Handled ✅
- [x] Empty rules list (just use phase/alphabetical order)
- [x] Rules referencing non-existent mods (skip silently)
- [x] Cycles in rules (warn but continue)
- [x] Mods without plugins (use alphabetical for tie-breaking)
- [x] Filter by folder existence (handles status update bugs)

---

## Phase 5: CLI Integration ✅

### 5.1 Collection Command ✅
- [x] Add `collection` subcommand to CLI
- [x] Arguments: collection JSON path OR Nexus URL, output dir, game dir, nexus key
- [x] URL support: `https://www.nexusmods.com/<game>/collections/<slug>`
  - [x] Create `src/collection/fetch.rs` - URL parsing and fetching
  - [x] Parse collection URL to extract game domain and slug
  - [x] Query Nexus GraphQL API for collection revision and download link
  - [x] Download and extract collection.7z to get collection.json
  - [x] Auto-detect URL vs file path input
- [x] Concurrency based on CPU threads (no --concurrent flag)
- [x] Downloads default to output/downloads (always protected)
- [x] Smart checking: only download missing/corrupted files (like Wabbajack)
- [x] Clean MO2 setup with user warning on existing installation
- [x] Interactive FOMOD error handling (prompt to skip or abort)
- [x] Add `collection-info` command to show collection details (also supports URL)
- [x] Progress reporting with indicatif (multi-progress bars)

### 5.2 Download Infrastructure ✅
- [x] Created `src/collection/download.rs` with parallel downloads
- [x] Moved `downloaders` module to lib.rs for shared access
- [x] Rate limit handling with exponential backoff
- [x] Size verification after download
- [x] Auto-retry on failures (3 attempts)
- [x] Failed download reporting with manual download URLs
- [ ] NXM browser mode (TODO - stub in place)

### 5.3 Shared Infrastructure (Future - Windows Support)
- [ ] Refactor downloaders into shared orchestrator
- [ ] Shared progress tracking abstraction
- [ ] Cross-platform path handling

---

## Phase 6: Polish & Testing

### 6.1 Testing
- [x] Unit tests for file router
- [x] Unit tests for collection parser
- [x] Unit tests for MO2 module
- [x] Unit tests for FOMOD parser (24 tests)
- [ ] Integration test with sample collection
- [ ] Test with real Nexus collection

### 6.2 Error Handling
- [ ] Graceful handling of missing files
- [ ] Clear error messages
- [ ] Recovery from partial installs
- [ ] Logging with tracing

---

## Future Phases (Not in current scope)

### Phase 7: Game Auto-Detection with GameFinder

For cross-platform game detection, integrate [GameFinder](https://github.com/erri120/GameFinder) by erri120 (Wabbajack creator).

**Why GameFinder:**
- Supports Windows + Linux (with Wine/Proton path remapping)
- Detects games from: Steam, GOG, Epic, Origin, EA Desktop, Xbox Game Pass
- Battle-tested (used by Wabbajack itself)
- Returns game path, store ID, and more

**Integration approach:**
- Create small .NET CLI tool using GameFinder
- Compile with .NET Native AOT for single binary
- Call from Rust via subprocess, parse JSON output
- Example: `gamefinder-cli find --game "skyrimspecialedition" --json`

This would remove the need for the `--game` CLI flag entirely.

### Phase 8: Additional Games
- [ ] Fallout 4
- [ ] Skyrim VR
- [ ] Other Bethesda games

### Phase 9: Windows Support
- [ ] Cross-platform path handling
- [ ] Windows game detection (via GameFinder)
- [ ] Windows-specific paths

### Phase 10: GUI/TUI
- [ ] Terminal UI with ratatui
- [ ] Progress visualization
- [ ] Interactive FOMOD selection

---

## Key Dependencies

```toml
# Already added
quick-xml = "0.39"          # FOMOD XML parsing
encoding_rs = "0.8.35"      # UTF-16/encoding handling

# To add in Phase 4
libloot = "0.28.4"          # Plugin sorting
```

---

## File Routing Reference

### Root Files (go to Stock Game/)
```
dinput8.dll, dxgi.dll, d3d11.dll, d3d9.dll
binkw64.dll, bink2w64.dll
version.dll, winmm.dll, winhttp.dll
enbseries.ini, enblocal.ini
skse64_loader.exe, skse64_*.dll
f4se_loader.exe, f4se_*.dll
BepInEx/ folder
```

### Data Files (go to mods/<name>/)
```
*.esp, *.esm, *.esl          # Plugins
*.bsa, *.ba2                  # Archives
textures/, meshes/, music/    # Asset folders
shaders/, video/, interface/
fonts/, scripts/, facegen/
menus/, lodsettings/, sound/
strings/, trees/, seq/, grass/
SKSE/Plugins/                 # SKSE plugins (Data folder)
```

---

## Notes & Reminders

- **modlist.txt sorting**: Will tweak algorithm when we get there (noted by user)
- **Skyrim SE only**: Focus on Steam version first
- **libloot**: Native Rust crate, no FFI needed
- **Stock Game**: Copy entire game to keep original clean
- **MO2 Portable**: Download from GitHub releases
- **GameFinder**: Use for auto-detection in future (Phase 7)
- **Qt INI format**: Use double backslashes for paths in @ByteArray()

---

## Progress Tracking

**Current Status**: Phase 6 - Polish & Testing

**Last Updated**: 2026-01-29

| Phase | Status | Notes |
|-------|--------|-------|
| Phase 1 | ✅ Complete | Foundation - File Router, Game Types, Parser |
| Phase 2 | ✅ Complete | MO2 Instance Creation - Tested & Working |
| Phase 3 | ✅ Complete | Mod Installation Pipeline + Hash-based FOMOD fallback |
| Phase 4.1 | ✅ Complete | Plugin Sorting with libloot (4 tests) |
| Phase 4.2 | ✅ Complete | Mod Sorting - Full ensemble (DFS, Kahn, Plugin, Collection) |
| Phase 5 | ✅ Complete | CLI Integration - `collection` and `collection-info` commands |
| Phase 6 | In Progress | Polish & Testing (125 tests passing) |
