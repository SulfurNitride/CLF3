# Progress Reporting Upgrade Plan

## Summary

CLF3 should treat CLI progress as the primary user experience and expose the same progress state to tools through machine-readable output. The current implementation already has `indicatif` and a `ProgressReporter` boundary, but the API is too generic and progress is duplicated across CLI handles, legacy callbacks, archive events, status bars, and logs.

This plan replaces the current progress trait with a structured task/event model, keeps `indicatif` as the interactive CLI renderer, removes the legacy GUI callback path, and adds JSON/snapshot modes for external tooling.

## Goals

- Show every active worker in the CLI by default, including all active download/extract/archive tasks up to the configured worker pool.
- Expose all active tasks to external tools without requiring terminal-output scraping.
- Make progress data typed and stage-aware: download, verify, extract, finalize, DDS, BSA, cleanup.
- Report exact byte speed where available, especially HTTP-style downloads.
- Report honest extraction progress: stage, active archive, known counts, elapsed time, and final throughput; do not fake live byte progress for external `7z`.
- Remove the old `progress_callback` / `ProgressEvent` path from `InstallConfig` and download code.

## Non-Goals For V1

- Do not replace `indicatif`.
- Do not parse live `7z` process output in this pass.
- Do not promise exact live byte progress for external `7z` extraction.
- Do not rebuild the GUI progress system; GUI is not the primary target for this pass.

## User-Facing CLI Modes

Add an install progress option:

```text
--progress auto|full|plain|json|snapshot
```

Default:

- `auto`: use `full` when attached to an interactive terminal; use `plain` otherwise.
- `full`: interactive `indicatif` display with aggregate phase rows plus every active worker row.
- `plain`: readable line-oriented progress without terminal redraws.
- `json`: newline-delimited JSON event stream on stdout.
- `snapshot`: newline-delimited JSON snapshots on stdout every 250ms.

Stream policy:

- In `json` and `snapshot` modes, stdout must contain only machine-readable JSON.
- Human logs in machine-readable modes go to stderr and the existing log file.
- Interactive progress and normal human summaries remain human-readable.

## Target Full CLI Shape

Example with 16 active extraction workers:

```text
=== Installing ===
Overall   54/128 archives | 42% | 22m elapsed
Download  71/128 archives | 4 active  | 38.6 MiB/s
Extract   54/128 archives | 16 active | 612 MiB/s effective
DDS       1,840/4,920 textures | 3 active
BSA       8/31 archives | 1 active

Active workers
[01] Skyland AIO.7z              extract    812/2400 files | 18m elapsed
[02] Legacy of the Dragonborn.7z extract    201/1800 files | 7m elapsed
[03] Skyrim 202X.7z              finalize   1900 files | 4.1 GiB archive
[04] Some Mod.7z                 download   620 MiB/1.9 GiB | 91 MiB/s
...
[16] Folkvangr.7z                extract    88/970 files | 3m elapsed
```

Notes:

- Download rows use exact byte position and speed when the downloader provides callbacks.
- Extraction rows use known directive/file counts and stage transitions.
- External `7z` rows show stage/elapsed/counts until the process completes, then final effective throughput.

## Core Types

Replace the current generic reporter API with typed task/event primitives.

```rust
pub enum ProgressMode {
    Auto,
    Full,
    Plain,
    Json,
    Snapshot,
}

pub enum TaskKind {
    Download,
    Verify,
    Extract,
    Finalize,
    Dds,
    Bsa,
    Cleanup,
}

pub enum TaskStage {
    Queued,
    Resolving,
    Downloading,
    Verifying,
    Extracting,
    Finalizing,
    Transforming,
    Building,
    Waiting,
    Complete,
    Failed,
    Manual,
}

pub enum ProgressUnit {
    Bytes,
    Files,
    Archives,
    Textures,
    Directives,
    Items,
}

pub struct TaskId(String);

pub struct TaskStarted {
    pub id: TaskId,
    pub parent: Option<TaskId>,
    pub kind: TaskKind,
    pub stage: TaskStage,
    pub label: String,
    pub unit: ProgressUnit,
    pub total: Option<u64>,
}

pub struct TaskUpdate {
    pub id: TaskId,
    pub stage: Option<TaskStage>,
    pub position: Option<u64>,
    pub total: Option<u64>,
    pub bytes_per_sec: Option<f64>,
    pub files_done: Option<u64>,
    pub files_total: Option<u64>,
    pub message: Option<String>,
}

pub enum TaskOutcome {
    Success,
    Failed { error: String },
    Manual,
    Skipped,
}

pub enum ProgressEvent {
    PhaseStarted { phase: Phase },
    TaskStarted(TaskStarted),
    TaskUpdate(TaskUpdate),
    TaskFinished { id: TaskId, outcome: TaskOutcome },
    Snapshot(ProgressSnapshot),
    Log { level: ProgressLogLevel, message: String },
}
```

Renderer state should maintain a map of active task snapshots:

```rust
HashMap<TaskId, TaskSnapshot>
```

Tasks remain in the active map from `TaskStarted` until `TaskFinished`. Snapshot mode serializes the current state every 250ms.

## JSON Output Contracts

`--progress=json` emits events:

```json
{"type":"task_started","id":"extract:abc","kind":"extract","stage":"extracting","label":"Skyland AIO.7z","unit":"files","total":2400}
{"type":"task_update","id":"extract:abc","stage":"extracting","files_done":812,"files_total":2400,"elapsed_ms":1080000}
{"type":"task_finished","id":"extract:abc","outcome":"success","files_done":2400,"bytes_total":3221225472,"effective_bytes_per_sec":141200000}
```

`--progress=snapshot` emits full active-state snapshots:

```json
{"type":"snapshot","active":[{"id":"extract:abc","kind":"extract","stage":"extracting","label":"Skyland AIO.7z","files_done":812,"files_total":2400},{"id":"download:def","kind":"download","stage":"downloading","label":"Some Mod.7z","bytes_done":650117120,"bytes_total":2040109465,"bytes_per_sec":95420416}]}
```

Use `serde` serialization for JSON rather than manual formatting.

## Implementation Phases

### Phase 1: Progress API Replacement

- Replace `ProgressReporter` / `ProgressHandle` with typed task/event APIs.
- Add `ProgressMode` and CLI parsing for `--progress`.
- Remove `progress_callback` and legacy `ProgressEvent` from `InstallConfig`.
- Keep a `NullReporter` for tests/headless paths.
- Ensure all reporter implementations are `Send + Sync`.

### Phase 2: Renderers

- Rework `CliReporter` into a full interactive renderer backed by `indicatif::MultiProgress`.
- Add a plain renderer for non-interactive logs.
- Add JSON event renderer writing clean NDJSON to stdout.
- Add snapshot renderer that stores task state and emits every 250ms.
- Route tracing/log writer through the active renderer so machine-readable stdout stays clean.

### Phase 3: Download Instrumentation

- Convert downloader task creation to `TaskKind::Download`.
- Use exact bytes, totals, and speed from existing HTTP progress callbacks.
- Add stages for resolving URL, downloading, verifying, retrying, proxy/mirror fallback, manual, failed, and complete.
- Update Wabbajack CDN, Mega, Google Drive, MediaFire, Yandex, LoversLab, and GameFileSource paths to emit at least start/stage/finish events.
- Report final byte totals even for downloaders that do not currently provide continuous speed.

### Phase 4: Extraction Pipeline Instrumentation

- Emit one active extract task per prepared archive before spawning its worker thread.
- Include archive name, archive hash-derived task id, archive size, directive count, and known texture count.
- Emit stage updates for prepare, extract, finalize, DDS transform, BSA readiness/build, complete, and failed.
- For direct ZIP/RAR/BSA callback paths, increment exact file/directive counters where callbacks already exist.
- For external `7z`, show stage/elapsed/known counts during execution, then final counts and effective throughput on completion.

### Phase 5: Cleanup And Deduplication

- Remove duplicated user-facing completion lines that are now represented by task finish events.
- Replace installer/download `println!`/`eprintln!` in progress-sensitive paths with structured log or progress events.
- Keep final install/download summaries as human-readable logs in interactive/plain modes.
- In JSON/snapshot modes, summaries should be emitted as structured events or logs on stderr, not mixed into stdout.

## Key Files

- `src/installer/progress.rs`: new event/task model and reporter trait.
- `src/installer/progress_cli.rs`: interactive, plain, JSON, and snapshot renderer implementations.
- `src/main.rs`: `--progress` option, reporter construction, stream routing.
- `src/installer/config.rs`: remove legacy callback fields and types.
- `src/installer/downloader.rs`: download task instrumentation.
- `src/installer/pipeline.rs`: active archive/extract/DDS/BSA task instrumentation.
- `src/installer/streaming.rs`: exact counter hooks for direct/native extraction paths.
- `src/archive/sevenzip.rs`: no live 7z parsing in v1; optionally accept future progress hooks later.

## Testing Plan

- Unit-test task state transitions: start, update, finish, failure, skipped/manual.
- Unit-test JSON serialization for all event variants.
- Unit-test snapshot renderer emits valid NDJSON and includes all active tasks.
- Unit-test that finished tasks are removed from active snapshots.
- Integration-check `--progress=json` produces only JSON on stdout.
- Integration-check `--progress=snapshot` cadence is approximately 250ms and output remains valid JSON lines.
- Run `cargo test --lib`.
- Run `cargo build`.
- Manually smoke-test one small install in `--progress=full`, `--progress=plain`, and `--progress=json` when a small modlist/test fixture is available.

## Acceptance Criteria

- Full CLI mode shows aggregate status plus all active workers up to the configured pool size.
- Tools can observe every active worker through JSON events or snapshots.
- JSON/snapshot stdout is parseable without filtering human logs.
- Download speed remains exact for HTTP-style downloads.
- Extraction progress is honest: no fake live 7z byte progress.
- Legacy `progress_callback` and old `ProgressEvent` are removed from installer config and downloader code.
- Existing install behavior is unchanged apart from progress/log presentation.
