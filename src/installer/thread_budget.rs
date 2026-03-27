//! Global thread budget for 7z extraction processes.
//!
//! The 7z binary benefits massively from multi-threading (91→454 MB/s).
//! But when multiple archives extract concurrently, each spawning all-core
//! 7z processes causes CPU oversubscription.
//!
//! Solution: pre-divide cores by max concurrent archives.
//! With 16 cores and 16 max_concurrent, each 7z gets 1 thread.
//! But that's too low — LZMA needs multiple threads to be fast.
//!
//! So we cap max_concurrent 7z processes separately:
//! - threads_per_7z = total_cores / max_7z_concurrent
//! - max_7z_concurrent = total_cores / threads_per_7z
//!
//! Default: 4 threads per 7z, so max 4 concurrent 7z on 16 cores.

use std::sync::atomic::{AtomicUsize, Ordering};

static THREADS_PER_7Z: AtomicUsize = AtomicUsize::new(4);

/// Initialize the thread budget.
/// `total_cores`: available CPU cores.
/// `max_concurrent`: how many archives can extract simultaneously.
///
/// Computes a fixed thread count per 7z process such that all concurrent
/// 7z processes together don't exceed total cores. Minimum 2 threads.
pub fn init(total_cores: usize, max_concurrent: usize) {
    // Give each 7z process a fair share, minimum 2
    let per_7z = (total_cores / max_concurrent.max(1)).max(2);
    THREADS_PER_7Z.store(per_7z, Ordering::Relaxed);
}

/// Get the fixed thread count for 7z processes.
/// This is constant for the lifetime of the install — no claim/release needed.
pub fn threads_per_7z() -> usize {
    THREADS_PER_7Z.load(Ordering::Relaxed).max(2)
}
