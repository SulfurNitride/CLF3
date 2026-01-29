//! Mod list generator for MO2 modlist.txt
//!
//! Implements ensemble sorting combining multiple methods:
//! 1. DFS topological sort (from sinks, respects before/after)
//! 2. Kahn's algorithm (topological sort with plugin tie-breaking)
//! 3. Plugin order (LOOT-sorted plugin positions)
//! 4. Collection order (original order from collection JSON)
//!
//! This matches the Vortex/NexusBridge approach since Vortex doesn't
//! export a modlist.txt and uses black magic voodoo internally.

use std::collections::{BinaryHeap, HashMap, HashSet};
use std::fs::File;
use std::io::Write;
use std::path::Path;

use anyhow::{Context, Result};
use tracing::info;

/// Mod rule from collection JSON
#[derive(Debug, Clone)]
pub struct ModRule {
    /// Rule type: "before" or "after"
    pub rule_type: String,
    /// Source mod logical filename
    pub source_logical_name: String,
    /// Source mod MD5
    pub source_md5: String,
    /// Reference mod logical filename
    pub reference_logical_name: String,
    /// Reference mod MD5
    pub reference_md5: String,
}

/// Mod info for sorting
#[derive(Debug, Clone)]
pub struct ModInfo {
    pub name: String,
    pub logical_filename: String,
    pub folder_name: String,
    pub md5: String,
}

/// Weights for ensemble sorting (matches C code)
const WEIGHT_DFS: f64 = 2.0;
const WEIGHT_KAHN: f64 = 2.0;
const WEIGHT_PLUGIN: f64 = 1.5;
const WEIGHT_COLLECTION: f64 = 0.5;

/// Mod list generator using ensemble sorting
pub struct ModListGenerator;

impl ModListGenerator {
    /// Generate mod order using ensemble sorting (matches C code approach)
    ///
    /// Combines 4 sorting methods:
    /// 1. DFS sort - depth-first from sinks, respects constraints
    /// 2. Kahn's algorithm - topological sort with plugin tie-breaking
    /// 3. Plugin order - sort by LOOT-sorted plugin positions
    /// 4. Collection order - original order from collection JSON
    ///
    /// Returns folder names in priority order (first = highest priority = top of modlist)
    pub fn generate_mod_order_combined(
        mods: &[ModInfo],
        rules: &[ModRule],
        sorted_plugins: &[String],
        mods_dir: &Path,
    ) -> Vec<String> {
        let n = mods.len();
        if n == 0 {
            return Vec::new();
        }

        // Build lookup maps
        let mut logical_name_to_idx: HashMap<String, usize> = HashMap::new();
        let mut md5_to_logical_name: HashMap<String, String> = HashMap::new();
        let mut mod_folders: Vec<String> = Vec::with_capacity(n);

        for (i, m) in mods.iter().enumerate() {
            let key = if m.logical_filename.is_empty() {
                m.name.clone()
            } else {
                m.logical_filename.clone()
            };
            logical_name_to_idx.insert(key.clone(), i);

            let folder = if m.folder_name.is_empty() {
                m.name.clone()
            } else {
                m.folder_name.clone()
            };
            mod_folders.push(folder);

            if !m.md5.is_empty() {
                md5_to_logical_name.insert(m.md5.clone(), key);
            }
        }

        // Build plugin position map
        let plugin_position = Self::build_plugin_position_map(sorted_plugins);

        // Pre-compute plugin positions for each mod
        let mut mod_plugin_pos: Vec<i32> = Vec::with_capacity(n);
        let mut mods_with_plugins = 0;
        for folder in &mod_folders {
            let pos = Self::get_mod_plugin_position(folder, mods_dir, &plugin_position);
            if pos < i32::MAX {
                mods_with_plugins += 1;
            }
            mod_plugin_pos.push(pos);
        }
        // Build adjacency lists for constraints
        let mut successors: Vec<Vec<usize>> = vec![Vec::new(); n];
        let mut predecessors: Vec<Vec<usize>> = vec![Vec::new(); n];

        let mut applied_rules = 0;
        for rule in rules {
            // Find source mod by logical name or MD5
            let src_key = if !rule.source_logical_name.is_empty() {
                rule.source_logical_name.clone()
            } else if !rule.source_md5.is_empty() {
                md5_to_logical_name
                    .get(&rule.source_md5)
                    .cloned()
                    .unwrap_or_default()
            } else {
                String::new()
            };

            // Find reference mod by logical name or MD5
            let ref_key = if !rule.reference_logical_name.is_empty() {
                rule.reference_logical_name.clone()
            } else if !rule.reference_md5.is_empty() {
                md5_to_logical_name
                    .get(&rule.reference_md5)
                    .cloned()
                    .unwrap_or_default()
            } else {
                String::new()
            };

            // Skip if we can't find either mod
            let src_idx = match logical_name_to_idx.get(&src_key) {
                Some(&idx) => idx,
                None => continue,
            };
            let ref_idx = match logical_name_to_idx.get(&ref_key) {
                Some(&idx) => idx,
                None => continue,
            };

            match rule.rule_type.as_str() {
                "before" => {
                    // source before reference: source has lower priority
                    successors[src_idx].push(ref_idx);
                    predecessors[ref_idx].push(src_idx);
                    applied_rules += 1;
                }
                "after" => {
                    // source after reference: source has higher priority
                    successors[ref_idx].push(src_idx);
                    predecessors[src_idx].push(ref_idx);
                    applied_rules += 1;
                }
                _ => {}
            }
        }

        info!(
            "Applied {} mod rules for sorting, {}/{} mods have plugins",
            applied_rules, mods_with_plugins, n
        );

        // =========================================================================
        // Method 1: DFS Sort
        // =========================================================================
        let dfs_order = Self::dfs_topological_sort(&mod_folders, &successors, &predecessors);
        let mut dfs_position: HashMap<String, usize> = HashMap::new();
        for (i, folder) in dfs_order.iter().enumerate() {
            dfs_position.insert(folder.clone(), i);
        }
        let dfs_rank: Vec<usize> = mod_folders
            .iter()
            .enumerate()
            .map(|(i, f)| *dfs_position.get(f).unwrap_or(&i))
            .collect();

        // =========================================================================
        // Method 2: Kahn's Algorithm with plugin tie-breaking
        // =========================================================================
        let kahn_indices = Self::kahn_sort(n, &successors, &predecessors, &mod_plugin_pos);
        let mut kahn_rank: Vec<usize> = vec![0; n];
        for (i, &idx) in kahn_indices.iter().enumerate() {
            kahn_rank[idx] = i;
        }

        // =========================================================================
        // Method 3: Plugin Order (sort purely by plugin position)
        // Use (plugin_pos, index) to simulate stable sort like C++ std::stable_sort
        // This preserves collection order for mods with same plugin position
        // =========================================================================
        let mut plugin_indices: Vec<usize> = (0..n).collect();
        // Sort by (plugin_pos, original_index) for stable-like behavior
        plugin_indices.sort_by_key(|&i| (mod_plugin_pos[i], i));
        let mut plugin_rank: Vec<usize> = vec![0; n];
        for (i, &idx) in plugin_indices.iter().enumerate() {
            plugin_rank[idx] = i;
        }

        // =========================================================================
        // Method 4: Collection Order (original order)
        // =========================================================================
        let collection_rank: Vec<usize> = (0..n).collect();

        // =========================================================================
        // Combine votes: weighted average of ranks
        // =========================================================================
        let total_weight = WEIGHT_DFS + WEIGHT_KAHN + WEIGHT_PLUGIN + WEIGHT_COLLECTION;

        let combined_score: Vec<f64> = (0..n)
            .map(|i| {
                (WEIGHT_DFS * dfs_rank[i] as f64
                    + WEIGHT_KAHN * kahn_rank[i] as f64
                    + WEIGHT_PLUGIN * plugin_rank[i] as f64
                    + WEIGHT_COLLECTION * collection_rank[i] as f64)
                    / total_weight
            })
            .collect();

        // =========================================================================
        // Final sort: Kahn's with combined score as tie-breaker
        // =========================================================================
        // Convert combined score to integer ranks
        // Use stable-like sort: (score, index) for deterministic ordering
        let mut sorted_by_score: Vec<usize> = (0..n).collect();
        sorted_by_score.sort_by(|&a, &b| {
            combined_score[a]
                .partial_cmp(&combined_score[b])
                .unwrap_or(std::cmp::Ordering::Equal)
                .then_with(|| a.cmp(&b)) // Use index as tie-breaker for stability
        });
        let mut combined_rank: Vec<i32> = vec![0; n];
        for (i, &idx) in sorted_by_score.iter().enumerate() {
            combined_rank[idx] = i as i32;
        }

        // Run Kahn's with combined rank as tie-breaker
        let final_indices = Self::kahn_sort(n, &successors, &predecessors, &combined_rank);

        // Count violations
        let mut final_position: HashMap<usize, usize> = HashMap::new();
        for (i, &idx) in final_indices.iter().enumerate() {
            final_position.insert(idx, i);
        }

        let mut violations = 0;
        for i in 0..n {
            for &pred in &predecessors[i] {
                if final_position.get(&pred).unwrap_or(&0) > final_position.get(&i).unwrap_or(&0) {
                    violations += 1;
                }
            }
        }

        if violations > 0 {
            tracing::warn!(
                "{} constraint violations (cycles in mod rules)",
                violations
            );
        }

        info!("Ensemble sorting complete (DFS + Kahn + Plugin + Collection)");

        // Build result - MO2: Top = Winner (mods higher in list override mods lower in list)
        // Kahn's algorithm produces sources first (lowest priority)
        // We need to reverse so highest priority (sinks) are at the top of modlist.txt
        let mut result: Vec<String> = final_indices
            .into_iter()
            .map(|i| mod_folders[i].clone())
            .collect();
        result.reverse();
        result
    }

    /// DFS-based topological sort (matches Vortex's graphlib.alg.topsort behavior)
    /// Starts from sinks and walks backwards via predecessors
    fn dfs_topological_sort(
        mod_folders: &[String],
        successors: &[Vec<usize>],
        predecessors: &[Vec<usize>],
    ) -> Vec<String> {
        let n = mod_folders.len();
        let mut visited: Vec<u8> = vec![0; n]; // 0=unvisited, 1=in-progress, 2=done
        let mut sorted: Vec<String> = Vec::with_capacity(n);
        let mut has_cycle = false;

        // Find sinks (nodes with no successors)
        let mut sinks: Vec<usize> = (0..n).filter(|&i| successors[i].is_empty()).collect();

        // Sort sinks alphabetically for deterministic tie-breaking
        sinks.sort_by(|&a, &b| mod_folders[a].cmp(&mod_folders[b]));

        // Iterative DFS to avoid stack overflow
        let visit = |start: usize,
                         visited: &mut Vec<u8>,
                         sorted: &mut Vec<String>,
                         has_cycle: &mut bool| {
            let mut stack: Vec<(usize, usize)> = vec![(start, 0)]; // (node, pred_idx)

            while let Some((node, mut pred_idx)) = stack.pop() {
                if pred_idx == 0 {
                    // First time visiting this node
                    if visited[node] == 2 {
                        continue;
                    }
                    if visited[node] == 1 {
                        *has_cycle = true;
                        continue;
                    }
                    visited[node] = 1;
                }

                // Try to visit unvisited predecessors
                let mut pushed_pred = false;
                while pred_idx < predecessors[node].len() {
                    let pred = predecessors[node][pred_idx];
                    pred_idx += 1;
                    if visited[pred] == 0 {
                        stack.push((node, pred_idx));
                        stack.push((pred, 0));
                        pushed_pred = true;
                        break;
                    } else if visited[pred] == 1 {
                        *has_cycle = true;
                    }
                }

                if !pushed_pred && pred_idx >= predecessors[node].len() {
                    visited[node] = 2;
                    sorted.push(mod_folders[node].clone());
                }
            }
        };

        // Process sinks
        for sink in sinks {
            if visited[sink] == 0 {
                visit(sink, &mut visited, &mut sorted, &mut has_cycle);
            }
        }

        // Visit remaining unvisited nodes (handles disconnected components)
        let mut remaining: Vec<usize> = (0..n).filter(|&i| visited[i] == 0).collect();
        remaining.sort_by(|&a, &b| mod_folders[a].cmp(&mod_folders[b]));
        for node in remaining {
            if visited[node] == 0 {
                visit(node, &mut visited, &mut sorted, &mut has_cycle);
            }
        }

        if has_cycle {
            tracing::warn!("Cycle detected in mod rules, some mods may be misordered");
        }

        // Reverse: predecessors added before dependents, MO2 wants top=winner
        sorted.reverse();
        sorted
    }

    /// Kahn's algorithm for topological sort with tie-breaking
    /// Lower tie_breaker value = earlier in output
    fn kahn_sort<T: Ord + Copy>(
        n: usize,
        successors: &[Vec<usize>],
        predecessors: &[Vec<usize>],
        tie_breaker: &[T],
    ) -> Vec<usize> {
        let mut in_degree: Vec<usize> = predecessors.iter().map(|p| p.len()).collect();

        // Priority queue: lower tie_breaker = higher priority (processed first)
        // BinaryHeap is max-heap, so we use Reverse
        // When priorities are equal, use index as secondary sort (lower index first)
        #[derive(Eq, PartialEq)]
        struct Node<T: Ord> {
            priority: T,
            index: usize,
        }
        impl<T: Ord> Ord for Node<T> {
            fn cmp(&self, other: &Self) -> std::cmp::Ordering {
                // Reverse ordering: lower priority value = higher heap priority
                // Use index as tie-breaker for deterministic ordering
                other
                    .priority
                    .cmp(&self.priority)
                    .then_with(|| other.index.cmp(&self.index))
            }
        }
        impl<T: Ord> PartialOrd for Node<T> {
            fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
                Some(self.cmp(other))
            }
        }

        let mut ready: BinaryHeap<Node<T>> = BinaryHeap::new();

        for i in 0..n {
            if in_degree[i] == 0 {
                ready.push(Node {
                    priority: tie_breaker[i],
                    index: i,
                });
            }
        }

        let mut result: Vec<usize> = Vec::with_capacity(n);

        while let Some(Node { index: node, .. }) = ready.pop() {
            result.push(node);

            for &succ in &successors[node] {
                in_degree[succ] -= 1;
                if in_degree[succ] == 0 {
                    ready.push(Node {
                        priority: tie_breaker[succ],
                        index: succ,
                    });
                }
            }
        }

        // If we didn't process all nodes, there's a cycle - add remaining
        if result.len() < n {
            let added: HashSet<usize> = result.iter().copied().collect();
            let mut remaining: Vec<usize> = (0..n).filter(|i| !added.contains(i)).collect();
            remaining.sort_by_key(|&i| &tie_breaker[i]);
            result.extend(remaining);
        }

        result
    }

    /// Build plugin position map from LOOT-sorted plugins
    fn build_plugin_position_map(sorted_plugins: &[String]) -> HashMap<String, i32> {
        let mut map = HashMap::new();
        for (i, plugin) in sorted_plugins.iter().enumerate() {
            map.insert(plugin.to_lowercase(), i as i32);
        }
        map
    }

    /// Get the earliest plugin position for a mod folder
    fn get_mod_plugin_position(
        mod_folder: &str,
        mods_dir: &Path,
        plugin_position: &HashMap<String, i32>,
    ) -> i32 {
        let mod_path = mods_dir.join(mod_folder);
        if !mod_path.exists() {
            return i32::MAX;
        }

        let mut earliest_pos = i32::MAX;

        for entry in walkdir::WalkDir::new(&mod_path)
            .into_iter()
            .filter_map(|e| e.ok())
        {
            if !entry.file_type().is_file() {
                continue;
            }

            let ext = entry
                .path()
                .extension()
                .and_then(|e| e.to_str())
                .unwrap_or("")
                .to_lowercase();

            if ext == "esp" || ext == "esm" || ext == "esl" {
                let plugin_name = entry
                    .path()
                    .file_name()
                    .and_then(|n| n.to_str())
                    .unwrap_or("")
                    .to_lowercase();

                if let Some(&pos) = plugin_position.get(&plugin_name) {
                    earliest_pos = earliest_pos.min(pos);
                }
            }
        }

        earliest_pos
    }

    /// Write modlist.txt file
    pub fn write_modlist(path: &Path, mod_order: &[String]) -> Result<()> {
        let mut file = File::create(path).context("Failed to create modlist.txt")?;

        writeln!(file, "# This file was automatically generated by NexusBridge")?;
        writeln!(file, "# Mod priority: Top = Winner, Bottom = Loser")?;

        for folder_name in mod_order {
            writeln!(file, "+{}", folder_name)?;
        }

        info!("Generated modlist.txt with {} mods", mod_order.len());

        Ok(())
    }

    /// Simple mod order without rules (fallback)
    pub fn generate_mod_order(mods: &[ModInfo]) -> Vec<String> {
        Self::generate_mod_order_combined(mods, &[], &[], Path::new(""))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_simple_order() {
        let mods = vec![
            ModInfo {
                name: "ModA".to_string(),
                logical_filename: "moda.7z".to_string(),
                folder_name: "ModA".to_string(),
                md5: "aaa".to_string(),
            },
            ModInfo {
                name: "ModB".to_string(),
                logical_filename: "modb.7z".to_string(),
                folder_name: "ModB".to_string(),
                md5: "bbb".to_string(),
            },
        ];

        let order = ModListGenerator::generate_mod_order(&mods);
        assert_eq!(order.len(), 2);
    }

    #[test]
    fn test_before_rule() {
        let mods = vec![
            ModInfo {
                name: "ModA".to_string(),
                logical_filename: "moda.7z".to_string(),
                folder_name: "ModA".to_string(),
                md5: "aaa".to_string(),
            },
            ModInfo {
                name: "ModB".to_string(),
                logical_filename: "modb.7z".to_string(),
                folder_name: "ModB".to_string(),
                md5: "bbb".to_string(),
            },
        ];

        let rules = vec![ModRule {
            rule_type: "before".to_string(),
            source_logical_name: "moda.7z".to_string(),
            source_md5: String::new(),
            reference_logical_name: "modb.7z".to_string(),
            reference_md5: String::new(),
        }];

        let order =
            ModListGenerator::generate_mod_order_combined(&mods, &rules, &[], Path::new(""));

        // ModA before ModB means ModA has lower priority
        // MO2: Top = Winner, so ModB (higher priority) should be first (top), ModA second (bottom)
        let pos_a = order.iter().position(|x| x == "ModA").unwrap();
        let pos_b = order.iter().position(|x| x == "ModB").unwrap();
        assert!(pos_b < pos_a, "ModB should come before ModA (higher priority at top)");
    }

    #[test]
    fn test_after_rule() {
        let mods = vec![
            ModInfo {
                name: "ModA".to_string(),
                logical_filename: "moda.7z".to_string(),
                folder_name: "ModA".to_string(),
                md5: "aaa".to_string(),
            },
            ModInfo {
                name: "ModB".to_string(),
                logical_filename: "modb.7z".to_string(),
                folder_name: "ModB".to_string(),
                md5: "bbb".to_string(),
            },
        ];

        let rules = vec![ModRule {
            rule_type: "after".to_string(),
            source_logical_name: "moda.7z".to_string(),
            source_md5: String::new(),
            reference_logical_name: "modb.7z".to_string(),
            reference_md5: String::new(),
        }];

        let order =
            ModListGenerator::generate_mod_order_combined(&mods, &rules, &[], Path::new(""));

        // ModA after ModB means ModA has higher priority
        // MO2: Top = Winner, so ModA (higher priority) should be first (top), ModB second (bottom)
        let pos_a = order.iter().position(|x| x == "ModA").unwrap();
        let pos_b = order.iter().position(|x| x == "ModB").unwrap();
        assert!(pos_a < pos_b, "ModA should come before ModB (higher priority at top)");
    }
}
