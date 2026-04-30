//! Mod list generator for MO2 modlist.txt
//!
//! Strategy: LOOT drives the sort. After libloot orders plugins, each mod
//! takes the earliest position of any plugin it ships, and that position
//! is the mod's priority in modlist.txt. Mods without plugins (textures,
//! meshes, sound, etc.) sit above plugin mods (so they can override
//! plugin-mod assets), ordered by Vortex `phase` then collection index.
//!
//! Kahn's topological sort handles `modRules[]` (`before`/`after`); when
//! constraints aren't violated the tie-breaker above shapes the order.

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
    /// Vortex install phase. Lower phase = installed earlier = lower MO2
    /// priority (bottom of modlist). Defaults to 0 when not specified.
    pub phase: i32,
}

/// Mod list generator backed by LOOT plugin order + Kahn's topological sort.
pub struct ModListGenerator;

impl ModListGenerator {
    /// Build mod order for `modlist.txt`.
    ///
    /// Sort key (Kahn pops MIN first → ends up at the BOTTOM of modlist
    /// after we reverse for `top = winner`):
    ///
    /// `(has_no_plugin, plugin_pos, phase, collection_index)`
    ///
    /// - `has_no_plugin`: 0 = mod ships an ESP/ESM/ESL (gets ordered by
    ///   LOOT among the plugin mods). 1 = no plugin → texture/mesh/sound
    ///   mod, sits ABOVE plugin mods so it can override their assets.
    /// - `plugin_pos`: earliest position of this mod's plugins in the
    ///   LOOT-sorted plugin list. Foundations (masters, libraries) have
    ///   low positions, so they pop first → bottom of modlist. Patches
    ///   have high positions → top.
    /// - `phase`: Vortex install phase (low → installed early → bottom).
    /// - `collection_index`: tie-break, preserves the collection JSON's
    ///   own ordering when nothing else differentiates.
    ///
    /// `modRules[]` (`before`/`after`) are honored as Kahn constraints.
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

        let plugin_position = Self::build_plugin_position_map(sorted_plugins);

        let mut mod_plugin_pos: Vec<i32> = Vec::with_capacity(n);
        let mut mods_with_plugins = 0;
        for folder in &mod_folders {
            let pos = Self::get_mod_plugin_position(folder, mods_dir, &plugin_position);
            if pos < i32::MAX {
                mods_with_plugins += 1;
            }
            mod_plugin_pos.push(pos);
        }

        // Adjacency lists from `modRules[]`.
        let mut successors: Vec<Vec<usize>> = vec![Vec::new(); n];
        let mut predecessors: Vec<Vec<usize>> = vec![Vec::new(); n];
        let mut applied_rules = 0;
        for rule in rules {
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
            let Some(&src_idx) = logical_name_to_idx.get(&src_key) else { continue };
            let Some(&ref_idx) = logical_name_to_idx.get(&ref_key) else { continue };
            match rule.rule_type.as_str() {
                "before" => {
                    successors[src_idx].push(ref_idx);
                    predecessors[ref_idx].push(src_idx);
                    applied_rules += 1;
                }
                "after" => {
                    successors[ref_idx].push(src_idx);
                    predecessors[src_idx].push(ref_idx);
                    applied_rules += 1;
                }
                _ => {}
            }
        }

        info!(
            "Sort: applied {} mod rules; {}/{} mods carry plugins (LOOT-driven order)",
            applied_rules, mods_with_plugins, n
        );

        // Build the per-mod tie-breaker tuple. Kahn pops MIN first → ends
        // up at the BOTTOM of modlist after reverse.
        let tie_breaker: Vec<(i32, i32, i32, i32)> = (0..n)
            .map(|i| {
                let pos = mod_plugin_pos[i];
                let no_plugin = if pos == i32::MAX { 1 } else { 0 };
                (no_plugin, pos, mods[i].phase, i as i32)
            })
            .collect();

        let final_indices = Self::kahn_sort(n, &successors, &predecessors, &tie_breaker);

        // Constraint-violation accounting.
        let mut final_position: HashMap<usize, usize> = HashMap::new();
        for (i, &idx) in final_indices.iter().enumerate() {
            final_position.insert(idx, i);
        }
        let mut violations = 0;
        for (i, preds) in predecessors.iter().enumerate() {
            for &pred in preds {
                if final_position.get(&pred).unwrap_or(&0) > final_position.get(&i).unwrap_or(&0) {
                    violations += 1;
                }
            }
        }
        if violations > 0 {
            tracing::warn!("{} constraint violations (cycles in mod rules)", violations);
        }

        // MO2: top of modlist = winner. Kahn produced sources-first; we
        // reverse so the highest-priority mods land at the top.
        let mut result: Vec<String> = final_indices
            .into_iter()
            .map(|i| mod_folders[i].clone())
            .collect();
        result.reverse();
        result
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
                phase: 0,
            },
            ModInfo {
                name: "ModB".to_string(),
                logical_filename: "modb.7z".to_string(),
                folder_name: "ModB".to_string(),
                md5: "bbb".to_string(),
                phase: 0,
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
                phase: 0,
            },
            ModInfo {
                name: "ModB".to_string(),
                logical_filename: "modb.7z".to_string(),
                folder_name: "ModB".to_string(),
                md5: "bbb".to_string(),
                phase: 0,
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
                phase: 0,
            },
            ModInfo {
                name: "ModB".to_string(),
                logical_filename: "modb.7z".to_string(),
                folder_name: "ModB".to_string(),
                md5: "bbb".to_string(),
                phase: 0,
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
