//! CLF3 - Wabbajack Modlist Installer
//!
//! Named after Chlorine Trifluoride - burns through modlists
//! like CLF3 burns through concrete.

pub mod archive;
pub mod collection;
pub mod downloaders;
pub mod file_router;
pub mod games;
pub mod loot;
pub mod mo2;

// Re-export commonly used types
pub use collection::{Collection, CollectionInstaller, InstallerConfig};
pub use file_router::{FileRouter, ModType};
pub use games::{validate_game_path, GameType};
pub use mo2::Mo2Instance;
