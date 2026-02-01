//! CLF3 - Wabbajack Modlist Installer
//!
//! Named after Chlorine Trifluoride - burns through modlists
//! like CLF3 burns through concrete.

pub mod archive;
pub mod bsa;
pub mod downloaders;
pub mod file_router;
pub mod game_finder;
pub mod games;
pub mod gui;
pub mod installer;
pub mod loot;
pub mod mo2;
pub mod modlist;
pub mod nxm_handler;
pub mod octodiff;
pub mod paths;
pub mod textures;

// Re-export commonly used types
pub use file_router::{FileRouter, ModType};
pub use games::{validate_game_path, GameType};
pub use mo2::Mo2Instance;
