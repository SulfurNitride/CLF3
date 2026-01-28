//! Directive handlers
//!
//! Each handler processes a specific directive type to install files.

pub mod create_bsa;
pub mod from_archive;
pub mod inline;
pub mod patched;
pub mod texture;

pub use create_bsa::{handle_create_bsa, output_bsa_valid};
pub use from_archive::handle_from_archive;
pub use inline::{handle_inline_file, handle_remapped_inline_file};
pub use patched::handle_patched_from_archive;
pub use texture::handle_transformed_texture;
