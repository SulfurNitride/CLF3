//! Texture/DDS processing
//!
//! Pure Rust texture processing using image_dds with DirectXTex fallback
//! for legacy formats. Includes rayon parallelism for batch processing.

// Module not yet integrated into main installation pipeline
#![allow(dead_code)]
#![allow(unused_imports)]

mod processor;

pub use processor::{
    process_texture, process_texture_batch, process_texture_with_fallback, resize_texture,
    OutputFormat, ProcessedTexture, TextureInfo, TextureJob,
};
