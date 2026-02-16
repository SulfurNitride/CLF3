//! Texture/DDS processing with GPU acceleration
//!
//! GPU (wgpu/block_compression) for BC7 encoding, CPU (image_dds) for other formats.
//! DirectXTex only for legacy format DECODING (L8, RGB565, etc.)

// Module not yet integrated into main installation pipeline
#![allow(dead_code)]
#![allow(unused_imports)]

mod gpu_encoder;
mod processor;

pub use gpu_encoder::{is_gpu_available, list_gpus, GpuEncoder, GpuInfo};
pub use processor::{
    estimate_dds_size, init_gpu, process_texture, process_texture_batch,
    process_texture_with_fallback, resize_texture, OutputFormat, ProcessedTexture, TextureInfo,
    TextureJob,
};
