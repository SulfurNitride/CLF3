//! GPU-accelerated texture encoding using wgpu + block_compression
//!
//! Based on Radium-Textures implementation. Uses compute shaders for BC7 encoding.
//! DirectXTex is only used for legacy format DECODING (L8, RGB565, etc.)

use anyhow::{Context, Result};
use block_compression::{BC7Settings, CompressionVariant, GpuBlockCompressor};
use std::sync::Arc;
use tracing::{debug, info, warn};
use wgpu::{
    Backends, Buffer, BufferDescriptor, BufferUsages, CommandEncoderDescriptor, Device, Extent3d,
    Instance, Queue, Texture, TextureDescriptor, TextureDimension, TextureFormat, TextureUsages,
    TextureView, TextureViewDescriptor,
};

/// GPU information for display/selection
#[derive(Debug, Clone)]
pub struct GpuInfo {
    pub name: String,
    pub backend: String,
    pub device_type: String,
    pub adapter_index: usize,
}

impl std::fmt::Display for GpuInfo {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} ({}, {})", self.name, self.backend, self.device_type)
    }
}

/// A queued BC7 encoding task (texture + buffers kept alive for batching)
struct QueuedEncode {
    #[allow(dead_code)] // Texture must stay alive while texture_view is used
    texture: Texture,
    texture_view: TextureView,
    output_buffer: Buffer,
    staging_buffer: Buffer,
    output_size: u64,
    width: u32,
    height: u32,
}

/// Batch of queued encoding tasks
pub struct EncodeBatch {
    tasks: Vec<QueuedEncode>,
    settings: BC7Settings,
}

/// GPU encoder for BC7 texture compression
pub struct GpuEncoder {
    device: Arc<Device>,
    queue: Arc<Queue>,
    compressor: GpuBlockCompressor,
    batch_budget_bytes: u64,
    pub gpu_info: GpuInfo,
}

impl GpuEncoder {
    /// Initialize GPU encoder with automatic GPU selection
    pub fn new() -> Result<Self> {
        Self::with_gpu_index(None)
    }

    /// Initialize GPU encoder with specific GPU index
    pub fn with_gpu_index(gpu_index: Option<usize>) -> Result<Self> {
        pollster::block_on(Self::new_async(gpu_index))
    }

    async fn new_async(gpu_index: Option<usize>) -> Result<Self> {
        info!("Initializing GPU encoder...");

        let instance = Instance::new(&wgpu::InstanceDescriptor {
            backends: Backends::VULKAN | Backends::DX12 | Backends::METAL,
            ..Default::default()
        });

        let adapters = instance.enumerate_adapters(Backends::all()).await;

        if adapters.is_empty() {
            anyhow::bail!("No GPU found on this system");
        }

        // Log available GPUs
        for (i, adapter) in adapters.iter().enumerate() {
            let info = adapter.get_info();
            info!(
                "GPU {}: {} ({:?}, {:?})",
                i, info.name, info.backend, info.device_type
            );
        }

        // Select adapter
        let (adapter_index, adapter) = if let Some(idx) = gpu_index {
            if idx >= adapters.len() {
                anyhow::bail!(
                    "GPU index {} out of range (found {} GPUs)",
                    idx,
                    adapters.len()
                );
            }
            (idx, &adapters[idx])
        } else {
            // Auto-select: prefer discrete GPU, then Vulkan backend
            adapters
                .iter()
                .enumerate()
                .max_by_key(|(_, a)| {
                    let info = a.get_info();
                    let mut score = 0i32;
                    if info.device_type == wgpu::DeviceType::DiscreteGpu {
                        score += 100;
                    }
                    if info.backend == wgpu::Backend::Vulkan {
                        score += 10;
                    }
                    score
                })
                .unwrap()
        };

        let adapter_info = adapter.get_info();
        let adapter_limits = adapter.limits();
        let gpu_info = GpuInfo {
            name: adapter_info.name.clone(),
            backend: format!("{:?}", adapter_info.backend),
            device_type: format!("{:?}", adapter_info.device_type),
            adapter_index,
        };

        info!(
            "Selected GPU: {} ({}, {})",
            gpu_info.name, gpu_info.backend, gpu_info.device_type
        );

        // Derive conservative per-batch GPU memory budget from adapter limits.
        // This is a practical proxy for VRAM pressure since direct VRAM reporting
        // is not consistently available across backends.
        let binding_cap = adapter_limits
            .max_buffer_size
            .min(adapter_limits.max_storage_buffer_binding_size as u64)
            .max(64 * 1024 * 1024);
        let utilization = match adapter_info.device_type {
            wgpu::DeviceType::DiscreteGpu => 0.25,
            wgpu::DeviceType::IntegratedGpu => 0.12,
            wgpu::DeviceType::VirtualGpu => 0.08,
            wgpu::DeviceType::Cpu => 0.04,
            _ => 0.10,
        };
        let batch_budget_bytes =
            ((binding_cap as f64) * utilization) as u64;
        let batch_budget_bytes = batch_budget_bytes.clamp(64 * 1024 * 1024, 1024 * 1024 * 1024);

        info!(
            "GPU batch budget: {:.0} MB (binding_cap={:.0} MB, utilization={:.0}%)",
            batch_budget_bytes as f64 / 1024.0 / 1024.0,
            binding_cap as f64 / 1024.0 / 1024.0,
            utilization * 100.0,
        );

        // Request device
        let (device, queue) = adapter
            .request_device(&wgpu::DeviceDescriptor {
                label: Some("CLF3 GPU Encoder"),
                required_features: wgpu::Features::empty(),
                required_limits: wgpu::Limits::default(),
                ..Default::default()
            })
            .await
            .context("Failed to create GPU device")?;

        let device = Arc::new(device);
        let queue = Arc::new(queue);

        // Initialize BC7 compressor
        info!("Initializing BC7 GPU compressor...");
        let compressor = GpuBlockCompressor::new((*device).clone(), (*queue).clone());

        info!("GPU encoder initialized successfully");

        Ok(Self {
            device,
            queue,
            compressor,
            batch_budget_bytes,
            gpu_info,
        })
    }

    /// Encode RGBA data to BC7 using GPU
    pub fn encode_bc7(&mut self, rgba_data: &[u8], width: u32, height: u32) -> Result<Vec<u8>> {
        self.encode_bc7_with_settings(rgba_data, width, height, BC7Settings::alpha_basic())
    }

    /// Encode RGBA data to BC7 with custom settings
    pub fn encode_bc7_with_settings(
        &mut self,
        rgba_data: &[u8],
        width: u32,
        height: u32,
        settings: BC7Settings,
    ) -> Result<Vec<u8>> {
        if rgba_data.len() != (width * height * 4) as usize {
            anyhow::bail!(
                "Invalid RGBA data size: expected {} bytes, got {}",
                width * height * 4,
                rgba_data.len()
            );
        }

        // BC7 requires dimensions to be multiples of 4
        if width % 4 != 0 || height % 4 != 0 {
            anyhow::bail!(
                "BC7 requires dimensions divisible by 4, got {}x{}",
                width,
                height
            );
        }

        debug!("GPU encoding BC7: {}x{}", width, height);

        // Create GPU texture from RGBA data
        let texture = self.device.create_texture(&TextureDescriptor {
            label: Some("BC7 source texture"),
            size: Extent3d {
                width,
                height,
                depth_or_array_layers: 1,
            },
            mip_level_count: 1,
            sample_count: 1,
            dimension: TextureDimension::D2,
            format: TextureFormat::Rgba8Unorm,
            usage: TextureUsages::TEXTURE_BINDING | TextureUsages::COPY_DST,
            view_formats: &[],
        });

        // Upload RGBA data to texture
        self.queue.write_texture(
            wgpu::TexelCopyTextureInfo {
                texture: &texture,
                mip_level: 0,
                origin: wgpu::Origin3d::ZERO,
                aspect: wgpu::TextureAspect::All,
            },
            rgba_data,
            wgpu::TexelCopyBufferLayout {
                offset: 0,
                bytes_per_row: Some(width * 4),
                rows_per_image: Some(height),
            },
            Extent3d {
                width,
                height,
                depth_or_array_layers: 1,
            },
        );

        let texture_view = texture.create_view(&TextureViewDescriptor::default());

        // Create output buffer
        let variant = CompressionVariant::BC7(settings);
        let output_size = variant.blocks_byte_size(width, height) as u64;

        let output_buffer = self.device.create_buffer(&BufferDescriptor {
            label: Some("BC7 output buffer"),
            size: output_size,
            usage: BufferUsages::STORAGE | BufferUsages::COPY_SRC,
            mapped_at_creation: false,
        });

        // Staging buffer for readback
        let staging_buffer = self.device.create_buffer(&BufferDescriptor {
            label: Some("BC7 staging buffer"),
            size: output_size,
            usage: BufferUsages::MAP_READ | BufferUsages::COPY_DST,
            mapped_at_creation: false,
        });

        // Add compression task
        self.compressor.add_compression_task(
            variant,
            &texture_view,
            width,
            height,
            &output_buffer,
            None,
            None,
        );

        // Create command encoder and compute pass
        let mut encoder = self
            .device
            .create_command_encoder(&CommandEncoderDescriptor {
                label: Some("BC7 compression encoder"),
            });

        {
            let mut compute_pass = encoder.begin_compute_pass(&wgpu::ComputePassDescriptor {
                label: Some("BC7 compression pass"),
                timestamp_writes: None,
            });
            self.compressor.compress(&mut compute_pass);
        }

        // Copy result to staging buffer
        encoder.copy_buffer_to_buffer(&output_buffer, 0, &staging_buffer, 0, output_size);

        // Submit and wait
        self.queue.submit(std::iter::once(encoder.finish()));

        // Read back results
        let buffer_slice = staging_buffer.slice(..);
        let (tx, rx) = std::sync::mpsc::channel();
        buffer_slice.map_async(wgpu::MapMode::Read, move |result| {
            tx.send(result).unwrap();
        });

        // Poll until the buffer is mapped
        let _ = self.device.poll(wgpu::PollType::Wait {
            submission_index: None,
            timeout: None,
        });
        rx.recv()
            .context("Channel closed")?
            .context("Failed to map buffer")?;

        let data = buffer_slice.get_mapped_range();
        let result = data.to_vec();

        drop(data);
        staging_buffer.unmap();

        debug!("GPU BC7 encode complete: {} bytes", result.len());
        Ok(result)
    }

    /// Create a new batch for queuing multiple BC7 encodes
    pub fn create_batch(&self) -> EncodeBatch {
        EncodeBatch {
            tasks: Vec::new(),
            settings: BC7Settings::alpha_basic(),
        }
    }

    /// Queue a BC7 encoding task into a batch (doesn't execute yet)
    pub fn queue_bc7(
        &self,
        batch: &mut EncodeBatch,
        rgba_data: &[u8],
        width: u32,
        height: u32,
    ) -> Result<usize> {
        if rgba_data.len() != (width * height * 4) as usize {
            anyhow::bail!(
                "Invalid RGBA data size: expected {} bytes, got {}",
                width * height * 4,
                rgba_data.len()
            );
        }

        if width % 4 != 0 || height % 4 != 0 {
            anyhow::bail!(
                "BC7 requires dimensions divisible by 4, got {}x{}",
                width,
                height
            );
        }

        debug!("Queuing BC7 encode: {}x{}", width, height);

        // Create GPU texture
        let texture = self.device.create_texture(&TextureDescriptor {
            label: Some("BC7 batch source texture"),
            size: Extent3d {
                width,
                height,
                depth_or_array_layers: 1,
            },
            mip_level_count: 1,
            sample_count: 1,
            dimension: TextureDimension::D2,
            format: TextureFormat::Rgba8Unorm,
            usage: TextureUsages::TEXTURE_BINDING | TextureUsages::COPY_DST,
            view_formats: &[],
        });

        // Upload RGBA data
        self.queue.write_texture(
            wgpu::TexelCopyTextureInfo {
                texture: &texture,
                mip_level: 0,
                origin: wgpu::Origin3d::ZERO,
                aspect: wgpu::TextureAspect::All,
            },
            rgba_data,
            wgpu::TexelCopyBufferLayout {
                offset: 0,
                bytes_per_row: Some(width * 4),
                rows_per_image: Some(height),
            },
            Extent3d {
                width,
                height,
                depth_or_array_layers: 1,
            },
        );

        let texture_view = texture.create_view(&TextureViewDescriptor::default());

        // Create output buffer
        let variant = CompressionVariant::BC7(batch.settings);
        let output_size = variant.blocks_byte_size(width, height) as u64;

        let output_buffer = self.device.create_buffer(&BufferDescriptor {
            label: Some("BC7 batch output buffer"),
            size: output_size,
            usage: BufferUsages::STORAGE | BufferUsages::COPY_SRC,
            mapped_at_creation: false,
        });

        // Staging buffer for readback
        let staging_buffer = self.device.create_buffer(&BufferDescriptor {
            label: Some("BC7 batch staging buffer"),
            size: output_size,
            usage: BufferUsages::MAP_READ | BufferUsages::COPY_DST,
            mapped_at_creation: false,
        });

        let idx = batch.tasks.len();
        batch.tasks.push(QueuedEncode {
            texture,
            texture_view,
            output_buffer,
            staging_buffer,
            output_size,
            width,
            height,
        });

        Ok(idx)
    }

    /// Execute all queued tasks in the batch and return results
    pub fn flush_batch(&mut self, batch: EncodeBatch) -> Result<Vec<Vec<u8>>> {
        if batch.tasks.is_empty() {
            return Ok(Vec::new());
        }

        debug!("Flushing batch of {} BC7 encodes", batch.tasks.len());

        // Add all compression tasks
        let variant = CompressionVariant::BC7(batch.settings);
        for task in &batch.tasks {
            self.compressor.add_compression_task(
                variant,
                &task.texture_view,
                task.width,
                task.height,
                &task.output_buffer,
                None,
                None,
            );
        }

        // Create command encoder
        let mut encoder = self
            .device
            .create_command_encoder(&CommandEncoderDescriptor {
                label: Some("BC7 batch compression encoder"),
            });

        // Single compute pass for all tasks
        {
            let mut compute_pass = encoder.begin_compute_pass(&wgpu::ComputePassDescriptor {
                label: Some("BC7 batch compression pass"),
                timestamp_writes: None,
            });
            self.compressor.compress(&mut compute_pass);
        }

        // Copy all results to staging buffers
        for task in &batch.tasks {
            encoder.copy_buffer_to_buffer(
                &task.output_buffer,
                0,
                &task.staging_buffer,
                0,
                task.output_size,
            );
        }

        // Submit all work at once
        self.queue.submit(std::iter::once(encoder.finish()));

        // Map all staging buffers for read
        let channels: Vec<_> = batch
            .tasks
            .iter()
            .map(|task| {
                let (tx, rx) = std::sync::mpsc::channel();
                task.staging_buffer
                    .slice(..)
                    .map_async(wgpu::MapMode::Read, move |result| {
                        let _ = tx.send(result);
                    });
                rx
            })
            .collect();

        // Wait for GPU
        let _ = self.device.poll(wgpu::PollType::Wait {
            submission_index: None,
            timeout: None,
        });

        // Collect all results
        let mut results = Vec::with_capacity(batch.tasks.len());
        for (task, rx) in batch.tasks.iter().zip(channels) {
            rx.recv()
                .context("Channel closed")?
                .context("Failed to map buffer")?;

            let data = task.staging_buffer.slice(..).get_mapped_range();
            results.push(data.to_vec());
            drop(data);
            task.staging_buffer.unmap();
        }

        debug!("Batch flush complete: {} textures", results.len());
        Ok(results)
    }

    /// Get GPU information
    pub fn info(&self) -> &GpuInfo {
        &self.gpu_info
    }

    /// Recommended memory budget for a single GPU batch (bytes).
    pub fn batch_budget_bytes(&self) -> u64 {
        self.batch_budget_bytes
    }
}

/// List available GPUs
pub fn list_gpus() -> Vec<GpuInfo> {
    pollster::block_on(list_gpus_async())
}

async fn list_gpus_async() -> Vec<GpuInfo> {
    let instance = Instance::new(&wgpu::InstanceDescriptor {
        backends: Backends::VULKAN | Backends::DX12 | Backends::METAL,
        ..Default::default()
    });

    let adapters = instance.enumerate_adapters(Backends::all()).await;

    adapters
        .iter()
        .enumerate()
        .map(|(idx, adapter)| {
            let info = adapter.get_info();
            GpuInfo {
                name: info.name.clone(),
                backend: format!("{:?}", info.backend),
                device_type: format!("{:?}", info.device_type),
                adapter_index: idx,
            }
        })
        .collect()
}

/// Check if GPU acceleration is available
pub fn is_gpu_available() -> bool {
    pollster::block_on(is_gpu_available_async())
}

async fn is_gpu_available_async() -> bool {
    let instance = Instance::new(&wgpu::InstanceDescriptor {
        backends: Backends::VULKAN | Backends::DX12 | Backends::METAL,
        ..Default::default()
    });

    let adapters = instance.enumerate_adapters(Backends::all()).await;
    !adapters.is_empty()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_list_gpus() {
        let gpus = list_gpus();
        println!("Found {} GPUs:", gpus.len());
        for gpu in &gpus {
            println!("  - {}", gpu);
        }
    }

    #[test]
    fn test_gpu_available() {
        let available = is_gpu_available();
        println!("GPU available: {}", available);
    }

    #[test]
    #[ignore] // Requires GPU
    fn test_encode_bc7() {
        let mut encoder = GpuEncoder::new().expect("Failed to create encoder");

        // Create test 4x4 RGBA image (64 bytes)
        let rgba = vec![255u8; 4 * 4 * 4];
        let result = encoder.encode_bc7(&rgba, 4, 4).expect("Failed to encode");

        // BC7: 1 block (4x4) = 16 bytes
        assert_eq!(result.len(), 16);
    }
}
