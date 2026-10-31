//! GPU compute for tasks through WebGPU (wgpu: Metal, Vulkan, DX12).
//!
//! A task calls `env.gpu_run(shader_ptr, shader_len, data_ptr, data_len, x, y, z) -> i32`:
//! the WGSL compute shader's `main` runs over `x × y × z` workgroups with the task's bytes
//! `data` bound as `@group(0) @binding(0) var<storage, read_write>`, and `data` is replaced
//! by the buffer's contents afterwards.
//!
//! Replicas must agree bit for bit, and GPU floats differ between vendors and drivers, so
//! shaders are checked before they run: integer and boolean types only. The shader is
//! validated by naga and runs with bounds checks and bounded loops, touching only its own
//! buffer. Its cost in fuel follows from the arguments alone (data size and invocations), so
//! every device charges the same.

use std::sync::mpsc;

use naga::valid::{Capabilities, ValidationFlags, Validator};
use naga::{Expression, Literal, ScalarKind, ShaderStage, TypeInner};

/// Largest buffer a task may bind (the WebGPU default storage binding limit).
pub const MAX_DATA: usize = 128 << 20;
/// Largest dispatch per dimension (the WebGPU default limit).
pub const MAX_GROUPS: u32 = 65_535;
/// Fuel charged per shader invocation, on top of one unit per byte of data.
pub const FUEL_PER_INVOCATION: u64 = 64;

#[derive(Debug)]
pub enum GpuError {
    /// The task's fault, the same on every device: a vote.
    Invalid(String),
    /// This device failed (lost device, driver error): the task goes elsewhere.
    Device(String),
}

/// A checked shader: what it costs to run.
#[derive(Debug)]
pub struct Shader {
    source: String,
    invocations_per_group: u64,
}

/// Parse and validate `wgsl`; reject anything that is not deterministic across GPUs.
pub fn check(wgsl: &str) -> Result<Shader, String> {
    let module = naga::front::wgsl::parse_str(wgsl).map_err(|e| e.emit_to_string(wgsl))?;
    Validator::new(ValidationFlags::all(), Capabilities::empty())
        .validate(&module)
        .map_err(|e| e.emit_to_string(wgsl))?;
    let float = |kind: ScalarKind| matches!(kind, ScalarKind::Float | ScalarKind::AbstractFloat);
    for (_, ty) in module.types.iter() {
        let floats = match &ty.inner {
            TypeInner::Scalar(s) | TypeInner::Atomic(s) => float(s.kind),
            TypeInner::Vector { scalar, .. } | TypeInner::Matrix { scalar, .. } => {
                float(scalar.kind)
            }
            _ => false,
        };
        if floats {
            return Err(
                "shaders may use integers and booleans only (floats differ between \
                        GPUs, so replicas could not agree)"
                    .into(),
            );
        }
    }
    let functions = module
        .functions
        .iter()
        .map(|(_, f)| f)
        .chain(module.entry_points.iter().map(|e| &e.function));
    for function in functions {
        for (_, expr) in function.expressions.iter() {
            let floats = match expr {
                Expression::Literal(l) => matches!(
                    l,
                    Literal::F64(_) | Literal::F32(_) | Literal::F16(_) | Literal::AbstractFloat(_)
                ),
                Expression::As { kind, .. } => float(*kind),
                _ => false,
            };
            if floats {
                return Err("shaders may use integers and booleans only".into());
            }
        }
    }
    let main = module
        .entry_points
        .iter()
        .find(|e| e.name == "main" && e.stage == ShaderStage::Compute)
        .ok_or("the shader needs a `@compute fn main`")?;
    let [x, y, z] = main.workgroup_size;
    Ok(Shader {
        source: wgsl.into(),
        invocations_per_group: u64::from(x) * u64::from(y) * u64::from(z),
    })
}

impl Shader {
    /// Fuel for one run: the same on every device, since it follows from the arguments.
    pub fn fuel(&self, data_len: usize, groups: [u32; 3]) -> u64 {
        let groups: u64 = groups.iter().map(|&g| u64::from(g)).product();
        (groups * self.invocations_per_group)
            .saturating_mul(FUEL_PER_INVOCATION)
            .saturating_add(data_len as u64)
    }
}

/// Checks on the arguments that do not depend on the device.
pub fn check_args(data_len: usize, groups: [u32; 3]) -> Result<(), String> {
    if data_len == 0 || !data_len.is_multiple_of(4) || data_len > MAX_DATA {
        return Err(format!(
            "gpu data must be 4..={MAX_DATA} bytes, a multiple of 4"
        ));
    }
    if groups.iter().any(|&g| g > MAX_GROUPS) {
        return Err(format!("at most {MAX_GROUPS} workgroups per dimension"));
    }
    Ok(())
}

pub struct Gpu {
    device: wgpu::Device,
    queue: wgpu::Queue,
    name: String,
}

impl Gpu {
    /// The first GPU wgpu can open, if any.
    pub fn open() -> Option<Self> {
        let instance = wgpu::Instance::new(wgpu::InstanceDescriptor::new_without_display_handle());
        let adapter =
            pollster::block_on(instance.request_adapter(&wgpu::RequestAdapterOptions::default()))
                .ok()?;
        let info = adapter.get_info();
        if info.device_type == wgpu::DeviceType::Cpu {
            return None; // a software renderer is not a GPU worth lending
        }
        let (device, queue) =
            pollster::block_on(adapter.request_device(&wgpu::DeviceDescriptor::default())).ok()?;
        Some(Gpu {
            device,
            queue,
            name: format!("{} ({:?})", info.name, info.backend),
        })
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    /// Run a checked shader over `data` in place.
    pub fn run(&self, shader: &Shader, data: &mut [u8], groups: [u32; 3]) -> Result<(), GpuError> {
        let device = &self.device;
        let scope = device.push_error_scope(wgpu::ErrorFilter::Validation);
        let module = device.create_shader_module(wgpu::ShaderModuleDescriptor {
            label: None,
            source: wgpu::ShaderSource::Wgsl(shader.source.as_str().into()),
        });
        let pipeline = device.create_compute_pipeline(&wgpu::ComputePipelineDescriptor {
            label: None,
            layout: None,
            module: &module,
            entry_point: Some("main"),
            compilation_options: Default::default(),
            cache: None,
        });
        let size = data.len() as u64;
        let storage = device.create_buffer(&wgpu::BufferDescriptor {
            label: None,
            size,
            usage: wgpu::BufferUsages::STORAGE
                | wgpu::BufferUsages::COPY_SRC
                | wgpu::BufferUsages::COPY_DST,
            mapped_at_creation: false,
        });
        let readback = device.create_buffer(&wgpu::BufferDescriptor {
            label: None,
            size,
            usage: wgpu::BufferUsages::MAP_READ | wgpu::BufferUsages::COPY_DST,
            mapped_at_creation: false,
        });
        self.queue.write_buffer(&storage, 0, data);
        // The binding layout is the shader's: one that does not fit fails validation.
        let bind_group = device.create_bind_group(&wgpu::BindGroupDescriptor {
            label: None,
            layout: &pipeline.get_bind_group_layout(0),
            entries: &[wgpu::BindGroupEntry {
                binding: 0,
                resource: storage.as_entire_binding(),
            }],
        });
        let mut encoder = device.create_command_encoder(&Default::default());
        {
            let mut pass = encoder.begin_compute_pass(&Default::default());
            pass.set_pipeline(&pipeline);
            pass.set_bind_group(0, &bind_group, &[]);
            pass.dispatch_workgroups(groups[0], groups[1], groups[2]);
        }
        encoder.copy_buffer_to_buffer(&storage, 0, &readback, 0, size);
        if let Some(e) = pollster::block_on(scope.pop()) {
            // The shader passed naga already; what is left is its interface (bindings).
            return Err(GpuError::Invalid(e.to_string()));
        }
        let index = self.queue.submit([encoder.finish()]);
        let (done, mapped) = mpsc::channel();
        readback.map_async(wgpu::MapMode::Read, .., move |r| {
            let _ = done.send(r);
        });
        device
            .poll(wgpu::PollType::Wait {
                submission_index: Some(index),
                timeout: None,
            })
            .map_err(|e| GpuError::Device(e.to_string()))?;
        match mapped.recv() {
            Ok(Ok(())) => {}
            Ok(Err(e)) => return Err(GpuError::Device(e.to_string())),
            Err(e) => return Err(GpuError::Device(e.to_string())),
        }
        let view = readback
            .get_mapped_range(..)
            .map_err(|e| GpuError::Device(e.to_string()))?;
        data.copy_from_slice(&view);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const DOUBLE: &str = "
        @group(0) @binding(0) var<storage, read_write> data: array<u32>;
        @compute @workgroup_size(64)
        fn main(@builtin(global_invocation_id) id: vec3<u32>) {
            if id.x < arrayLength(&data) { data[id.x] = data[id.x] * 2u + 1u; }
        }";

    #[test]
    fn floats_are_rejected() {
        let float = "
            @group(0) @binding(0) var<storage, read_write> data: array<f32>;
            @compute @workgroup_size(1) fn main() { data[0] = data[0] * 0.5; }";
        assert!(check(float).unwrap_err().contains("integers"));
        let cast = "
            @group(0) @binding(0) var<storage, read_write> data: array<u32>;
            @compute @workgroup_size(1) fn main() { data[0] = u32(sqrt(f32(data[0]))); }";
        assert!(check(cast).is_err());
        assert!(check("not wgsl").is_err());
        assert!(check(DOUBLE).is_ok());
    }

    #[test]
    fn fuel_follows_from_the_arguments() {
        let shader = check(DOUBLE).unwrap();
        assert_eq!(
            shader.fuel(400, [2, 1, 1]),
            2 * 64 * FUEL_PER_INVOCATION + 400
        );
        assert!(check_args(6, [1, 1, 1]).is_err());
        assert!(check_args(8, [MAX_GROUPS + 1, 1, 1]).is_err());
    }

    /// Needs a GPU; skipped (with a note) on machines without one, such as CI runners.
    #[test]
    fn runs_on_the_gpu() {
        let Some(gpu) = Gpu::open() else {
            eprintln!("no GPU here: skipping");
            return;
        };
        let mut data: Vec<u8> = (0u32..1000).flat_map(|v| v.to_le_bytes()).collect();
        gpu.run(&check(DOUBLE).unwrap(), &mut data, [16, 1, 1])
            .ok()
            .unwrap();
        let out: Vec<u32> = data
            .chunks(4)
            .map(|c| u32::from_le_bytes(c.try_into().unwrap()))
            .collect();
        assert_eq!(out[999], 999 * 2 + 1);
        assert!(out.iter().enumerate().all(|(i, &v)| v == i as u32 * 2 + 1));
    }
}
