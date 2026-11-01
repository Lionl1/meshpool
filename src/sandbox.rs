//! Deterministic WebAssembly sandbox.
//!
//! Host ABI (module "env"): `input_len() -> i32`, `read_input(ptr)` copies the input to
//! `ptr`, `write_output(ptr, len)` sets the result. The module exports `memory` and `run`.
//! Jobs submitted with `gpu: true` may also call `gpu_run(shader_ptr, shader_len, data_ptr,
//! data_len, x, y, z)`: see `crate::gpu` (devices built with the `gpu` feature).
//! No other host access exists: no files, network, clock or randomness.
//!
//! Only what the task itself determines is a vote (`Failed`): every honest device gets the
//! same trap, fuel and output. Problems of the device (allocation failure and the like) are
//! `Host` errors: the task goes to another device and nobody is blamed.

use wasmtime::{
    Caller, Config, Engine, Error, ExternType, Linker, Module, Store, StoreLimits,
    StoreLimitsBuilder, Trap,
};

use std::sync::Arc;

use crate::proto::FailKind;

#[cfg(feature = "gpu")]
type GpuHandle = Arc<crate::gpu::Gpu>;
/// Without the `gpu` feature there is never a GPU.
#[cfg(not(feature = "gpu"))]
type GpuHandle = Arc<std::convert::Infallible>;

pub const MAX_OUTPUT: usize = 64 << 20;
/// Tables live outside linear memory: cap them, or a task could exhaust the host.
pub const MAX_TABLE_ELEMENTS: usize = 1 << 20;
const IMPORTS: [&str; 4] = ["input_len", "read_input", "write_output", "gpu_run"];

pub struct Sandbox {
    engine: Engine,
    linker: Linker<Ctx>,
    gpu: Option<GpuHandle>,
}

struct Ctx {
    input: Vec<u8>,
    output: Option<Vec<u8>>,
    /// Set by host functions when the task broke the ABI (a deterministic failure).
    guest_error: Option<FailKind>,
    limits: StoreLimits,
    #[cfg_attr(not(feature = "gpu"), allow(dead_code))]
    gpu: Option<GpuHandle>,
}

#[derive(Debug)]
pub struct Run {
    pub output: Vec<u8>,
    pub fuel_used: u64,
}

#[derive(Debug)]
pub enum RunError {
    /// Deterministic: every honest replica reports the same kind and fuel.
    Failed {
        kind: FailKind,
        fuel_used: u64,
        detail: String,
    },
    /// This device could not run the task; not the task's fault.
    Host(String),
}

fn invalid(detail: impl ToString) -> RunError {
    RunError::Failed {
        kind: FailKind::InvalidModule,
        fuel_used: 0,
        detail: detail.to_string(),
    }
}

fn guest_error(caller: &mut Caller<'_, Ctx>, kind: FailKind, msg: &str) -> Error {
    caller.data_mut().guest_error = Some(kind);
    Error::msg(msg.to_string())
}

fn memory(caller: &mut Caller<'_, Ctx>) -> wasmtime::Result<wasmtime::Memory> {
    match caller.get_export("memory").and_then(|e| e.into_memory()) {
        Some(memory) => Ok(memory),
        None => Err(guest_error(caller, FailKind::InvalidModule, "no memory")),
    }
}

/// `env.gpu_run`: every failure the task causes is a deterministic trap; a failing GPU is
/// a host error (the task goes elsewhere).
#[allow(clippy::too_many_arguments)]
fn gpu_run(
    mut c: Caller<'_, Ctx>,
    shader_ptr: i32,
    shader_len: i32,
    data_ptr: i32,
    data_len: i32,
    x: i32,
    y: i32,
    z: i32,
) -> wasmtime::Result<()> {
    #[cfg(not(feature = "gpu"))]
    {
        let _ = (&mut c, shader_ptr, shader_len, data_ptr, data_len, x, y, z);
        Err(Error::msg("this device has no GPU"))
    }
    #[cfg(feature = "gpu")]
    {
        use crate::gpu::{GpuError, check, check_args};
        let Some(gpu) = c.data().gpu.clone() else {
            return Err(Error::msg("this device has no GPU"));
        };
        let mem = memory(&mut c)?;
        let range = |ptr: i32, len: i32| {
            let start = ptr as u32 as usize;
            start..start + len as u32 as usize
        };
        let (shader, data_range) = (range(shader_ptr, shader_len), range(data_ptr, data_len));
        let source = match mem.data(&c).get(shader).map(std::str::from_utf8) {
            Some(Ok(source)) => source.to_string(),
            Some(Err(_)) => return Err(guest_error(&mut c, FailKind::Trap, "shader is not UTF-8")),
            None => return Err(guest_error(&mut c, FailKind::Trap, "gpu_run out of bounds")),
        };
        let groups = [x as u32, y as u32, z as u32];
        let shader =
            match check(&source).and_then(|s| check_args(data_range.len(), groups).map(|_| s)) {
                Ok(shader) => shader,
                Err(e) => return Err(guest_error(&mut c, FailKind::Trap, &e)),
            };
        let cost = shader.fuel(data_range.len(), groups);
        let left = c.get_fuel()?;
        if cost > left {
            c.set_fuel(0)?;
            return Err(guest_error(
                &mut c,
                FailKind::OutOfFuel,
                "out of fuel for gpu_run",
            ));
        }
        c.set_fuel(left - cost)?;
        let Some(data) = mem.data(&c).get(data_range.clone()) else {
            return Err(guest_error(&mut c, FailKind::Trap, "gpu_run out of bounds"));
        };
        let mut data = data.to_vec();
        match gpu.run(&shader, &mut data, groups) {
            Ok(()) => {
                mem.data_mut(&mut c)[data_range].copy_from_slice(&data);
                Ok(())
            }
            Err(GpuError::Invalid(e)) => Err(guest_error(&mut c, FailKind::Trap, &e)),
            Err(GpuError::Device(e)) => Err(Error::msg(format!("GPU failed: {e}"))),
        }
    }
}

impl Sandbox {
    pub fn new() -> anyhow::Result<Self> {
        let mut config = Config::new();
        // Replicas must compute bit-identical results and fuel counts.
        config.consume_fuel(true);
        config.cranelift_nan_canonicalization(true);
        config.wasm_relaxed_simd(false);
        config.wasm_threads(false);
        let engine = Engine::new(&config).map_err(|e| anyhow::anyhow!("{e}"))?;
        let mut linker = Linker::new(&engine);
        let defined = linker
            .func_wrap("env", "input_len", |c: Caller<'_, Ctx>| {
                c.data().input.len() as i32
            })
            .and_then(|l| {
                l.func_wrap("env", "read_input", |mut c: Caller<'_, Ctx>, ptr: i32| {
                    let mem = memory(&mut c)?;
                    let (data, ctx) = mem.data_and_store_mut(&mut c);
                    let start = ptr as u32 as usize;
                    match data.get_mut(start..start + ctx.input.len()) {
                        Some(dst) => {
                            dst.copy_from_slice(&ctx.input);
                            Ok(())
                        }
                        None => Err(guest_error(
                            &mut c,
                            FailKind::Trap,
                            "read_input out of bounds",
                        )),
                    }
                })
            })
            .and_then(|l| {
                l.func_wrap(
                    "env",
                    "write_output",
                    |mut c: Caller<'_, Ctx>, ptr: i32, len: i32| {
                        let (start, len) = (ptr as u32 as usize, len as u32 as usize);
                        if len > MAX_OUTPUT {
                            return Err(guest_error(
                                &mut c,
                                FailKind::OutputTooLarge,
                                "output too large",
                            ));
                        }
                        let mem = memory(&mut c)?;
                        let (data, ctx) = mem.data_and_store_mut(&mut c);
                        match data.get(start..start + len) {
                            Some(src) => {
                                ctx.output = Some(src.to_vec());
                                Ok(())
                            }
                            None => Err(guest_error(
                                &mut c,
                                FailKind::Trap,
                                "write_output out of bounds",
                            )),
                        }
                    },
                )
            })
            .and_then(|l| l.func_wrap("env", "gpu_run", gpu_run));
        defined.map_err(|e| anyhow::anyhow!("{e}"))?;
        #[cfg(feature = "gpu")]
        let gpu = crate::gpu::Gpu::open().map(Arc::new);
        #[cfg(not(feature = "gpu"))]
        let gpu = None;
        Ok(Sandbox {
            engine,
            linker,
            gpu,
        })
    }

    /// The GPU this device lends, if it was built with the `gpu` feature and has one.
    pub fn gpu_name(&self) -> Option<String> {
        #[cfg(feature = "gpu")]
        return self.gpu.as_ref().map(|g| g.name().to_string());
        #[cfg(not(feature = "gpu"))]
        None
    }

    /// Compile and check a module against the ABI (binary format only: no text parser on
    /// devices). Every way a module can fail to link is caught here, deterministically.
    pub fn compile(&self, wasm: &[u8]) -> Result<Module, RunError> {
        let module = Module::from_binary(&self.engine, wasm).map_err(invalid)?;
        for import in module.imports() {
            let known = import.module() == "env" && IMPORTS.contains(&import.name());
            if !known || !matches!(import.ty(), ExternType::Func(_)) {
                return Err(invalid(format!(
                    "unknown import {}.{}",
                    import.module(),
                    import.name()
                )));
            }
        }
        let exports = |name, memory: bool| {
            module.get_export(name).is_some_and(|t| match t {
                ExternType::Memory(_) => memory,
                ExternType::Func(f) => !memory && f.params().len() == 0 && f.results().len() == 0,
                _ => false,
            })
        };
        if !exports("memory", true) || !exports("run", false) {
            return Err(invalid("module must export `memory` and `run: () -> ()`"));
        }
        Ok(module)
    }

    /// Run a task; `gpu` is the job's permission to call `gpu_run`.
    pub fn run(
        &self,
        module: &Module,
        input: Vec<u8>,
        fuel: u64,
        memory_mb: u32,
        gpu: bool,
    ) -> Result<Run, RunError> {
        if module.imports().any(|i| i.name() == "gpu_run") {
            if !gpu {
                return Err(invalid(
                    "the module uses the GPU: submit the job with gpu: true",
                ));
            }
            if self.gpu.is_none() {
                return Err(RunError::Host("this device has no GPU".into()));
            }
        }
        let limits = StoreLimitsBuilder::new()
            .memory_size(memory_mb as usize * (1 << 20))
            .table_elements(MAX_TABLE_ELEMENTS)
            .instances(1)
            .tables(16)
            .memories(1)
            .build();
        let ctx = Ctx {
            input,
            output: None,
            guest_error: None,
            limits,
            gpu: self.gpu.clone(),
        };
        let mut store = Store::new(&self.engine, ctx);
        store.limiter(|ctx| &mut ctx.limits);
        store
            .set_fuel(fuel)
            .map_err(|e| RunError::Host(e.to_string()))?;
        let result = self
            .linker
            .instantiate(&mut store, module)
            .and_then(|instance| {
                let run = instance.get_typed_func::<(), ()>(&mut store, "run")?;
                run.call(&mut store, ())
            });
        let fuel_used = fuel - store.get_fuel().unwrap_or(0);
        let Err(e) = result else {
            let output = store.into_data().output.unwrap_or_default();
            return Ok(Run { output, fuel_used });
        };
        let kind = match (store.data().guest_error, e.downcast_ref::<Trap>()) {
            (Some(kind), _) => kind,
            (None, Some(Trap::OutOfFuel)) => FailKind::OutOfFuel,
            (None, Some(_)) => FailKind::Trap,
            // Not a trap and not the task breaking the ABI: the device failed (e.g. it
            // could not allocate the instance).
            (None, None) => return Err(RunError::Host(e.to_string())),
        };
        let fuel_used = if kind == FailKind::InvalidModule {
            0
        } else {
            fuel_used
        };
        Err(RunError::Failed {
            kind,
            fuel_used,
            detail: e.to_string(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn run(wat: &str, fuel: u64) -> Result<Run, RunError> {
        let sandbox = Sandbox::new().unwrap();
        let module = sandbox.compile(&wat::parse_str(wat).unwrap())?;
        sandbox.run(&module, b"abc".to_vec(), fuel, 1, false)
    }

    fn kind(result: Result<Run, RunError>) -> FailKind {
        match result {
            Err(RunError::Failed { kind, .. }) => kind,
            other => panic!("expected a failure vote, got {other:?}"),
        }
    }

    const MEM: &str = r#"(memory (export "memory") 1)"#;

    #[test]
    fn modules_breaking_the_abi_are_invalid() {
        let missing_run = format!("(module {MEM})");
        let foreign_import =
            format!(r#"(module (import "wasi" "fd_write" (func)) {MEM} (func (export "run")))"#);
        assert_eq!(kind(run(&missing_run, 1000)), FailKind::InvalidModule);
        assert_eq!(kind(run(&foreign_import, 1000)), FailKind::InvalidModule);
    }

    #[test]
    fn huge_tables_cannot_be_grown() {
        // table.grow returns -1 past the limit; the task stores it as its output.
        let wat = format!(
            r#"(module (import "env" "write_output" (func $w (param i32 i32))) {MEM}
              (table 1 funcref)
              (func (export "run")
                (i32.store (i32.const 0) (table.grow (ref.null func) (i32.const 100000000)))
                (call $w (i32.const 0) (i32.const 4))))"#
        );
        assert_eq!(run(&wat, 1_000_000).unwrap().output, (-1i32).to_le_bytes());
    }

    #[test]
    fn failures_report_deterministic_fuel() {
        let spin = format!(r#"(module {MEM} (func (export "run") (loop $l (br $l))))"#);
        match run(&spin, 5000) {
            Err(RunError::Failed {
                kind: FailKind::OutOfFuel,
                fuel_used,
                ..
            }) => assert_eq!(fuel_used, 5000),
            other => panic!("{other:?}"),
        }
        let oob = format!(
            r#"(module (import "env" "write_output" (func $w (param i32 i32))) {MEM}
              (func (export "run") (call $w (i32.const 65530) (i32.const 100))))"#
        );
        assert_eq!(kind(run(&oob, 1000)), FailKind::Trap);
    }
}
