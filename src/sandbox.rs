//! Deterministic WebAssembly sandbox.
//!
//! Host ABI (module "env"): `input_len() -> i32`, `read_input(ptr)` copies the input to
//! `ptr`, `write_output(ptr, len)` sets the result. The module exports `memory` and `run`.
//! No other host access exists: no files, network, clock or randomness.

use wasmtime::{
    Caller, Config, Engine, Error, Linker, Module, Store, StoreLimits, StoreLimitsBuilder, Trap,
};

use crate::proto::FailKind;

pub const MAX_OUTPUT: usize = 64 << 20;

pub struct Sandbox {
    engine: Engine,
    linker: Linker<Ctx>,
}

struct Ctx {
    input: Vec<u8>,
    output: Option<Vec<u8>>,
    output_too_large: bool,
    limits: StoreLimits,
}

#[derive(Debug)]
pub struct Run {
    pub output: Vec<u8>,
    pub fuel_used: u64,
}

#[derive(Debug)]
pub struct Failure {
    pub kind: FailKind,
    pub detail: String,
}

fn fail(kind: FailKind, detail: impl ToString) -> Failure {
    Failure {
        kind,
        detail: detail.to_string(),
    }
}

fn memory(caller: &mut Caller<'_, Ctx>) -> wasmtime::Result<wasmtime::Memory> {
    caller
        .get_export("memory")
        .and_then(|e| e.into_memory())
        .ok_or_else(|| Error::msg("module exports no memory"))
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
        linker
            .func_wrap("env", "input_len", |c: Caller<'_, Ctx>| {
                c.data().input.len() as i32
            })
            .and_then(|l| {
                l.func_wrap("env", "read_input", |mut c: Caller<'_, Ctx>, ptr: i32| {
                    let mem = memory(&mut c)?;
                    let (data, ctx) = mem.data_and_store_mut(&mut c);
                    let dst = data
                        .get_mut(ptr as u32 as usize..)
                        .and_then(|d| d.get_mut(..ctx.input.len()))
                        .ok_or_else(|| Error::msg("read_input out of bounds"))?;
                    dst.copy_from_slice(&ctx.input);
                    Ok(())
                })
            })
            .and_then(|l| {
                l.func_wrap(
                    "env",
                    "write_output",
                    |mut c: Caller<'_, Ctx>, ptr: i32, len: i32| {
                        let mem = memory(&mut c)?;
                        let (data, ctx) = mem.data_and_store_mut(&mut c);
                        if len as u32 as usize > MAX_OUTPUT {
                            ctx.output_too_large = true;
                            return Err(Error::msg("output too large"));
                        }
                        let start = ptr as u32 as usize;
                        let src = data
                            .get(start..start.saturating_add(len as u32 as usize))
                            .ok_or_else(|| Error::msg("write_output out of bounds"))?;
                        ctx.output = Some(src.to_vec());
                        Ok(())
                    },
                )
            })
            .map_err(|e| anyhow::anyhow!("{e}"))?;
        Ok(Sandbox { engine, linker })
    }

    /// Compile a module (binary format only: no text parser on devices).
    pub fn compile(&self, wasm: &[u8]) -> Result<Module, Failure> {
        Module::from_binary(&self.engine, wasm).map_err(|e| fail(FailKind::InvalidModule, e))
    }

    pub fn run(
        &self,
        module: &Module,
        input: Vec<u8>,
        fuel: u64,
        memory_mb: u32,
    ) -> Result<Run, Failure> {
        let limits = StoreLimitsBuilder::new()
            .memory_size(memory_mb as usize * (1 << 20))
            .instances(1)
            .build();
        let mut store = Store::new(
            &self.engine,
            Ctx {
                input,
                output: None,
                output_too_large: false,
                limits,
            },
        );
        store.limiter(|ctx| &mut ctx.limits);
        store.set_fuel(fuel).map_err(|e| fail(FailKind::Trap, e))?;
        let result = self
            .linker
            .instantiate(&mut store, module)
            .map_err(|e| (e, FailKind::InvalidModule))
            .and_then(|instance| {
                instance
                    .get_typed_func::<(), ()>(&mut store, "run")
                    .map_err(|e| (e, FailKind::InvalidModule))
            })
            .and_then(|run| run.call(&mut store, ()).map_err(|e| (e, FailKind::Trap)));
        let fuel_used = fuel - store.get_fuel().unwrap_or(0);
        if let Err((e, kind)) = result {
            let kind = if store.data().output_too_large {
                FailKind::OutputTooLarge
            } else if e.downcast_ref::<Trap>() == Some(&Trap::OutOfFuel) {
                FailKind::OutOfFuel
            } else {
                kind
            };
            return Err(fail(kind, e));
        }
        let output = store.into_data().output.unwrap_or_default();
        Ok(Run { output, fuel_used })
    }
}
