//! Write meshpool tasks as ordinary Rust functions.
//!
//! ```ignore
//! fn reverse(input: &[u8]) -> Vec<u8> {
//!     input.iter().rev().copied().collect()
//! }
//! meshpool_task::task!(reverse);
//! ```
//!
//! Build the crate as a `cdylib` with `cargo build --release --target wasm32-unknown-unknown`
//! and submit the `.wasm` file. The macros emit the task entry point on `wasm32` only, so on
//! the host the same functions can be unit-tested as usual.
//!
//! Replicas must agree bit for bit, so a task must be deterministic: use `BTreeMap` rather
//! than iterating a `HashMap`, and derive any randomness from the input. Floats are fine
//! (NaNs are canonicalised by the sandbox). A panic fails the task with a `Trap` vote.

#[cfg(target_arch = "wasm32")]
mod host {
    #[link(wasm_import_module = "env")]
    unsafe extern "C" {
        fn input_len() -> i32;
        fn read_input(ptr: *mut u8);
        fn write_output(ptr: *const u8, len: i32);
        fn gpu_run(
            shader_ptr: *const u8,
            shader_len: i32,
            data_ptr: *mut u8,
            data_len: i32,
            x: u32,
            y: u32,
            z: u32,
        );
    }

    pub fn input() -> Vec<u8> {
        // SAFETY: the host writes exactly `input_len()` bytes to the buffer.
        unsafe {
            let mut data = vec![0u8; input_len() as u32 as usize];
            read_input(data.as_mut_ptr());
            data
        }
    }

    pub fn output(data: &[u8]) {
        // SAFETY: the host copies `data` before returning.
        unsafe { write_output(data.as_ptr(), data.len() as i32) }
    }

    pub fn gpu(shader: &str, data: &mut [u8], groups: [u32; 3]) {
        // SAFETY: the host reads the shader and replaces `data` in place, both in bounds.
        unsafe {
            gpu_run(
                shader.as_ptr(),
                shader.len() as i32,
                data.as_mut_ptr(),
                data.len() as i32,
                groups[0],
                groups[1],
                groups[2],
            )
        }
    }
}

/// Run the WGSL compute shader `shader` on the device's GPU: its `@compute fn main` runs
/// over `groups` workgroups with `data` bound as
/// `@group(0) @binding(0) var<storage, read_write>`, and `data` is replaced by the result.
///
/// Submit the job with `gpu: true`. The shader may use integers and booleans only (GPU
/// floats differ between vendors, so replicas could not agree); `data` is 4 bytes to
/// 128 MiB, a multiple of 4. A shader that breaks these rules fails the task.
#[cfg(target_arch = "wasm32")]
pub fn gpu_run(shader: &str, data: &mut [u8], groups: [u32; 3]) {
    host::gpu(shader, data, groups)
}

#[cfg(target_arch = "wasm32")]
#[doc(hidden)]
pub use host::{input as __input, output as __output};

#[cfg(feature = "json")]
#[doc(hidden)]
pub use serde_json as __json;

/// Turn `fn(&[u8]) -> impl AsRef<[u8]>` (a function or a closure) into the task.
#[macro_export]
macro_rules! task {
    ($f:expr) => {
        #[cfg(target_arch = "wasm32")]
        #[unsafe(no_mangle)]
        pub extern "C" fn run() {
            let input = $crate::__input();
            let output = ($f)(&input[..]);
            $crate::__output(::core::convert::AsRef::<[u8]>::as_ref(&output));
        }
        // On the host the entry point is left out; keep the task function "used".
        #[cfg(not(target_arch = "wasm32"))]
        const _: () = {
            let _ = $f;
        };
    };
}

/// Turn `fn(T) -> U` into the task, with `T` and `U` (de)serialised as JSON. Input that does
/// not parse as `T` fails the task. Needs the `json` feature.
#[cfg(feature = "json")]
#[macro_export]
macro_rules! task_json {
    ($f:expr) => {
        $crate::task!(|input: &[u8]| -> ::std::vec::Vec<u8> {
            let value = $crate::__json::from_slice(input).expect("input does not match the task");
            $crate::__json::to_vec(&($f)(value)).expect("output is not serialisable")
        });
    };
}
