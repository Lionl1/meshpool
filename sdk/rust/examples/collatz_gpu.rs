//! Collatz stopping times on the GPU: one shader invocation per number. Input: first number
//! and count as u32 little endian; output: the steps for each number (u32 little endian).
//! Submit with `gpu: true`.

// The GPU call exists on wasm32 only; on the host just the helpers are tested.
#![cfg_attr(not(target_arch = "wasm32"), allow(dead_code))]

const SHADER: &str = "
@group(0) @binding(0) var<storage, read_write> data: array<u32>;

@compute @workgroup_size(64)
fn main(@builtin(global_invocation_id) id: vec3<u32>) {
    if id.x >= arrayLength(&data) { return; }
    var n = data[id.x];
    var steps = 0u;
    while n > 1u && steps < 10000u {
        if n % 2u == 0u { n = n / 2u; } else { n = 3u * n + 1u; }
        steps = steps + 1u;
    }
    data[id.x] = steps;
}";

fn numbers(input: &[u8]) -> Vec<u8> {
    let arg = |i: usize| u32::from_le_bytes(input[4 * i..4 * i + 4].try_into().unwrap());
    let (first, count) = (arg(0), arg(1));
    (first..first + count).flat_map(u32::to_le_bytes).collect()
}

#[cfg(target_arch = "wasm32")]
fn collatz(input: &[u8]) -> Vec<u8> {
    let mut data = numbers(input);
    let groups = (data.len() as u32 / 4).div_ceil(64);
    meshpool_task::gpu_run(SHADER, &mut data, [groups, 1, 1]);
    data
}

#[cfg(target_arch = "wasm32")]
meshpool_task::task!(collatz);

#[test]
fn numbers_and_cpu_reference() {
    let input: Vec<u8> = [27u32, 2].iter().flat_map(|v| v.to_le_bytes()).collect();
    assert_eq!(
        numbers(&input),
        [27u32, 28]
            .iter()
            .flat_map(|v| v.to_le_bytes())
            .collect::<Vec<_>>()
    );
    assert!(SHADER.contains("fn main"));
}
