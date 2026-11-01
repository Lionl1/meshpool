//! Tasks written in Rust with the SDK (sdk/rust): built to wasm32 and run in the sandbox
//! and in a pool. Needs `rustup target add wasm32-unknown-unknown`.

use std::path::PathBuf;
use std::process::Command;
use std::sync::OnceLock;
use std::time::Duration;

use meshpool::agent::Agent;
use meshpool::client::Client;
use meshpool::coordinator::{self, Config};
use meshpool::proto::{FailKind, JobSpec, JobStatus};
use meshpool::sandbox::{RunError, Sandbox};

/// Builds the SDK examples once per test binary and returns their directory.
fn tasks() -> &'static PathBuf {
    static DIR: OnceLock<PathBuf> = OnceLock::new();
    DIR.get_or_init(|| {
        let root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        let target = root.join("target/wasm-tasks");
        let status = Command::new(env!("CARGO"))
            .args(["build", "--release", "--target", "wasm32-unknown-unknown"])
            .args(["-p", "meshpool-task", "--examples", "--features", "json"])
            .arg("--target-dir")
            .arg(&target)
            .current_dir(&root)
            .status()
            .unwrap();
        assert!(
            status.success(),
            "building SDK tasks failed; is the target installed? \
             rustup target add wasm32-unknown-unknown"
        );
        target.join("wasm32-unknown-unknown/release/examples")
    })
}

fn module(name: &str) -> Vec<u8> {
    std::fs::read(tasks().join(format!("{name}.wasm"))).unwrap()
}

fn u32s(values: &[u32]) -> Vec<u8> {
    values.iter().flat_map(|v| v.to_le_bytes()).collect()
}

#[test]
fn rust_tasks_run_deterministically_in_the_sandbox() {
    let sandbox = Sandbox::new().unwrap();
    let run = |name: &str, input: Vec<u8>| {
        let module = sandbox.compile(&module(name)).unwrap();
        sandbox.run(&module, input, 10_000_000_000, 64, false)
    };

    let primes = run("primes", u32s(&[100_000])).unwrap();
    assert_eq!(primes.output, u32s(&[9592]));
    assert_eq!(
        run("primes", u32s(&[100_000])).unwrap().fuel_used,
        primes.fuel_used
    );

    let words = run("wordcount", br#""the cat and the hat""#.to_vec()).unwrap();
    assert_eq!(words.output, br#"{"and":1,"cat":1,"hat":1,"the":2}"#);
    // Input that is not what the task expects is a deterministic failure, not a device error.
    match run("wordcount", b"not json".to_vec()) {
        Err(RunError::Failed { kind, .. }) => assert_eq!(kind, FailKind::Trap),
        other => panic!("{other:?}"),
    }

    let image = run("mandelbrot", u32s(&[64, 32, 0, 32])).unwrap();
    assert_eq!(image.output.len(), 64 * 32);
    assert_eq!(
        run("mandelbrot", u32s(&[64, 32, 0, 32])).unwrap().output,
        image.output
    );
}

/// Renders an image in strips on a pool and stitches them back together.
#[tokio::test]
async fn rust_task_renders_on_a_pool() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let cfg = Config {
        distinct_ips: false,
        ..Config::default()
    };
    tokio::spawn(coordinator::serve(listener, cfg, None));
    for name in ["a", "b"] {
        let client = Client::new(&url, None);
        let agent = Agent::connect(client, name, Duration::from_millis(100), 2)
            .await
            .unwrap();
        tokio::spawn(agent.run());
    }

    let client = Client::new(&url, None);
    let wasm = module("mandelbrot");
    let module = client.put_blob(wasm.clone()).await.unwrap();
    let (width, height, strip) = (64, 32, 8);
    let mut specs = Vec::new();
    for first in (0..height).step_by(strip as usize) {
        let input = u32s(&[width, height, first, strip]);
        specs.push(JobSpec {
            module: module.clone(),
            input: client.put_blob(input).await.unwrap(),
            fuel: 1_000_000_000,
            memory_mb: 16,
            replicas: 2,
            gpu: false,
        });
    }
    let mut image = Vec::new();
    for job in client.submit_batch(&specs).await.unwrap() {
        let view = client.wait(&job, Duration::from_secs(30)).await.unwrap();
        assert_eq!(view.status, JobStatus::Done);
        image.extend(client.output(&view).await.unwrap());
    }

    let sandbox = Sandbox::new().unwrap();
    let whole = sandbox
        .run(
            &sandbox.compile(&wasm).unwrap(),
            u32s(&[width, height, 0, height]),
            1_000_000_000,
            16,
            false,
        )
        .unwrap();
    assert_eq!(image, whole.output);
}

/// A GPU task (Collatz steps in a WGSL shader) on a pool with one GPU device. Needs the
/// `gpu` feature and a GPU; skipped (with a note) otherwise.
#[cfg(feature = "gpu")]
#[tokio::test]
async fn gpu_task_runs_on_a_gpu_device() {
    if meshpool::gpu::Gpu::open().is_none() {
        eprintln!("no GPU here: skipping");
        return;
    }
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let cfg = Config {
        distinct_ips: false,
        ..Config::default()
    };
    tokio::spawn(coordinator::serve(listener, cfg, None));
    let agent = Agent::connect(
        Client::new(&url, None),
        "gpu",
        Duration::from_millis(100),
        1,
    )
    .await
    .unwrap();
    tokio::spawn(agent.run());
    let client = Client::new(&url, None);
    assert_eq!(client.agents().await.unwrap()[0].caps.gpus.len(), 1);

    let (first, count) = (1u32, 5000u32);
    let spec = JobSpec {
        module: client.put_blob(module("collatz_gpu")).await.unwrap(),
        input: client.put_blob(u32s(&[first, count])).await.unwrap(),
        fuel: 1_000_000_000,
        memory_mb: 16,
        replicas: 1,
        gpu: true,
    };
    let job = client.submit(&spec).await.unwrap();
    let view = client.wait(&job, Duration::from_secs(30)).await.unwrap();
    let output = client.output(&view).await.unwrap();
    let steps = |mut n: u32| {
        let mut steps = 0;
        while n > 1 && steps < 10_000 {
            n = if n.is_multiple_of(2) {
                n / 2
            } else {
                n.wrapping_mul(3).wrapping_add(1)
            };
            steps += 1;
        }
        steps
    };
    let expected: Vec<u32> = (first..first + count).map(steps).collect();
    assert_eq!(output, u32s(&expected));
    assert_eq!(expected[26], 111); // 27 takes 111 steps

    // Without `gpu: true` the same module is refused, the same way on every device.
    let job = client
        .submit(&JobSpec { gpu: false, ..spec })
        .await
        .unwrap();
    let view = client.wait(&job, Duration::from_secs(30)).await.unwrap();
    assert!(matches!(
        view.outcome,
        Some(meshpool::proto::Outcome::Failed {
            kind: FailKind::InvalidModule,
            ..
        })
    ));
}
