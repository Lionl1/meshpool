//! Benchmark client: submits many jobs to a running pool and times them.
//!
//!   cargo run --release --example bench -- --kind primes --tasks 32 --n 3000000
//!   cargo run --release --example bench -- --local --n 3000000   # native vs sandbox, no network

use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{Result, ensure};
use clap::Parser;
use meshpool::client::Client;
use meshpool::proto::JobSpec;
use meshpool::sandbox::Sandbox;
use tokio::sync::Semaphore;

#[derive(Parser)]
struct Args {
    #[arg(long, default_value = "http://127.0.0.1:7700")]
    url: String,
    /// noop (reverse 16 bytes: scheduling overhead) or primes (CPU-bound).
    #[arg(long, default_value = "noop")]
    kind: String,
    #[arg(long, default_value_t = 1000)]
    tasks: usize,
    /// Primes below n, per task.
    #[arg(long, default_value_t = 2_000_000)]
    n: u32,
    #[arg(long, default_value_t = 1)]
    replicas: u32,
    #[arg(long, default_value_t = 256)]
    concurrency: usize,
    /// Compare native Rust with the WASM sandbox on this machine, without a pool.
    #[arg(long)]
    local: bool,
}

fn count_primes(n: u32) -> u32 {
    (2..n)
        .filter(|&i| (2u32..).take_while(|d| d * d <= i).all(|d| i % d != 0))
        .count() as u32
}

fn module(name: &str) -> Result<Vec<u8>> {
    meshpool::load_module(
        &Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("examples/tasks")
            .join(name),
    )
}

fn local(n: u32) -> Result<()> {
    let start = Instant::now();
    let expected = count_primes(n);
    let native = start.elapsed();
    let sandbox = Sandbox::new()?;
    let compiled = sandbox
        .compile(&module("primes.wat")?)
        .map_err(|f| anyhow::anyhow!(f.detail))?;
    let start = Instant::now();
    let run = sandbox
        .run(&compiled, n.to_le_bytes().to_vec(), u64::MAX, 16)
        .map_err(|f| anyhow::anyhow!(f.detail))?;
    let wasm = start.elapsed();
    ensure!(run.output == expected.to_le_bytes(), "wrong count");
    println!(
        r#"{{"kind":"local","n":{n},"native_s":{:.3},"wasm_s":{:.3},"overhead":{:.2},"fuel":{}}}"#,
        native.as_secs_f64(),
        wasm.as_secs_f64(),
        wasm.as_secs_f64() / native.as_secs_f64(),
        run.fuel_used
    );
    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();
    if args.local {
        return local(args.n);
    }
    let client = Client::new(&args.url, std::env::var("MESHPOOL_TOKEN").ok());
    let (wasm, input, expected) = match args.kind.as_str() {
        "primes" => (
            module("primes.wat")?,
            args.n.to_le_bytes().to_vec(),
            count_primes(args.n).to_le_bytes().to_vec(),
        ),
        _ => (
            module("reverse.wat")?,
            b"0123456789abcdef".to_vec(),
            b"fedcba9876543210".to_vec(),
        ),
    };
    let spec = JobSpec {
        module: client.put_blob(wasm).await?,
        input: client.put_blob(input).await?,
        fuel: u64::MAX / 2,
        memory_mb: 16,
        replicas: args.replicas,
    };
    let limit = Arc::new(Semaphore::new(args.concurrency));
    let start = Instant::now();
    let mut jobs = tokio::task::JoinSet::new();
    for _ in 0..args.tasks {
        let (client, spec, limit, expected) = (
            client.clone(),
            spec.clone(),
            limit.clone(),
            expected.clone(),
        );
        jobs.spawn(async move {
            let _permit = limit.acquire_owned().await?;
            let id = client.submit(&spec).await?;
            let view = client.wait(&id, Duration::from_secs(3600)).await?;
            ensure!(
                client.output(&view).await? == expected,
                "job {id}: wrong output"
            );
            Ok::<_, anyhow::Error>(())
        });
    }
    let mut ok = 0;
    while let Some(done) = jobs.join_next().await {
        match done? {
            Ok(()) => ok += 1,
            Err(e) => eprintln!("[!] {e}"),
        }
    }
    let secs = start.elapsed().as_secs_f64();
    println!(
        r#"{{"system":"meshpool","kind":"{}","tasks":{},"replicas":{},"ok":{ok},"seconds":{secs:.3},"tasks_per_s":{:.1}}}"#,
        args.kind,
        args.tasks,
        args.replicas,
        args.tasks as f64 / secs
    );
    Ok(())
}
