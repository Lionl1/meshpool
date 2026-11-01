use std::io::Write;
use std::path::PathBuf;
use std::time::Duration;

use anyhow::Result;
use clap::{Parser, Subcommand};

use meshpool::agent::Agent;
use meshpool::client::Client;
use meshpool::coordinator::{self, Config};
use meshpool::proto::{Caps, JobSpec};

#[derive(Parser)]
#[command(version, about = "Pool the compute of untrusted devices")]
struct Cli {
    /// Coordinator URL.
    #[arg(
        long,
        env = "MESHPOOL_URL",
        default_value = "http://127.0.0.1:7700",
        global = true
    )]
    url: String,
    /// Extra CA certificate (PEM) to trust, e.g. a pool's self-signed certificate.
    #[arg(long, env = "MESHPOOL_CA_CERT", global = true)]
    ca_cert: Option<PathBuf>,
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Run the coordinator.
    Coordinator {
        #[arg(long, default_value = "127.0.0.1:7700")]
        listen: String,
        /// Token clients must present (default: open).
        #[arg(long, env = "MESHPOOL_TOKEN")]
        token: Option<String>,
        /// Token devices must present to join (default: anyone may join).
        #[arg(long, env = "MESHPOOL_AGENT_TOKEN")]
        agent_token: Option<String>,
        #[arg(long, default_value_t = 30.0)]
        agent_timeout: f64,
        /// Seconds a lease survives without being renewed by the device's heartbeats.
        #[arg(long, default_value_t = 60.0)]
        lease_timeout: f64,
        /// Let devices on one IP address take replicas of the same job and do not ban
        /// addresses (for a pool on one machine or one LAN behind a single address).
        #[arg(long)]
        allow_same_ip: bool,
        /// Save the pool here (state.json and blobs/) and reload it on start; without it the
        /// pool lives in memory only.
        #[arg(long, env = "MESHPOOL_DATA_DIR")]
        data_dir: Option<PathBuf>,
        /// Serve HTTPS with this certificate chain (PEM); needs --tls-key.
        #[arg(long, env = "MESHPOOL_TLS_CERT", requires = "tls_key")]
        tls_cert: Option<PathBuf>,
        /// Private key (PEM) for --tls-cert.
        #[arg(long, env = "MESHPOOL_TLS_KEY", requires = "tls_cert")]
        tls_key: Option<PathBuf>,
    },
    /// Lend this device to the pool.
    Agent {
        /// Default: the host name.
        #[arg(long)]
        name: Option<String>,
        #[arg(long, env = "MESHPOOL_AGENT_TOKEN")]
        token: Option<String>,
        #[arg(long, default_value_t = 10.0)]
        heartbeat: f64,
        /// Tasks to run at once (default: one per CPU core).
        #[arg(long)]
        slots: Option<u32>,
    },
    /// Run a WebAssembly task (.wasm or .wat) and print its output.
    Submit {
        module: PathBuf,
        /// Input as a string (default: empty).
        #[arg(long, conflicts_with = "input_file")]
        input: Option<String>,
        #[arg(long)]
        input_file: Option<PathBuf>,
        #[arg(long, default_value_t = 2)]
        replicas: u32,
        #[arg(long, default_value_t = 10_000_000_000)]
        fuel: u64,
        #[arg(long, default_value_t = 64)]
        memory_mb: u32,
        /// The task calls `gpu_run`: run it only on devices that lend a GPU.
        #[arg(long)]
        gpu: bool,
        /// Print the job id and return instead of waiting.
        #[arg(long)]
        detach: bool,
        #[arg(long, env = "MESHPOOL_TOKEN")]
        token: Option<String>,
    },
    /// Check a live pool: run jobs with known answers, verify them, show what each device did.
    Check {
        #[arg(long, default_value_t = 64)]
        tasks: usize,
        /// Each job counts the primes below n (+ its index); larger n, longer jobs.
        #[arg(long, default_value_t = 1_000_000)]
        n: u32,
        #[arg(long, default_value_t = 2)]
        replicas: u32,
        #[arg(long, env = "MESHPOOL_TOKEN")]
        token: Option<String>,
    },
    /// Show a job.
    Job {
        id: String,
        #[arg(long, env = "MESHPOOL_TOKEN")]
        token: Option<String>,
    },
    /// List devices in the pool.
    Agents {
        #[arg(long, env = "MESHPOOL_TOKEN")]
        token: Option<String>,
    },
}

fn hostname() -> String {
    std::env::var("HOSTNAME")
        .ok()
        .or_else(|| {
            std::process::Command::new("hostname")
                .output()
                .ok()
                .and_then(|o| String::from_utf8(o.stdout).ok())
        })
        .map(|h| h.trim().to_string())
        .filter(|h| !h.is_empty())
        .unwrap_or_else(|| "device".into())
}

/// Ctrl-C, or SIGTERM (systemd, docker stop).
async fn shutdown_signal() {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{SignalKind, signal};
        let mut term = signal(SignalKind::terminate()).expect("SIGTERM handler");
        tokio::select! {
            _ = tokio::signal::ctrl_c() => {}
            _ = term.recv() => {}
        }
    }
    #[cfg(not(unix))]
    let _ = tokio::signal::ctrl_c().await;
    eprintln!("[*] shutting down");
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    let ca = cli.ca_cert.as_ref().map(std::fs::read).transpose()?;
    let client = |token| Client::with_root_cert(&cli.url, token, ca.as_deref());
    match cli.command {
        Command::Coordinator {
            listen,
            token,
            agent_token,
            agent_timeout,
            lease_timeout,
            allow_same_ip,
            tls_cert,
            tls_key,
            data_dir,
        } => {
            let listener = tokio::net::TcpListener::bind(&listen).await?;
            let addr = listener.local_addr()?;
            let tls = match (tls_cert, tls_key) {
                (Some(cert), Some(key)) => Some(coordinator::Tls {
                    cert_pem: std::fs::read(cert)?,
                    key_pem: std::fs::read(key)?,
                }),
                _ => None,
            };
            let scheme = if tls.is_some() { "https" } else { "http" };
            eprintln!("[*] coordinator on {scheme}://{addr}");
            if tls.is_none() && !addr.ip().is_loopback() {
                eprintln!(
                    "[!] plain HTTP: device secrets and tokens are readable on the network; \
                     use --tls-cert/--tls-key or a TLS proxy"
                );
            }
            if agent_token.is_none() {
                eprintln!(
                    "[*] open pool: any device may join (set MESHPOOL_AGENT_TOKEN to restrict)"
                );
            }
            let cfg = Config {
                client_token: token,
                agent_token,
                agent_timeout: Duration::from_secs_f64(agent_timeout),
                lease_timeout: Duration::from_secs_f64(lease_timeout),
                distinct_ips: !allow_same_ip,
                data_dir,
            };
            coordinator::serve_until(listener, cfg, tls, shutdown_signal()).await
        }
        Command::Agent {
            name,
            token,
            heartbeat,
            slots,
        } => {
            let agent = Agent::connect(
                client(token)?,
                &name.unwrap_or_else(hostname),
                Duration::from_secs_f64(heartbeat),
                slots.unwrap_or_else(|| Caps::detect().cpus),
            )
            .await?;
            agent.run().await
        }
        Command::Submit {
            module,
            input,
            input_file,
            replicas,
            fuel,
            memory_mb,
            detach,
            token,
            gpu,
        } => {
            let client = client(token)?;
            let input = match input_file {
                Some(path) => std::fs::read(path)?,
                None => input.unwrap_or_default().into_bytes(),
            };
            let spec = JobSpec {
                module: client.put_blob(meshpool::load_module(&module)?).await?,
                input: client.put_blob(input).await?,
                fuel,
                memory_mb,
                replicas,
                gpu,
            };
            let id = client.submit(&spec).await?;
            if detach {
                println!("{id}");
                return Ok(());
            }
            let view = client.wait(&id, Duration::from_secs(24 * 3600)).await?;
            let output = client.output(&view).await?;
            eprintln!("[v] job {id}: {} vote(s)", view.votes.len());
            std::io::stdout().write_all(&output)?;
            Ok(())
        }
        Command::Check {
            tasks,
            n,
            replicas,
            token,
        } => {
            let report = meshpool::check::check(&client(token)?, tasks, n, replicas).await?;
            print!("{report}");
            Ok(())
        }
        Command::Job { id, token } => {
            let view = client(token)?.job(&id, Duration::ZERO).await?;
            println!("{}", serde_json::to_string_pretty(&view)?);
            Ok(())
        }
        Command::Agents { token } => {
            for a in client(token)?.agents().await? {
                let state = if a.banned {
                    "banned"
                } else if a.online {
                    "online"
                } else {
                    "offline"
                };
                println!(
                    "{}  {:<16} {:<7} {}/{} {} cpus  strikes {}  credits {}",
                    a.id, a.name, state, a.caps.os, a.caps.arch, a.caps.cpus, a.strikes, a.credits
                );
            }
            Ok(())
        }
    }
}
