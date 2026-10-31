use std::io::Write;
use std::path::PathBuf;
use std::time::Duration;

use anyhow::Result;
use clap::{Parser, Subcommand};

use meshpool::agent::Agent;
use meshpool::client::Client;
use meshpool::coordinator::{self, Config};
use meshpool::proto::JobSpec;

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
        #[arg(long, default_value_t = 300.0)]
        lease_timeout: f64,
    },
    /// Lend this device to the pool.
    Agent {
        #[arg(long, default_value_t = hostname())]
        name: String,
        #[arg(long, env = "MESHPOOL_AGENT_TOKEN")]
        token: Option<String>,
        #[arg(long, default_value_t = 10.0)]
        heartbeat: f64,
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
        /// Print the job id and return instead of waiting.
        #[arg(long)]
        detach: bool,
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

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    match cli.command {
        Command::Coordinator {
            listen,
            token,
            agent_token,
            agent_timeout,
            lease_timeout,
        } => {
            let listener = tokio::net::TcpListener::bind(&listen).await?;
            eprintln!("[*] coordinator on http://{}", listener.local_addr()?);
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
            };
            coordinator::serve(listener, cfg).await
        }
        Command::Agent {
            name,
            token,
            heartbeat,
        } => {
            let agent = Agent::connect(
                Client::new(&cli.url, token),
                &name,
                Duration::from_secs_f64(heartbeat),
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
        } => {
            let client = Client::new(&cli.url, token);
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
        Command::Job { id, token } => {
            let view = Client::new(&cli.url, token)
                .job(&id, Duration::ZERO)
                .await?;
            println!("{}", serde_json::to_string_pretty(&view)?);
            Ok(())
        }
        Command::Agents { token } => {
            for a in Client::new(&cli.url, token).agents().await? {
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
