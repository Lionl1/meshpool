//! Agent: lends this device to the pool. Only outbound HTTP, so it works behind NAT.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use anyhow::Result;
use reqwest::StatusCode;
use wasmtime::Module;

use crate::client::{AgentAuth, Client, status_of};
use crate::proto::*;
use crate::sandbox::Sandbox;

const LEASE_WAIT: Duration = Duration::from_secs(20);
const MODULE_CACHE: usize = 32;

pub struct Agent {
    client: Client,
    name: String,
    heartbeat: Duration,
    sandbox: Arc<Sandbox>,
    modules: HashMap<String, Module>,
    auth: Arc<Mutex<AgentAuth>>,
}

impl Agent {
    pub async fn connect(client: Client, name: &str, heartbeat: Duration) -> Result<Self> {
        let auth = client.register(name, Caps::detect()).await?;
        eprintln!("[*] joined the pool as {} ({})", auth.id, name);
        Ok(Agent {
            client,
            name: name.into(),
            heartbeat,
            sandbox: Arc::new(Sandbox::new()?),
            modules: HashMap::new(),
            auth: Arc::new(Mutex::new(auth)),
        })
    }

    fn auth(&self) -> AgentAuth {
        self.auth.lock().unwrap().clone()
    }

    /// Serve tasks until the coordinator refuses this device (bad token, banned).
    pub async fn run(mut self) -> Result<()> {
        let (client, auth, every) = (self.client.clone(), self.auth.clone(), self.heartbeat);
        let beat = tokio::spawn(async move {
            loop {
                tokio::time::sleep(every).await;
                let current = auth.lock().unwrap().clone();
                if let Err(e) = client.heartbeat(&current).await {
                    eprintln!("[!] heartbeat: {e}");
                }
            }
        });
        let result = self.serve().await;
        beat.abort();
        result
    }

    async fn serve(&mut self) -> Result<()> {
        loop {
            match self.client.lease(&self.auth(), LEASE_WAIT).await {
                Ok(Some(lease)) => self.execute(lease).await,
                Ok(None) => {}
                Err(e) => match status_of(&e) {
                    Some(StatusCode::UNAUTHORIZED | StatusCode::FORBIDDEN) => return Err(e),
                    Some(StatusCode::NOT_FOUND) => {
                        // The coordinator restarted and forgot this device: join again.
                        let auth = self.client.register(&self.name, Caps::detect()).await?;
                        *self.auth.lock().unwrap() = auth;
                    }
                    _ => {
                        eprintln!("[!] lease: {e}");
                        tokio::time::sleep(Duration::from_secs(2)).await;
                    }
                },
            }
        }
    }

    async fn execute(&mut self, lease: Lease) {
        let outcome = match self.run_task(&lease.spec).await {
            Ok(outcome) => outcome,
            Err(e) => Outcome::Abandoned {
                reason: e.to_string(),
            },
        };
        eprintln!(
            "[+] task {} -> {}",
            lease.task_id,
            outcome.vote().unwrap_or_else(|| "abandoned".into())
        );
        if let Err(e) = self
            .client
            .report(&self.auth(), &lease.task_id, &outcome)
            .await
        {
            eprintln!("[!] report of {}: {e}", lease.task_id);
        }
    }

    /// Ok(outcome) is a vote; Err means this device could not run the task.
    async fn run_task(&mut self, spec: &JobSpec) -> Result<Outcome> {
        let module = match self.module(&spec.module).await? {
            Ok(module) => module,
            Err(f) => {
                return Ok(Outcome::Failed {
                    kind: f.kind,
                    detail: f.detail,
                });
            }
        };
        let input = self.client.get_blob(&spec.input).await?;
        let (sandbox, fuel, memory_mb) = (self.sandbox.clone(), spec.fuel, spec.memory_mb);
        let run = tokio::task::spawn_blocking(move || sandbox.run(&module, input, fuel, memory_mb))
            .await?;
        Ok(match run {
            Ok(run) => Outcome::Ok {
                output: self.client.put_blob(run.output).await?,
                fuel_used: run.fuel_used,
            },
            Err(f) => Outcome::Failed {
                kind: f.kind,
                detail: f.detail,
            },
        })
    }

    async fn module(&mut self, hash: &str) -> Result<Result<Module, crate::sandbox::Failure>> {
        if let Some(module) = self.modules.get(hash) {
            return Ok(Ok(module.clone()));
        }
        let wasm = self.client.get_blob(hash).await?;
        let sandbox = self.sandbox.clone();
        let compiled = tokio::task::spawn_blocking(move || sandbox.compile(&wasm)).await?;
        if let Ok(module) = &compiled {
            if self.modules.len() >= MODULE_CACHE {
                self.modules.clear();
            }
            self.modules.insert(hash.into(), module.clone());
        }
        Ok(compiled)
    }
}
