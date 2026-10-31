//! Agent: lends this device to the pool. Only outbound HTTP, so it works behind NAT.
//! Runs `slots` tasks at once (default: one per CPU core).

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use anyhow::Result;
use reqwest::StatusCode;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use wasmtime::Module;

use crate::client::{AgentAuth, Client, status_of};
use crate::proto::*;
use crate::sandbox::{Run, RunError, Sandbox};

const LEASE_WAIT: Duration = Duration::from_secs(20);
const MODULE_CACHE: usize = 32;
/// Inputs up to this size are kept (a map over one input runs many tasks on it).
const INPUT_CACHE_ITEM: usize = 1 << 20;
const INPUT_CACHE: usize = 64;
const ABANDON_BACKOFF: Duration = Duration::from_secs(1);

pub struct Agent {
    client: Client,
    name: String,
    caps: Caps,
    heartbeat: Duration,
    sandbox: Sandbox,
    modules: Mutex<HashMap<String, Module>>,
    inputs: Mutex<HashMap<String, Arc<Vec<u8>>>>,
    auth: tokio::sync::Mutex<AgentAuth>,
    /// Leases in progress: renewed by every heartbeat.
    working_on: Mutex<HashSet<String>>,
}

/// Keep a small cache from growing without bound (dropping everything is enough here).
fn remember<V>(cache: &Mutex<HashMap<String, V>>, limit: usize, key: &str, value: V) {
    let mut cache = cache.lock().unwrap();
    if cache.len() >= limit {
        cache.clear();
    }
    cache.insert(key.into(), value);
}

impl Agent {
    pub async fn connect(
        client: Client,
        name: &str,
        heartbeat: Duration,
        slots: u32,
    ) -> Result<Arc<Self>> {
        let caps = Caps {
            slots: slots.max(1),
            ..Caps::detect()
        };
        let auth = client.register(name, caps.clone()).await?;
        eprintln!(
            "[*] joined the pool as {} ({name}, {} slots)",
            auth.id, caps.slots
        );
        Ok(Arc::new(Agent {
            client,
            name: name.into(),
            caps,
            heartbeat,
            sandbox: Sandbox::new()?,
            modules: Mutex::default(),
            inputs: Mutex::default(),
            auth: tokio::sync::Mutex::new(auth),
            working_on: Mutex::default(),
        }))
    }

    async fn auth(&self) -> AgentAuth {
        self.auth.lock().await.clone()
    }

    /// Serve tasks until the coordinator refuses this device (bad token, banned).
    ///
    /// One loop leases as many tasks as there are free slots in a single request; each
    /// task runs on its own and frees its slot before reporting, so on a slow network
    /// the next task starts while the previous report is still on its way.
    pub async fn run(self: Arc<Self>) -> Result<()> {
        let slots = Arc::new(Semaphore::new(self.caps.slots as usize));
        let beat = tokio::spawn(self.clone().beat());
        let result = self.lease_loop(&slots).await;
        beat.abort();
        result
    }

    async fn lease_loop(self: &Arc<Self>, slots: &Arc<Semaphore>) -> Result<()> {
        loop {
            let first = slots.clone().acquire_owned().await?;
            let free = 1 + slots.available_permits();
            let auth = self.auth().await;
            let leases = match self.client.lease(&auth, LEASE_WAIT, free).await {
                Ok(leases) => leases,
                Err(e) => {
                    match status_of(&e) {
                        Some(StatusCode::UNAUTHORIZED | StatusCode::FORBIDDEN) => return Err(e),
                        Some(StatusCode::NOT_FOUND) => self.rejoin(&auth).await?,
                        _ => {
                            eprintln!("[!] lease: {e}");
                            tokio::time::sleep(Duration::from_secs(2)).await;
                        }
                    }
                    continue;
                }
            };
            let mut first = Some(first);
            for lease in leases {
                let slot = match first.take() {
                    Some(slot) => slot,
                    None => slots.clone().try_acquire_owned()?, // counted as free above
                };
                tokio::spawn(self.clone().execute(auth.clone(), lease, slot));
            }
        }
    }

    async fn beat(self: Arc<Self>) {
        loop {
            tokio::time::sleep(self.heartbeat).await;
            let working_on = self.working_on.lock().unwrap().iter().cloned().collect();
            if let Err(e) = self.client.heartbeat(&self.auth().await, working_on).await {
                eprintln!("[!] heartbeat: {e}");
            }
        }
    }

    /// The coordinator restarted and forgot this device: join again (once for all slots).
    async fn rejoin(&self, stale: &AgentAuth) -> Result<()> {
        let mut auth = self.auth.lock().await;
        if auth.id == stale.id {
            *auth = self.client.register(&self.name, self.caps.clone()).await?;
        }
        Ok(())
    }

    async fn execute(self: Arc<Self>, auth: AgentAuth, lease: Lease, slot: OwnedSemaphorePermit) {
        let task = lease.task_id.clone();
        self.working_on.lock().unwrap().insert(task.clone());
        let result = self.run_task(&auth, &lease).await;
        drop(slot); // the next task may start while this one is reported
        let reported = match result {
            Ok(Ok(run)) => {
                eprintln!("[+] task {task} -> ok, {} fuel", run.fuel_used);
                self.client
                    .report_output(&auth, &task, run.output, run.fuel_used)
                    .await
            }
            Ok(Err(error)) => {
                let outcome = Self::outcome(error);
                let vote = outcome.vote();
                eprintln!(
                    "[+] task {task} -> {}",
                    vote.as_deref().unwrap_or("given back")
                );
                self.client.report(&auth, &task, &outcome).await
            }
            Err(e) => {
                eprintln!("[!] task {task} given back: {e}");
                let outcome = Outcome::Abandoned {
                    reason: e.to_string(),
                };
                tokio::time::sleep(ABANDON_BACKOFF).await; // do not spin on a broken task
                self.client.report(&auth, &task, &outcome).await
            }
        };
        if let Err(e) = reported {
            eprintln!("[!] report of {task}: {e}");
        }
        self.working_on.lock().unwrap().remove(&task);
    }

    /// The run, a deterministic failure (Ok(Err)), or Err if this device could not run it.
    async fn run_task(
        self: &Arc<Self>,
        auth: &AgentAuth,
        lease: &Lease,
    ) -> Result<Result<Run, RunError>> {
        let spec = &lease.spec;
        let (module, input) = tokio::try_join!(
            self.module(auth, &spec.module),
            self.input(auth, &spec.input)
        )?;
        let module = match module {
            Ok(module) => module,
            Err(e) => return Ok(Err(e)),
        };
        let input = Arc::unwrap_or_clone(input);
        let (fuel, memory_mb) = (spec.fuel, spec.memory_mb);
        let this = self.clone();
        let run =
            tokio::task::spawn_blocking(move || this.sandbox.run(&module, input, fuel, memory_mb));
        Ok(run.await?)
    }

    fn outcome(error: RunError) -> Outcome {
        match error {
            RunError::Failed {
                kind,
                fuel_used,
                detail,
            } => Outcome::Failed {
                kind,
                fuel_used,
                detail,
            },
            RunError::Host(reason) => Outcome::Abandoned { reason },
        }
    }

    async fn input(&self, auth: &AgentAuth, hash: &str) -> Result<Arc<Vec<u8>>> {
        if let Some(input) = self.inputs.lock().unwrap().get(hash) {
            return Ok(input.clone());
        }
        let input = Arc::new(self.client.get_blob_as(auth, hash).await?);
        if input.len() <= INPUT_CACHE_ITEM {
            remember(&self.inputs, INPUT_CACHE, hash, input.clone());
        }
        Ok(input)
    }

    async fn module(
        self: &Arc<Self>,
        auth: &AgentAuth,
        hash: &str,
    ) -> Result<Result<Module, RunError>> {
        if let Some(module) = self.modules.lock().unwrap().get(hash) {
            return Ok(Ok(module.clone()));
        }
        let wasm = self.client.get_blob_as(auth, hash).await?;
        let this = self.clone();
        let compiled = tokio::task::spawn_blocking(move || this.sandbox.compile(&wasm)).await?;
        if let Ok(module) = &compiled {
            remember(&self.modules, MODULE_CACHE, hash, module.clone());
        }
        Ok(compiled)
    }
}
