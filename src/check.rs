//! `meshpool check`: run a workload with known answers on a live pool and report what each
//! device did. Meant for trying a pool of real devices on real networks.

use std::collections::BTreeMap;
use std::fmt;
use std::time::{Duration, Instant};

use anyhow::{Result, ensure};

use crate::client::Client;
use crate::proto::{AgentView, JobSpec, JobStatus};

const PRIMES: &str = include_str!("../examples/tasks/primes.wat");

pub struct Report {
    pub tasks: usize,
    pub replicas: u32,
    pub elapsed: Duration,
    /// Replicas each device ran for decided jobs (agreeing or not), by device id.
    pub votes: BTreeMap<String, usize>,
    pub agents: Vec<AgentView>,
}

fn count_primes(n: u32) -> u32 {
    (2..n)
        .filter(|&i| (2u32..).take_while(|d| d * d <= i).all(|d| i % d != 0))
        .count() as u32
}

/// Submit `tasks` jobs counting the primes below `n` (each with its own input), wait for all
/// and check every output against the answer computed here.
pub async fn check(client: &Client, tasks: usize, n: u32, replicas: u32) -> Result<Report> {
    let module = client.put_blob(wat::parse_str(PRIMES)?).await?;
    // Distinct inputs, so every job is real work; answers are computed incrementally.
    let limits: Vec<u32> = (0..tasks as u32).map(|i| n + i).collect();
    let mut expected = Vec::with_capacity(tasks);
    let mut count = count_primes(n);
    for &limit in &limits {
        expected.push(count);
        if (2..).take_while(|d| d * d <= limit).all(|d| limit % d != 0) && limit >= 2 {
            count += 1;
        }
    }
    let start = Instant::now();
    let mut specs = Vec::with_capacity(tasks);
    for limit in &limits {
        specs.push(JobSpec {
            module: module.clone(),
            input: client.put_blob(limit.to_le_bytes().to_vec()).await?,
            fuel: u64::MAX / 2,
            memory_mb: 1,
            replicas,
        });
    }
    let ids = client.submit_batch(&specs).await?;
    // The jobs run in parallel on the pool; waiting for them in order costs nothing extra.
    let mut views = Vec::with_capacity(ids.len());
    for id in &ids {
        views.push(client.wait(id, Duration::from_secs(3600)).await?);
    }
    let elapsed = start.elapsed();
    let mut votes = BTreeMap::new();
    for (view, want) in views.iter().zip(&expected) {
        ensure!(
            view.status == JobStatus::Done,
            "job {} did not finish: {:?} {}",
            view.id,
            view.status,
            view.error.as_deref().unwrap_or("")
        );
        let output = client.output(view).await?;
        ensure!(
            output == want.to_le_bytes(),
            "job {}: the pool agreed on a wrong answer",
            view.id
        );
        for vote in &view.votes {
            *votes.entry(vote.agent_id.clone()).or_insert(0) += 1;
        }
    }
    Ok(Report {
        tasks,
        replicas,
        elapsed,
        votes,
        agents: client.agents().await?,
    })
}

impl fmt::Display for Report {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let secs = self.elapsed.as_secs_f64();
        writeln!(
            f,
            "[v] {} jobs × {} replicas, every answer right, in {secs:.2} s ({:.1} jobs/s)",
            self.tasks,
            self.replicas,
            self.tasks as f64 / secs
        )?;
        writeln!(
            f,
            "{:<34} {:<16} {:<8} {:>5} {:>7}  os/arch, gpu",
            "device", "name", "state", "ran", "strikes"
        )?;
        for a in &self.agents {
            let state = if a.banned {
                "banned"
            } else if a.online {
                "online"
            } else {
                "offline"
            };
            writeln!(
                f,
                "{:<34} {:<16} {:<8} {:>5} {:>7}  {}/{}{}",
                a.id,
                a.name,
                state,
                self.votes.get(&a.id).copied().unwrap_or(0),
                a.strikes,
                a.caps.os,
                a.caps.arch,
                a.caps
                    .gpus
                    .first()
                    .map(|g| format!(", {g}"))
                    .unwrap_or_default()
            )?;
        }
        Ok(())
    }
}
