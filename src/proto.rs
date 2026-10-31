//! Wire types shared by the coordinator, agents and clients (JSON over HTTP).

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

/// Bumped when agents and coordinators can no longer talk to each other. Fuel accounting
/// depends on the wasmtime version, so it is part of it: replicas must count fuel alike.
pub const PROTOCOL: &str = "meshpool/0.2+wasmtime50";

pub fn blob_hash(data: &[u8]) -> String {
    hex::encode(Sha256::digest(data))
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub struct JobSpec {
    /// Blob hash of a WebAssembly module (binary format).
    pub module: String,
    /// Blob hash of the task input.
    pub input: String,
    #[serde(default = "default_fuel")]
    pub fuel: u64,
    #[serde(default = "default_memory_mb")]
    pub memory_mb: u32,
    /// Devices that must run the task; a result needs ⌊R/2⌋+1 of them to agree.
    #[serde(default = "default_replicas")]
    pub replicas: u32,
}

fn default_fuel() -> u64 {
    10_000_000_000
}
fn default_memory_mb() -> u32 {
    64
}
fn default_replicas() -> u32 {
    2
}

/// Failures that every honest device reproduces, so replicas vote on them like on outputs.
#[derive(Serialize, Deserialize, Clone, Copy, Debug, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum FailKind {
    InvalidModule,
    OutOfFuel,
    Trap,
    OutputTooLarge,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
#[serde(tag = "status", rename_all = "snake_case")]
pub enum Outcome {
    Ok {
        output: String,
        fuel_used: u64,
    },
    Failed {
        kind: FailKind,
        fuel_used: u64,
        detail: String,
    },
    /// The device could not run the task (fetch failed, shutting down): re-queued, no vote.
    Abandoned {
        reason: String,
    },
}

impl Outcome {
    /// What replicas must agree on; None for an abandoned task.
    /// Fuel the task burned on the device (what agreeing devices are credited).
    pub fn fuel_used(&self) -> u64 {
        match self {
            Outcome::Ok { fuel_used, .. } | Outcome::Failed { fuel_used, .. } => *fuel_used,
            Outcome::Abandoned { .. } => 0,
        }
    }

    pub fn vote(&self) -> Option<String> {
        match self {
            Outcome::Ok { output, fuel_used } => Some(format!("ok:{output}:{fuel_used}")),
            // The detail (trap message) is not voted on: it may differ between devices.
            Outcome::Failed {
                kind, fuel_used, ..
            } => Some(format!("failed:{kind:?}:{fuel_used}")),
            Outcome::Abandoned { .. } => None,
        }
    }
}

#[derive(Serialize, Deserialize, Clone, Copy, Debug, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum JobStatus {
    Pending,
    Done,
    Failed,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct Vote {
    pub agent_id: String,
    pub vote: String,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct JobView {
    pub id: String,
    pub status: JobStatus,
    pub spec: JobSpec,
    /// The agreed outcome (Ok or Failed) once the job is done.
    pub outcome: Option<Outcome>,
    pub error: Option<String>,
    pub votes: Vec<Vote>,
}

#[derive(Serialize, Deserialize, Clone, Debug, Default)]
pub struct Caps {
    pub cpus: u32,
    /// Tasks the device runs at once.
    pub slots: u32,
    pub os: String,
    pub arch: String,
    pub gpus: Vec<String>,
}

impl Caps {
    pub fn detect() -> Self {
        Caps {
            cpus: std::thread::available_parallelism().map_or(1, |n| n.get() as u32),
            slots: 1,
            os: std::env::consts::OS.into(),
            arch: std::env::consts::ARCH.into(),
            gpus: Vec::new(),
        }
    }
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct Register {
    pub name: String,
    pub caps: Caps,
    pub protocol: String,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct Registered {
    pub agent_id: String,
    pub secret: String,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct Lease {
    pub task_id: String,
    pub job_id: String,
    pub spec: JobSpec,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct AgentView {
    pub id: String,
    pub name: String,
    pub caps: Caps,
    pub online: bool,
    pub strikes: u32,
    pub banned: bool,
    /// Fuel of tasks this device ran and the quorum confirmed.
    pub credits: u64,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct Created {
    pub id: String,
}

/// Sent with every heartbeat: the tasks the device is still working on (their leases are
/// extended, so a long task is never taken away from a live device).
#[derive(Serialize, Deserialize, Clone, Debug, Default)]
pub struct Heartbeat {
    pub tasks: Vec<String>,
}
