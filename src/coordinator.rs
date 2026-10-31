//! Coordinator: job queue, task leases, quorum voting, strikes, credits and the blob store.
//! State lives in memory; `Pool` holds the logic (synchronous, unit-tested), the HTTP layer
//! below only translates requests.

use std::collections::{HashMap, HashSet, VecDeque};
use std::net::{IpAddr, SocketAddr};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use axum::body::Bytes;
use axum::extract::{ConnectInfo, DefaultBodyLimit, Path, Query, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use serde::Deserialize;
use tokio::net::TcpListener;
use tokio::sync::Notify;

use crate::proto::*;
use crate::sandbox::MAX_OUTPUT;

pub const MAX_STRIKES: u32 = 3;
pub const EXTRA_REPLICAS: u32 = 2; // added one by one when replicas disagree
pub const MAX_REPLICAS: u32 = 7;
pub const MAX_MEMORY_MB: u32 = 4096;
pub const MAX_BLOB: usize = 256 << 20;
pub const MAX_BATCH: usize = 10_000;
/// Decided jobs (and their outputs) are kept this long for their clients.
pub const FINISHED_TTL: Duration = Duration::from_secs(3600);
/// Blobs no job refers to are dropped after this (time to submit after uploading).
pub const BLOB_TTL: Duration = Duration::from_secs(3600);
/// Devices silent for this long are forgotten (their address keeps its strikes).
pub const AGENT_TTL: Duration = Duration::from_secs(24 * 3600);
const PRUNE_EVERY: Duration = Duration::from_secs(10);
const MAX_WAIT: f64 = 60.0;

#[derive(Clone, Debug)]
pub struct Config {
    /// Bearer token for clients (submit, upload, results); None = open.
    pub client_token: Option<String>,
    /// Bearer token for devices joining the pool; None = anyone may join.
    pub agent_token: Option<String>,
    pub agent_timeout: Duration,
    /// A lease not renewed by the device's heartbeats for this long is re-queued.
    pub lease_timeout: Duration,
    /// Replicas of a job go to devices with distinct IP addresses, and strikes count per
    /// address, so one machine cannot outvote others or shed a ban by re-registering.
    pub distinct_ips: bool,
}

impl Default for Config {
    fn default() -> Self {
        Config {
            client_token: None,
            agent_token: None,
            agent_timeout: Duration::from_secs(30),
            lease_timeout: Duration::from_secs(60),
            distinct_ips: true,
        }
    }
}

#[derive(Debug, PartialEq)]
pub enum ApiError {
    Unauthorized,
    Forbidden(&'static str),
    NotFound,
    Conflict(&'static str),
    BadRequest(String),
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let (code, msg) = match self {
            ApiError::Unauthorized => (
                StatusCode::UNAUTHORIZED,
                "invalid or missing token".to_string(),
            ),
            ApiError::Forbidden(m) => (StatusCode::FORBIDDEN, m.to_string()),
            ApiError::NotFound => (StatusCode::NOT_FOUND, "not found".to_string()),
            ApiError::Conflict(m) => (StatusCode::CONFLICT, m.to_string()),
            ApiError::BadRequest(m) => (StatusCode::BAD_REQUEST, m),
        };
        (code, msg).into_response()
    }
}

struct Agent {
    name: String,
    caps: Caps,
    ip: IpAddr,
    secret: String,
    last_seen: Instant,
    strikes: u32,
    credits: u64,
}

struct Job {
    spec: JobSpec,
    status: JobStatus,
    outcome: Option<Outcome>,
    error: Option<String>,
    decided_at: Option<Instant>,
    tasks: Vec<String>,
    /// Devices (and their addresses) holding or having voted on a replica: replicas go to
    /// distinct devices. A device that gives a replica back is removed again.
    assigned: HashSet<String>,
    ips: HashSet<IpAddr>,
    votes: Vec<(String, String, Outcome)>, // agent, vote, outcome
    /// Woken when the job is decided (clients long-poll on it).
    decided: Arc<Notify>,
}

#[derive(PartialEq)]
enum TaskState {
    Pending,
    Leased { agent: String, renewed: Instant },
    Closed, // voted, or not needed any more
}

struct Task {
    job: String,
    state: TaskState,
}

struct Blob {
    data: Bytes,
    added: Instant,
}

#[derive(Default)]
pub struct Pool {
    distinct_ips: bool,
    agents: HashMap<String, Agent>,
    ip_strikes: HashMap<IpAddr, u32>,
    jobs: HashMap<String, Job>,
    tasks: HashMap<String, Task>,
    pending: VecDeque<String>,
    blobs: HashMap<String, Blob>,
    pruned: Option<Instant>,
}

fn new_id() -> String {
    uuid::Uuid::new_v4().simple().to_string()
}

impl Pool {
    pub fn new(cfg: &Config) -> Self {
        Pool {
            distinct_ips: cfg.distinct_ips,
            ..Pool::default()
        }
    }

    /// Store a blob under its hash (computed by the caller, outside the pool lock).
    pub fn insert_blob(&mut self, hash: String, data: Bytes) {
        let added = Instant::now();
        self.blobs.entry(hash).or_insert(Blob { data, added }).added = added;
    }

    pub fn put_blob(&mut self, data: Bytes) -> String {
        let hash = blob_hash(&data);
        self.insert_blob(hash.clone(), data);
        hash
    }

    pub fn blob(&self, hash: &str) -> Option<Bytes> {
        self.blobs.get(hash).map(|b| b.data.clone())
    }

    fn banned(&self, agent: &Agent) -> bool {
        let address = self.ip_strikes.get(&agent.ip).copied().unwrap_or(0);
        agent.strikes >= MAX_STRIKES || self.distinct_ips && address >= MAX_STRIKES
    }

    pub fn register(&mut self, req: Register, ip: IpAddr) -> Result<Registered, ApiError> {
        if req.protocol != PROTOCOL {
            return Err(ApiError::BadRequest(format!(
                "protocol {} != {PROTOCOL}",
                req.protocol
            )));
        }
        if self.distinct_ips && self.ip_strikes.get(&ip).copied().unwrap_or(0) >= MAX_STRIKES {
            return Err(ApiError::Forbidden("address banned for wrong results"));
        }
        let (agent_id, secret) = (new_id(), new_id());
        let agent = Agent {
            name: req.name,
            caps: req.caps,
            ip,
            secret: secret.clone(),
            last_seen: Instant::now(),
            strikes: 0,
            credits: 0,
        };
        self.agents.insert(agent_id.clone(), agent);
        Ok(Registered { agent_id, secret })
    }

    /// Check an agent call's identity; also refreshes the device's liveness.
    pub fn authenticate(&mut self, id: &str, secret: &str) -> Result<IpAddr, ApiError> {
        let agent = self.agents.get(id).ok_or(ApiError::NotFound)?;
        if !constant_time_eq(agent.secret.as_bytes(), secret.as_bytes()) {
            return Err(ApiError::Unauthorized);
        }
        if self.banned(agent) {
            return Err(ApiError::Forbidden("device banned for wrong results"));
        }
        let agent = self.agents.get_mut(id).unwrap();
        agent.last_seen = Instant::now();
        Ok(agent.ip)
    }

    /// Liveness, and renewal of the leases the device is still working on.
    pub fn heartbeat(
        &mut self,
        id: &str,
        secret: &str,
        working_on: &[String],
    ) -> Result<(), ApiError> {
        self.authenticate(id, secret)?;
        let now = Instant::now();
        for task_id in working_on {
            if let Some(Task {
                state: TaskState::Leased { agent, renewed },
                ..
            }) = self.tasks.get_mut(task_id)
                && agent == id
            {
                *renewed = now;
            }
        }
        Ok(())
    }

    pub fn submit(&mut self, spec: JobSpec) -> Result<String, ApiError> {
        let bad = |m: &str| Err(ApiError::BadRequest(m.into()));
        if !self.blobs.contains_key(&spec.module) || !self.blobs.contains_key(&spec.input) {
            return bad("upload the module and the input first");
        }
        if spec.replicas == 0 || spec.replicas > MAX_REPLICAS {
            return bad("replicas must be 1..=7");
        }
        if spec.memory_mb == 0 || spec.memory_mb > MAX_MEMORY_MB {
            return bad("memory_mb out of range");
        }
        let id = new_id();
        let replicas = spec.replicas;
        let job = Job {
            status: JobStatus::Pending,
            outcome: None,
            error: None,
            decided_at: None,
            tasks: Vec::new(),
            assigned: HashSet::new(),
            ips: HashSet::new(),
            votes: Vec::new(),
            decided: Arc::new(Notify::new()),
            spec,
        };
        self.jobs.insert(id.clone(), job);
        for _ in 0..replicas {
            self.add_task(&id);
        }
        Ok(id)
    }

    fn add_task(&mut self, job_id: &str) {
        let task_id = new_id();
        let task = Task {
            job: job_id.into(),
            state: TaskState::Pending,
        };
        self.tasks.insert(task_id.clone(), task);
        self.jobs
            .get_mut(job_id)
            .unwrap()
            .tasks
            .push(task_id.clone());
        self.pending.push_back(task_id);
    }

    /// Hand the device the oldest task of a job it (and its address) has no replica of.
    pub fn lease(&mut self, agent_id: &str, secret: &str) -> Result<Option<Lease>, ApiError> {
        let ip = self.authenticate(agent_id, secret)?;
        let distinct_ips = self.distinct_ips;
        let Some(pos) = self.pending.iter().position(|t| {
            let job = &self.jobs[&self.tasks[t].job];
            !job.assigned.contains(agent_id) && !(distinct_ips && job.ips.contains(&ip))
        }) else {
            return Ok(None);
        };
        let task_id = self.pending.remove(pos).unwrap();
        let task = self.tasks.get_mut(&task_id).unwrap();
        task.state = TaskState::Leased {
            agent: agent_id.into(),
            renewed: Instant::now(),
        };
        let job = self.jobs.get_mut(&task.job).unwrap();
        job.assigned.insert(agent_id.into());
        job.ips.insert(ip);
        Ok(Some(Lease {
            task_id,
            job_id: task.job.clone(),
            spec: job.spec.clone(),
        }))
    }

    fn check_lease(&self, task_id: &str, agent_id: &str) -> Result<(), ApiError> {
        let task = self.tasks.get(task_id).ok_or(ApiError::NotFound)?;
        match &task.state {
            TaskState::Leased { agent, .. } if agent == agent_id => Ok(()),
            _ => Err(ApiError::Conflict("task is not leased to this device")),
        }
    }

    /// Record a successful run: the output is stored and its hash computed here, so a
    /// device can only vote for an output it actually delivered (one bounded upload per
    /// lease, not open storage).
    pub fn report_output(
        &mut self,
        task_id: &str,
        agent_id: &str,
        secret: &str,
        (hash, data): (String, Bytes),
        fuel_used: u64,
    ) -> Result<(), ApiError> {
        self.authenticate(agent_id, secret)?;
        self.check_lease(task_id, agent_id)?;
        if data.len() > MAX_OUTPUT {
            return Err(ApiError::BadRequest("output too large".into()));
        }
        self.insert_blob(hash.clone(), data);
        let outcome = Outcome::Ok {
            output: hash,
            fuel_used,
        };
        self.report(task_id, agent_id, secret, outcome)
    }

    /// Up to `max` leases at once (a device with free slots fills them in one request).
    pub fn lease_many(
        &mut self,
        agent_id: &str,
        secret: &str,
        max: usize,
    ) -> Result<Vec<Lease>, ApiError> {
        let mut leases = Vec::new();
        while leases.len() < max {
            match self.lease(agent_id, secret)? {
                Some(lease) => leases.push(lease),
                None => break,
            }
        }
        Ok(leases)
    }

    /// Give a leased task back to the queue (or drop it if its job is already decided).
    fn release(&mut self, task_id: &str) {
        let task = self.tasks.get_mut(task_id).unwrap();
        let TaskState::Leased { agent, .. } = std::mem::replace(&mut task.state, TaskState::Closed)
        else {
            return;
        };
        let job = self.jobs.get_mut(&task.job).unwrap();
        if job.status != JobStatus::Pending {
            return; // the replica is not needed any more
        }
        // The device may take it again: in a small pool nobody else can.
        job.assigned.remove(&agent);
        if let Some(a) = self.agents.get(&agent) {
            job.ips.remove(&a.ip);
        }
        task.state = TaskState::Pending;
        self.pending.push_back(task_id.into());
    }

    pub fn report(
        &mut self,
        task_id: &str,
        agent_id: &str,
        secret: &str,
        outcome: Outcome,
    ) -> Result<(), ApiError> {
        self.authenticate(agent_id, secret)?;
        self.check_lease(task_id, agent_id)?;
        let Some(vote) = outcome.vote() else {
            self.release(task_id); // someone (maybe this device again) will run it
            return Ok(());
        };
        if let Outcome::Ok { output, .. } = &outcome
            && !self.blobs.contains_key(output)
        {
            return Err(ApiError::BadRequest("upload the output first".into()));
        }
        let task = self.tasks.get_mut(task_id).unwrap();
        task.state = TaskState::Closed;
        let job_id = task.job.clone();
        let job = self.jobs.get_mut(&job_id).unwrap();
        job.votes.push((agent_id.into(), vote, outcome));
        self.tally(&job_id);
        Ok(())
    }

    /// Accept a result once a quorum agrees; otherwise add a replica or give up.
    fn tally(&mut self, job_id: &str) {
        let job = self.jobs.get_mut(job_id).unwrap();
        if job.status != JobStatus::Pending {
            return; // a late replica: the job is decided
        }
        let quorum = job.spec.replicas / 2 + 1;
        let mut counts: HashMap<&str, u32> = HashMap::new();
        for (_, vote, _) in &job.votes {
            *counts.entry(vote).or_default() += 1;
        }
        if let Some((winner, _)) = counts.into_iter().find(|&(_, n)| n >= quorum) {
            let winner = winner.to_string();
            for (agent_id, vote, out) in &job.votes {
                let Some(agent) = self.agents.get_mut(agent_id) else {
                    continue;
                };
                if *vote == winner {
                    agent.credits += out.fuel_used();
                    job.outcome = Some(out.clone());
                } else {
                    agent.strikes += 1;
                    *self.ip_strikes.entry(agent.ip).or_default() += 1;
                }
            }
            job.status = JobStatus::Done;
            self.decide(job_id);
            return;
        }
        let open = job
            .tasks
            .iter()
            .any(|t| self.tasks[t].state != TaskState::Closed);
        if open {
            return;
        }
        if (job.tasks.len() as u32) < job.spec.replicas + EXTRA_REPLICAS {
            self.add_task(job_id);
        } else {
            job.status = JobStatus::Failed;
            job.error = Some("replicas did not reach a quorum".into());
            self.decide(job_id);
        }
    }

    fn decide(&mut self, job_id: &str) {
        let job = self.jobs.get_mut(job_id).unwrap();
        job.decided_at = Some(Instant::now());
        job.decided.notify_waiters();
        let tasks: HashSet<&String> = job.tasks.iter().collect();
        self.pending.retain(|t| !tasks.contains(t)); // spare replicas are not needed
    }

    /// Re-queue leases of silent devices and leases nobody renewed; forget old state.
    pub fn sweep(&mut self, cfg: &Config) {
        let now = Instant::now();
        let expired: Vec<String> = self
            .tasks
            .iter()
            .filter(|(_, task)| match &task.state {
                TaskState::Leased { agent, renewed } => {
                    let silent = self
                        .agents
                        .get(agent)
                        .is_none_or(|a| now - a.last_seen > cfg.agent_timeout);
                    silent || now - *renewed > cfg.lease_timeout
                }
                _ => false,
            })
            .map(|(id, _)| id.clone())
            .collect();
        for task_id in expired {
            self.release(&task_id);
        }
        if self.pruned.is_none_or(|at| now - at >= PRUNE_EVERY) {
            self.pruned = Some(now);
            self.prune(now);
        }
    }

    fn prune(&mut self, now: Instant) {
        let old: Vec<String> = self
            .jobs
            .iter()
            .filter(|(_, j)| j.decided_at.is_some_and(|at| now - at > FINISHED_TTL))
            .map(|(id, _)| id.clone())
            .collect();
        for id in old {
            for task in self.jobs.remove(&id).unwrap().tasks {
                self.tasks.remove(&task);
            }
        }
        let mut used = HashSet::new();
        for job in self.jobs.values() {
            used.extend([job.spec.module.clone(), job.spec.input.clone()]);
            for (_, _, out) in &job.votes {
                if let Outcome::Ok { output, .. } = out {
                    used.insert(output.clone());
                }
            }
        }
        self.blobs
            .retain(|hash, blob| used.contains(hash) || now - blob.added < BLOB_TTL);
        let leasing: HashSet<&String> = self
            .tasks
            .values()
            .filter_map(|t| match &t.state {
                TaskState::Leased { agent, .. } => Some(agent),
                _ => None,
            })
            .collect();
        let gone: Vec<String> = self
            .agents
            .iter()
            .filter(|(id, a)| now - a.last_seen > AGENT_TTL && !leasing.contains(id))
            .map(|(id, _)| id.clone())
            .collect();
        for id in gone {
            self.agents.remove(&id);
        }
    }

    pub fn job(&self, id: &str) -> Option<JobView> {
        self.jobs.get(id).map(|j| JobView {
            id: id.into(),
            status: j.status,
            spec: j.spec.clone(),
            outcome: j.outcome.clone(),
            error: j.error.clone(),
            votes: j
                .votes
                .iter()
                .map(|(a, v, _)| Vote {
                    agent_id: a.clone(),
                    vote: v.clone(),
                })
                .collect(),
        })
    }

    fn decided_signal(&self, id: &str) -> Option<Arc<Notify>> {
        self.jobs.get(id).map(|j| j.decided.clone())
    }

    pub fn agents(&self, cfg: &Config) -> Vec<AgentView> {
        self.agents
            .iter()
            .map(|(id, a)| AgentView {
                id: id.clone(),
                name: a.name.clone(),
                caps: a.caps.clone(),
                online: a.last_seen.elapsed() <= cfg.agent_timeout,
                strikes: a.strikes,
                banned: self.banned(a),
                credits: a.credits,
            })
            .collect()
    }
}

fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    a.len() == b.len() && a.iter().zip(b).fold(0u8, |acc, (x, y)| acc | (x ^ y)) == 0
}

// --- HTTP ---

struct Shared {
    pool: Mutex<Pool>,
    cfg: Config,
    /// Woken when tasks become pending (devices long-poll on it).
    tasks_changed: Notify,
}

type App = State<Arc<Shared>>;

#[derive(Clone, Copy)]
enum Side {
    Client,
    Agent,
}

impl Shared {
    fn pool(&self) -> std::sync::MutexGuard<'_, Pool> {
        self.pool.lock().unwrap_or_else(|p| p.into_inner())
    }

    fn token_ok(&self, headers: &HeaderMap, side: Side) -> bool {
        let given = headers
            .get("authorization")
            .and_then(|v| v.to_str().ok())
            .and_then(|v| v.strip_prefix("Bearer "))
            .unwrap_or("");
        let token = match side {
            Side::Client => &self.cfg.client_token,
            Side::Agent => &self.cfg.agent_token,
        };
        token
            .as_ref()
            .is_none_or(|t| constant_time_eq(t.as_bytes(), given.as_bytes()))
    }

    fn check(&self, headers: &HeaderMap, side: Side) -> Result<(), ApiError> {
        if self.token_ok(headers, side) {
            Ok(())
        } else {
            Err(ApiError::Unauthorized)
        }
    }
}

fn header<'a>(headers: &'a HeaderMap, name: &str) -> &'a str {
    headers
        .get(name)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("")
}

fn secret(headers: &HeaderMap) -> &str {
    header(headers, "x-agent-secret")
}

#[derive(Deserialize)]
struct Wait {
    #[serde(default)]
    wait: f64,
}

#[derive(Deserialize)]
struct LeaseQuery {
    #[serde(default)]
    wait: f64,
    #[serde(default = "one")]
    max: usize,
}

fn one() -> usize {
    1
}

pub const MAX_LEASES: usize = 256;

#[derive(Deserialize)]
struct Fuel {
    fuel_used: u64,
}

fn deadline(wait: f64) -> tokio::time::Instant {
    tokio::time::Instant::now() + Duration::from_secs_f64(wait.clamp(0.0, MAX_WAIT))
}

/// Uploads are for clients only; devices upload the outputs of their own leases.
async fn put_blob(
    State(s): App,
    headers: HeaderMap,
    body: Bytes,
) -> Result<Json<Created>, ApiError> {
    s.check(&headers, Side::Client)?;
    let hash = blob_hash(&body); // outside the lock: hashing 256 MiB takes a while
    s.pool().insert_blob(hash.clone(), body);
    Ok(Json(Created { id: hash }))
}

/// Readable by clients and by registered devices (x-agent-id + x-agent-secret).
async fn get_blob(
    State(s): App,
    headers: HeaderMap,
    Path(hash): Path<String>,
) -> Result<Bytes, ApiError> {
    let mut pool = s.pool();
    let agent = header(&headers, "x-agent-id");
    if agent.is_empty() || !s.token_ok(&headers, Side::Agent) {
        s.check(&headers, Side::Client)?;
    } else {
        pool.authenticate(agent, secret(&headers))?;
    }
    pool.blob(&hash).ok_or(ApiError::NotFound)
}

async fn submit(
    State(s): App,
    headers: HeaderMap,
    Json(spec): Json<JobSpec>,
) -> Result<Json<Created>, ApiError> {
    s.check(&headers, Side::Client)?;
    let id = s.pool().submit(spec)?;
    s.tasks_changed.notify_waiters();
    Ok(Json(Created { id }))
}

/// Many jobs in one request (a map over inputs): ids in the same order.
async fn submit_batch(
    State(s): App,
    headers: HeaderMap,
    Json(specs): Json<Vec<JobSpec>>,
) -> Result<Json<Vec<String>>, ApiError> {
    s.check(&headers, Side::Client)?;
    if specs.len() > MAX_BATCH {
        return Err(ApiError::BadRequest(format!(
            "at most {MAX_BATCH} jobs per batch"
        )));
    }
    let ids = {
        let mut pool = s.pool();
        specs
            .into_iter()
            .map(|spec| pool.submit(spec))
            .collect::<Result<Vec<_>, _>>()?
    };
    s.tasks_changed.notify_waiters();
    Ok(Json(ids))
}

async fn get_job(
    State(s): App,
    headers: HeaderMap,
    Path(id): Path<String>,
    Query(q): Query<Wait>,
) -> Result<Json<JobView>, ApiError> {
    s.check(&headers, Side::Client)?;
    let until = deadline(q.wait);
    let signal = s.pool().decided_signal(&id).ok_or(ApiError::NotFound)?;
    loop {
        let decided = signal.notified(); // only this job's decision wakes this request
        let view = s.pool().job(&id).ok_or(ApiError::NotFound)?;
        if view.status != JobStatus::Pending
            || tokio::time::timeout_at(until, decided).await.is_err()
        {
            return Ok(Json(view));
        }
    }
}

async fn list_agents(State(s): App, headers: HeaderMap) -> Result<Json<Vec<AgentView>>, ApiError> {
    s.check(&headers, Side::Client)?;
    Ok(Json(s.pool().agents(&s.cfg)))
}

async fn register(
    State(s): App,
    ConnectInfo(addr): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    Json(req): Json<Register>,
) -> Result<Json<Registered>, ApiError> {
    s.check(&headers, Side::Agent)?;
    Ok(Json(s.pool().register(req, addr.ip())?))
}

async fn heartbeat(
    State(s): App,
    headers: HeaderMap,
    Path(id): Path<String>,
    Json(beat): Json<Heartbeat>,
) -> Result<StatusCode, ApiError> {
    s.check(&headers, Side::Agent)?;
    s.pool().heartbeat(&id, secret(&headers), &beat.tasks)?;
    Ok(StatusCode::NO_CONTENT)
}

async fn lease(
    State(s): App,
    headers: HeaderMap,
    Path(id): Path<String>,
    Query(q): Query<LeaseQuery>,
) -> Result<Json<Vec<Lease>>, ApiError> {
    s.check(&headers, Side::Agent)?;
    let until = deadline(q.wait);
    let max = q.max.clamp(1, MAX_LEASES);
    loop {
        let changed = s.tasks_changed.notified();
        let leases = s.pool().lease_many(&id, secret(&headers), max)?;
        if !leases.is_empty() || tokio::time::timeout_at(until, changed).await.is_err() {
            return Ok(Json(leases));
        }
    }
}

/// A successful run in one request: the raw output is the body.
async fn report_output(
    State(s): App,
    headers: HeaderMap,
    Path((agent_id, task_id)): Path<(String, String)>,
    Query(q): Query<Fuel>,
    body: Bytes,
) -> Result<StatusCode, ApiError> {
    s.check(&headers, Side::Agent)?;
    let output = (blob_hash(&body), body); // hashed outside the lock
    s.pool()
        .report_output(&task_id, &agent_id, secret(&headers), output, q.fuel_used)?;
    s.tasks_changed.notify_waiters();
    Ok(StatusCode::NO_CONTENT)
}

async fn report(
    State(s): App,
    headers: HeaderMap,
    Path((agent_id, task_id)): Path<(String, String)>,
    Json(outcome): Json<Outcome>,
) -> Result<StatusCode, ApiError> {
    s.check(&headers, Side::Agent)?;
    s.pool()
        .report(&task_id, &agent_id, secret(&headers), outcome)?;
    // A report may re-queue a task or add a replica.
    s.tasks_changed.notify_waiters();
    Ok(StatusCode::NO_CONTENT)
}

/// Certificate chain and private key (PEM) for serving HTTPS.
pub struct Tls {
    pub cert_pem: Vec<u8>,
    pub key_pem: Vec<u8>,
}

/// Serve the API on `listener`: plain HTTP, or HTTPS with `tls`. Without TLS, agent
/// secrets and client tokens cross the network readable; put a TLS proxy in front then.
pub async fn serve(listener: TcpListener, cfg: Config, tls: Option<Tls>) -> anyhow::Result<()> {
    let service = app(cfg).into_make_service_with_connect_info::<SocketAddr>();
    match tls {
        None => axum::serve(listener, service).await?,
        Some(tls) => {
            let config =
                axum_server::tls_rustls::RustlsConfig::from_pem(tls.cert_pem, tls.key_pem).await?;
            axum_server::from_tcp_rustls(listener.into_std()?, config)?
                .serve(service)
                .await?
        }
    }
    Ok(())
}

fn app(cfg: Config) -> Router {
    let shared = Arc::new(Shared {
        pool: Mutex::new(Pool::new(&cfg)),
        cfg,
        tasks_changed: Notify::new(),
    });
    let sweeper = shared.clone();
    tokio::spawn(async move {
        let every = (sweeper.cfg.agent_timeout.min(sweeper.cfg.lease_timeout) / 3)
            .max(Duration::from_millis(50));
        loop {
            tokio::time::sleep(every).await;
            sweeper.pool().sweep(&sweeper.cfg);
            sweeper.tasks_changed.notify_waiters();
        }
    });
    Router::new()
        .route("/v1/blobs", post(put_blob))
        .route("/v1/blobs/{hash}", get(get_blob))
        .route("/v1/jobs", post(submit))
        .route("/v1/jobs/batch", post(submit_batch))
        .route("/v1/jobs/{id}", get(get_job))
        .route("/v1/agents", get(list_agents).post(register))
        .route("/v1/agents/{id}/heartbeat", post(heartbeat))
        .route("/v1/agents/{id}/lease", get(lease))
        .route("/v1/agents/{id}/tasks/{task}", post(report))
        .route("/v1/agents/{id}/tasks/{task}/output", post(report_output))
        .layer(DefaultBodyLimit::max(MAX_BLOB))
        .with_state(shared)
}

#[cfg(test)]
mod tests {
    use super::*;

    const LOCAL: IpAddr = IpAddr::V4(std::net::Ipv4Addr::LOCALHOST);

    fn ip(n: u8) -> IpAddr {
        IpAddr::V4(std::net::Ipv4Addr::new(10, 0, 0, n))
    }

    type Device = (String, String);

    fn register(pool: &mut Pool, ip: IpAddr) -> Result<Device, ApiError> {
        let req = Register {
            name: "d".into(),
            caps: Caps::default(),
            protocol: PROTOCOL.into(),
        };
        pool.register(req, ip).map(|r| (r.agent_id, r.secret))
    }

    fn submit(pool: &mut Pool, replicas: u32) -> String {
        let module = pool.put_blob(Bytes::from_static(b"m"));
        let input = pool.put_blob(Bytes::from_static(b"i"));
        let spec = JobSpec {
            module,
            input,
            fuel: 100,
            memory_mb: 1,
            replicas,
        };
        pool.submit(spec).unwrap()
    }

    /// A pool without the per-address rules (all devices on one machine) and one job.
    fn pool_with_job(replicas: u32, devices: usize) -> (Pool, String, Vec<Device>) {
        let mut pool = Pool::default();
        let devices = (0..devices)
            .map(|_| register(&mut pool, LOCAL).unwrap())
            .collect();
        let job = submit(&mut pool, replicas);
        (pool, job, devices)
    }

    fn ok(pool: &mut Pool, data: &'static [u8]) -> Outcome {
        Outcome::Ok {
            output: pool.put_blob(Bytes::from_static(data)),
            fuel_used: 7,
        }
    }

    fn lease(pool: &mut Pool, (id, secret): &Device) -> Option<Lease> {
        pool.lease(id, secret).unwrap()
    }

    fn report(pool: &mut Pool, (id, secret): &Device, lease: &Lease, outcome: Outcome) {
        pool.report(&lease.task_id, id, secret, outcome).unwrap();
    }

    fn run(pool: &mut Pool, device: &Device, outcome: Outcome) {
        let lease = lease(pool, device).expect("a task to lease");
        report(pool, device, &lease, outcome);
    }

    fn stats(pool: &Pool, (id, _): &Device) -> (u32, u64) {
        let agent = &pool.agents[id];
        (agent.strikes, agent.credits)
    }

    #[test]
    fn replicas_go_to_distinct_devices() {
        let (mut pool, _, devices) = pool_with_job(2, 2);
        assert!(lease(&mut pool, &devices[0]).is_some());
        assert!(lease(&mut pool, &devices[0]).is_none());
        assert!(lease(&mut pool, &devices[1]).is_some());
    }

    #[test]
    fn disagreement_adds_a_replica_and_strikes_the_liar() {
        let (mut pool, job, devices) = pool_with_job(2, 3);
        let good = ok(&mut pool, b"right");
        let bad = ok(&mut pool, b"wrong");
        run(&mut pool, &devices[0], good.clone());
        run(&mut pool, &devices[1], bad);
        assert_eq!(pool.job(&job).unwrap().status, JobStatus::Pending);
        run(&mut pool, &devices[2], good.clone());
        let view = pool.job(&job).unwrap();
        assert_eq!((view.status, view.outcome), (JobStatus::Done, Some(good)));
        assert_eq!(stats(&pool, &devices[0]), (0, 7));
        assert_eq!(stats(&pool, &devices[1]), (1, 0));
    }

    #[test]
    fn no_quorum_after_extra_replicas_fails_the_job() {
        let (mut pool, job, devices) = pool_with_job(2, 4);
        for (i, device) in devices.iter().enumerate() {
            let out = Outcome::Ok {
                output: pool.put_blob(Bytes::from(vec![i as u8])),
                fuel_used: 1,
            };
            run(&mut pool, device, out);
        }
        assert_eq!(pool.job(&job).unwrap().status, JobStatus::Failed);
    }

    #[test]
    fn agreed_failures_are_credited_with_the_fuel_they_burned() {
        let (mut pool, job, devices) = pool_with_job(2, 2);
        let failed = |detail: &str| Outcome::Failed {
            kind: FailKind::OutOfFuel,
            fuel_used: 100,
            detail: detail.into(),
        };
        // Details (trap messages) may differ between devices; kind and fuel are voted on.
        run(&mut pool, &devices[0], failed("x"));
        run(&mut pool, &devices[1], failed("y"));
        assert_eq!(pool.job(&job).unwrap().status, JobStatus::Done);
        assert_eq!(stats(&pool, &devices[0]), (0, 100));
    }

    #[test]
    fn a_given_back_task_can_return_to_the_only_device() {
        let (mut pool, job, devices) = pool_with_job(1, 1);
        let abandoned = Outcome::Abandoned {
            reason: "fetch failed".into(),
        };
        run(&mut pool, &devices[0], abandoned);
        let good = ok(&mut pool, b"r");
        run(&mut pool, &devices[0], good);
        assert_eq!(pool.job(&job).unwrap().status, JobStatus::Done);
    }

    #[test]
    fn heartbeats_renew_leases_of_long_tasks() {
        let (mut pool, _, devices) = pool_with_job(1, 2);
        let cfg = Config::default();
        let task = lease(&mut pool, &devices[0]).unwrap().task_id;
        let age = |pool: &mut Pool| {
            if let TaskState::Leased { renewed, .. } = &mut pool.tasks.get_mut(&task).unwrap().state
            {
                *renewed -= cfg.lease_timeout * 2;
            }
        };
        age(&mut pool);
        let working_on = std::slice::from_ref(&task);
        pool.heartbeat(&devices[0].0, &devices[0].1, working_on)
            .unwrap();
        pool.sweep(&cfg);
        assert!(
            lease(&mut pool, &devices[1]).is_none(),
            "a renewed lease stays"
        );
        age(&mut pool); // the device hangs: no more renewals
        pool.sweep(&cfg);
        assert!(lease(&mut pool, &devices[1]).is_some());
    }

    #[test]
    fn leases_of_silent_devices_are_requeued_unless_the_job_is_decided() {
        let (mut pool, job, devices) = pool_with_job(1, 2);
        lease(&mut pool, &devices[0]).unwrap();
        pool.agents.get_mut(&devices[0].0).unwrap().last_seen -= Duration::from_secs(60);
        pool.sweep(&Config::default());
        let good = ok(&mut pool, b"r");
        run(&mut pool, &devices[1], good);
        assert_eq!(pool.job(&job).unwrap().status, JobStatus::Done);

        // A spare replica given back after the decision is not run again.
        let job = submit(&mut pool, 3);
        let third = register(&mut pool, LOCAL).unwrap();
        let leases: Vec<Lease> = [&devices[0], &devices[1], &third]
            .iter()
            .map(|d| lease(&mut pool, d).unwrap())
            .collect();
        let good = ok(&mut pool, b"q");
        report(&mut pool, &devices[0], &leases[0], good.clone());
        report(&mut pool, &devices[1], &leases[1], good);
        assert_eq!(pool.job(&job).unwrap().status, JobStatus::Done);
        let gone = Outcome::Abandoned { reason: "x".into() };
        report(&mut pool, &third, &leases[2], gone);
        assert!(pool.pending.is_empty());
    }

    #[test]
    fn strikes_follow_the_address_and_replicas_need_distinct_addresses() {
        let mut pool = Pool::new(&Config::default());
        let liar = register(&mut pool, ip(1)).unwrap();
        let liar2 = register(&mut pool, ip(1)).unwrap();
        let honest = register(&mut pool, ip(2)).unwrap();
        submit(&mut pool, 2);
        assert!(lease(&mut pool, &liar).is_some());
        assert!(lease(&mut pool, &liar2).is_none(), "same address, same job");
        assert!(lease(&mut pool, &honest).is_some());
        *pool.ip_strikes.entry(ip(1)).or_default() = MAX_STRIKES;
        // Every identity behind the address is banned, and new ones are refused.
        let banned = pool.lease(&liar2.0, &liar2.1);
        assert!(matches!(banned, Err(ApiError::Forbidden(_))));
        assert!(matches!(
            register(&mut pool, ip(1)),
            Err(ApiError::Forbidden(_))
        ));
        let wrong_secret = pool.heartbeat(&honest.0, "wrong", &[]);
        assert_eq!(wrong_secret, Err(ApiError::Unauthorized));
    }

    #[test]
    fn old_jobs_blobs_and_devices_are_forgotten() {
        let (mut pool, job, devices) = pool_with_job(1, 1);
        let good = ok(&mut pool, b"out");
        run(&mut pool, &devices[0], good);
        let stray = pool.put_blob(Bytes::from_static(b"never used"));
        let later = FINISHED_TTL.max(BLOB_TTL).max(AGENT_TTL) + Duration::from_secs(1);
        pool.prune(Instant::now() + later);
        assert!(pool.job(&job).is_none() && pool.tasks.is_empty());
        assert!(pool.blob(&stray).is_none() && pool.blobs.is_empty());
        assert!(pool.agents.is_empty());
    }
}
