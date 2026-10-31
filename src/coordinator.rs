//! Coordinator: job queue, task leases, quorum voting, strikes, credits and the blob store.
//! State lives in memory; `Pool` holds the logic (synchronous, unit-tested), the HTTP layer
//! below only translates requests.

use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use axum::body::Bytes;
use axum::extract::{DefaultBodyLimit, Path, Query, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use serde::Deserialize;
use tokio::net::TcpListener;
use tokio::sync::Notify;

use crate::proto::*;

pub const MAX_STRIKES: u32 = 3;
pub const EXTRA_REPLICAS: u32 = 2; // added one by one when replicas disagree
pub const MAX_REPLICAS: u32 = 7;
pub const MAX_MEMORY_MB: u32 = 4096;
pub const MAX_BLOB: usize = 256 << 20;
const MAX_WAIT: f64 = 60.0;

#[derive(Clone, Debug)]
pub struct Config {
    /// Bearer token for clients (submit, results); None = open.
    pub client_token: Option<String>,
    /// Bearer token for devices joining the pool; None = anyone may join.
    pub agent_token: Option<String>,
    pub agent_timeout: Duration,
    pub lease_timeout: Duration,
}

impl Default for Config {
    fn default() -> Self {
        Config {
            client_token: None,
            agent_token: None,
            agent_timeout: Duration::from_secs(30),
            lease_timeout: Duration::from_secs(300),
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
    tasks: Vec<String>,
    /// Devices that got a replica of this job (replicas go to distinct devices).
    assigned: HashSet<String>,
    votes: Vec<(String, String, Outcome)>, // agent, vote, outcome
}

#[derive(PartialEq)]
enum TaskState {
    Pending,
    Leased { agent: String, at: Instant },
    Voted,
}

struct Task {
    job: String,
    state: TaskState,
}

#[derive(Default)]
pub struct Pool {
    agents: HashMap<String, Agent>,
    jobs: HashMap<String, Job>,
    tasks: HashMap<String, Task>,
    pending: VecDeque<String>,
    blobs: HashMap<String, Bytes>,
}

fn new_id() -> String {
    uuid::Uuid::new_v4().simple().to_string()
}

impl Pool {
    pub fn put_blob(&mut self, data: Bytes) -> String {
        let hash = blob_hash(&data);
        self.blobs.entry(hash.clone()).or_insert(data);
        hash
    }

    pub fn register(&mut self, req: Register) -> Result<Registered, ApiError> {
        if req.protocol != PROTOCOL {
            return Err(ApiError::BadRequest(format!(
                "protocol {} != {PROTOCOL}",
                req.protocol
            )));
        }
        let (agent_id, secret) = (new_id(), new_id());
        let agent = Agent {
            name: req.name,
            caps: req.caps,
            secret: secret.clone(),
            last_seen: Instant::now(),
            strikes: 0,
            credits: 0,
        };
        self.agents.insert(agent_id.clone(), agent);
        Ok(Registered { agent_id, secret })
    }

    /// The agent behind an agent call; also refreshes its liveness.
    fn agent(&mut self, id: &str, secret: &str) -> Result<&mut Agent, ApiError> {
        let agent = self.agents.get_mut(id).ok_or(ApiError::NotFound)?;
        if !constant_time_eq(agent.secret.as_bytes(), secret.as_bytes()) {
            return Err(ApiError::Unauthorized);
        }
        if agent.strikes >= MAX_STRIKES {
            return Err(ApiError::Forbidden("device banned for wrong results"));
        }
        agent.last_seen = Instant::now();
        Ok(agent)
    }

    pub fn heartbeat(&mut self, id: &str, secret: &str) -> Result<(), ApiError> {
        self.agent(id, secret).map(|_| ())
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
        let job = Job {
            status: JobStatus::Pending,
            outcome: None,
            error: None,
            tasks: Vec::new(),
            assigned: HashSet::new(),
            votes: Vec::new(),
            spec,
        };
        self.jobs.insert(id.clone(), job);
        for _ in 0..self.jobs[&id].spec.replicas {
            self.add_task(&id);
        }
        Ok(id)
    }

    fn add_task(&mut self, job_id: &str) {
        let task_id = new_id();
        self.tasks.insert(
            task_id.clone(),
            Task {
                job: job_id.into(),
                state: TaskState::Pending,
            },
        );
        self.jobs
            .get_mut(job_id)
            .unwrap()
            .tasks
            .push(task_id.clone());
        self.pending.push_back(task_id);
    }

    /// Hand the agent the oldest task of a job it has no replica of yet.
    pub fn lease(&mut self, agent_id: &str, secret: &str) -> Result<Option<Lease>, ApiError> {
        self.agent(agent_id, secret)?;
        let Some(pos) = self
            .pending
            .iter()
            .position(|t| !self.jobs[&self.tasks[t].job].assigned.contains(agent_id))
        else {
            return Ok(None);
        };
        let task_id = self.pending.remove(pos).unwrap();
        let task = self.tasks.get_mut(&task_id).unwrap();
        task.state = TaskState::Leased {
            agent: agent_id.into(),
            at: Instant::now(),
        };
        let job = self.jobs.get_mut(&task.job).unwrap();
        job.assigned.insert(agent_id.into());
        Ok(Some(Lease {
            task_id,
            job_id: task.job.clone(),
            spec: job.spec.clone(),
        }))
    }

    pub fn report(
        &mut self,
        task_id: &str,
        agent_id: &str,
        secret: &str,
        outcome: Outcome,
    ) -> Result<(), ApiError> {
        self.agent(agent_id, secret)?;
        let task = self.tasks.get_mut(task_id).ok_or(ApiError::NotFound)?;
        if !matches!(&task.state, TaskState::Leased { agent, .. } if agent == agent_id) {
            return Err(ApiError::Conflict("task is not leased to this device"));
        }
        let job_id = task.job.clone();
        let Some(vote) = outcome.vote() else {
            task.state = TaskState::Pending; // someone else will run it
            self.pending.push_back(task_id.into());
            return Ok(());
        };
        if let Outcome::Ok { output, .. } = &outcome
            && !self.blobs.contains_key(output)
        {
            return Err(ApiError::BadRequest("upload the output first".into()));
        }
        task.state = TaskState::Voted;
        self.jobs
            .get_mut(&job_id)
            .unwrap()
            .votes
            .push((agent_id.into(), vote, outcome));
        self.tally(&job_id);
        Ok(())
    }

    /// Accept a result once a quorum agrees; otherwise add a replica or give up.
    fn tally(&mut self, job_id: &str) {
        let job = self.jobs.get_mut(job_id).unwrap();
        if job.status != JobStatus::Pending {
            return;
        }
        let quorum = job.spec.replicas / 2 + 1;
        let mut counts: HashMap<&str, u32> = HashMap::new();
        for (_, vote, _) in &job.votes {
            *counts.entry(vote).or_default() += 1;
        }
        if let Some((winner, _)) = counts.into_iter().find(|&(_, n)| n >= quorum) {
            let winner = winner.to_string();
            let mut outcome = None;
            for (agent_id, vote, out) in &job.votes {
                let Some(agent) = self.agents.get_mut(agent_id) else {
                    continue;
                };
                if *vote == winner {
                    agent.credits += match out {
                        Outcome::Ok { fuel_used, .. } => *fuel_used,
                        _ => job.spec.fuel.min(1_000_000), // failures still cost the device
                    };
                    outcome = Some(out.clone());
                } else {
                    agent.strikes += 1;
                }
            }
            job.status = JobStatus::Done;
            job.outcome = outcome;
            let tasks: HashSet<&String> = job.tasks.iter().collect();
            self.pending.retain(|t| !tasks.contains(t)); // spare replicas are not needed
            return;
        }
        let open = job
            .tasks
            .iter()
            .any(|t| self.tasks[t].state != TaskState::Voted);
        if open {
            return;
        }
        if (job.tasks.len() as u32) < job.spec.replicas + EXTRA_REPLICAS {
            self.add_task(job_id);
        } else {
            job.status = JobStatus::Failed;
            job.error = Some("replicas did not reach a quorum".into());
        }
    }

    /// Re-queue leases of silent devices and leases that took too long.
    pub fn sweep(&mut self, cfg: &Config) {
        let now = Instant::now();
        for (task_id, task) in &mut self.tasks {
            if let TaskState::Leased { agent, at } = &task.state {
                let silent = self
                    .agents
                    .get(agent)
                    .is_none_or(|a| now - a.last_seen > cfg.agent_timeout);
                if self.jobs[&task.job].status != JobStatus::Pending {
                    task.state = TaskState::Voted; // the job is decided: the replica is not needed
                } else if silent || now - *at > cfg.lease_timeout {
                    task.state = TaskState::Pending;
                    self.pending.push_back(task_id.clone());
                }
            }
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

    pub fn agents(&self, cfg: &Config) -> Vec<AgentView> {
        self.agents
            .iter()
            .map(|(id, a)| AgentView {
                id: id.clone(),
                name: a.name.clone(),
                caps: a.caps.clone(),
                online: a.last_seen.elapsed() <= cfg.agent_timeout,
                strikes: a.strikes,
                banned: a.strikes >= MAX_STRIKES,
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
    /// Woken when a task becomes pending / a job finishes (long polls wait on them).
    tasks_changed: Notify,
    jobs_changed: Notify,
}

type App = State<Arc<Shared>>;

#[derive(Clone, Copy)]
enum Side {
    Client,
    Agent,
    Either,
}

impl Shared {
    fn pool(&self) -> std::sync::MutexGuard<'_, Pool> {
        self.pool.lock().unwrap_or_else(|p| p.into_inner())
    }

    fn check(&self, headers: &HeaderMap, side: Side) -> Result<(), ApiError> {
        let given = headers
            .get("authorization")
            .and_then(|v| v.to_str().ok())
            .and_then(|v| v.strip_prefix("Bearer "))
            .unwrap_or("");
        let ok = |token: &Option<String>| {
            token
                .as_ref()
                .is_none_or(|t| constant_time_eq(t.as_bytes(), given.as_bytes()))
        };
        let allowed = match side {
            Side::Client => ok(&self.cfg.client_token),
            Side::Agent => ok(&self.cfg.agent_token),
            Side::Either => ok(&self.cfg.client_token) || ok(&self.cfg.agent_token),
        };
        if allowed {
            Ok(())
        } else {
            Err(ApiError::Unauthorized)
        }
    }
}

fn secret(headers: &HeaderMap) -> &str {
    headers
        .get("x-agent-secret")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("")
}

#[derive(Deserialize)]
struct Wait {
    #[serde(default)]
    wait: f64,
}

fn deadline(wait: f64) -> tokio::time::Instant {
    tokio::time::Instant::now() + Duration::from_secs_f64(wait.clamp(0.0, MAX_WAIT))
}

async fn put_blob(
    State(s): App,
    headers: HeaderMap,
    body: Bytes,
) -> Result<Json<Created>, ApiError> {
    s.check(&headers, Side::Either)?;
    Ok(Json(Created {
        id: s.pool().put_blob(body),
    }))
}

async fn get_blob(
    State(s): App,
    headers: HeaderMap,
    Path(hash): Path<String>,
) -> Result<Bytes, ApiError> {
    s.check(&headers, Side::Either)?;
    s.pool().blobs.get(&hash).cloned().ok_or(ApiError::NotFound)
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

async fn get_job(
    State(s): App,
    headers: HeaderMap,
    Path(id): Path<String>,
    Query(q): Query<Wait>,
) -> Result<Json<JobView>, ApiError> {
    s.check(&headers, Side::Client)?;
    let until = deadline(q.wait);
    loop {
        let changed = s.jobs_changed.notified();
        let view = s.pool().job(&id).ok_or(ApiError::NotFound)?;
        if view.status != JobStatus::Pending
            || tokio::time::timeout_at(until, changed).await.is_err()
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
    headers: HeaderMap,
    Json(req): Json<Register>,
) -> Result<Json<Registered>, ApiError> {
    s.check(&headers, Side::Agent)?;
    Ok(Json(s.pool().register(req)?))
}

async fn heartbeat(
    State(s): App,
    headers: HeaderMap,
    Path(id): Path<String>,
) -> Result<StatusCode, ApiError> {
    s.check(&headers, Side::Agent)?;
    s.pool().heartbeat(&id, secret(&headers))?;
    Ok(StatusCode::NO_CONTENT)
}

async fn lease(
    State(s): App,
    headers: HeaderMap,
    Path(id): Path<String>,
    Query(q): Query<Wait>,
) -> Result<Response, ApiError> {
    s.check(&headers, Side::Agent)?;
    let until = deadline(q.wait);
    loop {
        let changed = s.tasks_changed.notified();
        if let Some(lease) = s.pool().lease(&id, secret(&headers))? {
            return Ok(Json(lease).into_response());
        }
        if tokio::time::timeout_at(until, changed).await.is_err() {
            return Ok(StatusCode::NO_CONTENT.into_response());
        }
    }
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
    // A report may finish a job, re-queue a task or add a replica.
    s.jobs_changed.notify_waiters();
    s.tasks_changed.notify_waiters();
    Ok(StatusCode::NO_CONTENT)
}

pub async fn serve(listener: TcpListener, cfg: Config) -> anyhow::Result<()> {
    let shared = Arc::new(Shared {
        pool: Mutex::default(),
        cfg,
        tasks_changed: Notify::new(),
        jobs_changed: Notify::new(),
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
    let app = Router::new()
        .route("/v1/blobs", post(put_blob))
        .route("/v1/blobs/{hash}", get(get_blob))
        .route("/v1/jobs", post(submit))
        .route("/v1/jobs/{id}", get(get_job))
        .route("/v1/agents", get(list_agents).post(register))
        .route("/v1/agents/{id}/heartbeat", post(heartbeat))
        .route("/v1/agents/{id}/lease", get(lease))
        .route("/v1/agents/{id}/tasks/{task}", post(report))
        .layer(DefaultBodyLimit::max(MAX_BLOB))
        .with_state(shared);
    axum::serve(listener, app).await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn pool_with_job(replicas: u32, agents: usize) -> (Pool, String, Vec<(String, String)>) {
        let mut pool = Pool::default();
        let module = pool.put_blob(Bytes::from_static(b"m"));
        let input = pool.put_blob(Bytes::from_static(b"i"));
        let agents = (0..agents)
            .map(|i| {
                let r = pool
                    .register(Register {
                        name: format!("a{i}"),
                        caps: Caps::default(),
                        protocol: PROTOCOL.into(),
                    })
                    .unwrap();
                (r.agent_id, r.secret)
            })
            .collect();
        let job = pool
            .submit(JobSpec {
                module,
                input,
                fuel: 100,
                memory_mb: 1,
                replicas,
            })
            .unwrap();
        (pool, job, agents)
    }

    fn ok(pool: &mut Pool, data: &'static [u8]) -> Outcome {
        Outcome::Ok {
            output: pool.put_blob(Bytes::from_static(data)),
            fuel_used: 7,
        }
    }

    fn run(pool: &mut Pool, (id, secret): &(String, String), outcome: Outcome) {
        let lease = pool.lease(id, secret).unwrap().expect("a task to lease");
        pool.report(&lease.task_id, id, secret, outcome).unwrap();
    }

    #[test]
    fn replicas_go_to_distinct_devices() {
        let (mut pool, _, agents) = pool_with_job(2, 2);
        let (a, s) = &agents[0];
        assert!(pool.lease(a, s).unwrap().is_some());
        assert!(pool.lease(a, s).unwrap().is_none());
        assert!(pool.lease(&agents[1].0, &agents[1].1).unwrap().is_some());
    }

    #[test]
    fn disagreement_adds_a_replica_and_strikes_the_liar() {
        let (mut pool, job, agents) = pool_with_job(2, 3);
        let good = ok(&mut pool, b"right");
        let bad = ok(&mut pool, b"wrong");
        run(&mut pool, &agents[0], good.clone());
        run(&mut pool, &agents[1], bad);
        assert_eq!(pool.job(&job).unwrap().status, JobStatus::Pending);
        run(&mut pool, &agents[2], good.clone());
        let view = pool.job(&job).unwrap();
        assert_eq!((view.status, view.outcome), (JobStatus::Done, Some(good)));
        let strikes: HashMap<_, _> = pool
            .agents(&Config::default())
            .into_iter()
            .map(|a| (a.id, (a.strikes, a.credits)))
            .collect();
        assert_eq!(strikes[&agents[0].0], (0, 7));
        assert_eq!(strikes[&agents[1].0], (1, 0));
    }

    #[test]
    fn no_quorum_after_extra_replicas_fails_the_job() {
        let (mut pool, job, agents) = pool_with_job(2, 4);
        for (i, agent) in agents.iter().enumerate() {
            let out = Outcome::Ok {
                output: pool.put_blob(Bytes::from(vec![i as u8])),
                fuel_used: 1,
            };
            run(&mut pool, agent, out);
        }
        assert_eq!(pool.job(&job).unwrap().status, JobStatus::Failed);
    }

    #[test]
    fn deterministic_failures_are_voted_on() {
        let (mut pool, job, agents) = pool_with_job(2, 2);
        let failed = Outcome::Failed {
            kind: FailKind::OutOfFuel,
            detail: "x".into(),
        };
        run(&mut pool, &agents[0], failed.clone());
        // Details (trap messages) may differ between devices; only the kind is voted on.
        run(
            &mut pool,
            &agents[1],
            Outcome::Failed {
                kind: FailKind::OutOfFuel,
                detail: "y".into(),
            },
        );
        assert_eq!(pool.job(&job).unwrap().status, JobStatus::Done);
    }

    #[test]
    fn abandoned_tasks_go_to_another_device_and_banned_devices_get_nothing() {
        let (mut pool, _, agents) = pool_with_job(1, 2);
        run(
            &mut pool,
            &agents[0],
            Outcome::Abandoned {
                reason: "bye".into(),
            },
        );
        assert!(pool.lease(&agents[0].0, &agents[0].1).unwrap().is_none());
        assert!(pool.lease(&agents[1].0, &agents[1].1).unwrap().is_some());
        pool.agents.get_mut(&agents[0].0).unwrap().strikes = MAX_STRIKES;
        assert!(matches!(
            pool.lease(&agents[0].0, &agents[0].1),
            Err(ApiError::Forbidden(_))
        ));
        assert_eq!(
            pool.heartbeat(&agents[1].0, "wrong"),
            Err(ApiError::Unauthorized)
        );
    }

    #[test]
    fn leases_of_silent_devices_are_requeued() {
        let (mut pool, _, agents) = pool_with_job(1, 2);
        pool.lease(&agents[0].0, &agents[0].1).unwrap().unwrap();
        pool.agents.get_mut(&agents[0].0).unwrap().last_seen -= Duration::from_secs(60);
        pool.sweep(&Config::default());
        assert!(pool.lease(&agents[1].0, &agents[1].1).unwrap().is_some());
    }
}
