//! HTTP client for the coordinator API, used by agents, the CLI and tests.

use std::time::Duration;

use anyhow::{Context, Result, bail};
use reqwest::{RequestBuilder, Response, StatusCode};
use serde::de::DeserializeOwned;

use crate::proto::*;

#[derive(Clone)]
pub struct Client {
    base: String,
    token: Option<String>,
    http: reqwest::Client,
}

/// An error status from the coordinator (kept so callers can react to 401/403/404).
#[derive(Debug)]
pub struct HttpError {
    pub status: StatusCode,
    pub body: String,
}

impl std::fmt::Display for HttpError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "coordinator returned {}: {}", self.status, self.body)
    }
}

impl std::error::Error for HttpError {}

pub fn status_of(err: &anyhow::Error) -> Option<StatusCode> {
    err.downcast_ref::<HttpError>().map(|e| e.status)
}

/// An agent's identity, sent with every agent call.
#[derive(Clone, Debug)]
pub struct AgentAuth {
    pub id: String,
    pub secret: String,
}

impl Client {
    pub fn new(base: &str, token: Option<String>) -> Self {
        let http = reqwest::Client::builder()
            .timeout(Duration::from_secs(120))
            .build()
            .expect("http client");
        Client {
            base: base.trim_end_matches('/').into(),
            token,
            http,
        }
    }

    fn req(&self, method: reqwest::Method, path: &str) -> RequestBuilder {
        let req = self.http.request(method, format!("{}{path}", self.base));
        match &self.token {
            Some(t) => req.bearer_auth(t),
            None => req,
        }
    }

    fn agent_req(&self, method: reqwest::Method, auth: &AgentAuth, path: &str) -> RequestBuilder {
        self.req(method, &format!("/v1/agents/{}{path}", auth.id))
            .header("x-agent-secret", &auth.secret)
    }

    async fn send(req: RequestBuilder) -> Result<Response> {
        let resp = req.send().await?;
        if resp.status().is_success() {
            return Ok(resp);
        }
        let status = resp.status();
        Err(HttpError {
            status,
            body: resp.text().await.unwrap_or_default(),
        }
        .into())
    }

    async fn json<T: DeserializeOwned>(req: RequestBuilder) -> Result<T> {
        Ok(Self::send(req).await?.json().await?)
    }

    pub async fn put_blob(&self, data: Vec<u8>) -> Result<String> {
        let created: Created =
            Self::json(self.req(reqwest::Method::POST, "/v1/blobs").body(data)).await?;
        Ok(created.id)
    }

    /// Fetch a blob and check that it matches its hash.
    pub async fn get_blob(&self, hash: &str) -> Result<Vec<u8>> {
        let data = Self::send(self.req(reqwest::Method::GET, &format!("/v1/blobs/{hash}")))
            .await?
            .bytes()
            .await?
            .to_vec();
        if blob_hash(&data) != hash {
            bail!("blob {hash} does not match its hash");
        }
        Ok(data)
    }

    pub async fn submit(&self, spec: &JobSpec) -> Result<String> {
        let created: Created =
            Self::json(self.req(reqwest::Method::POST, "/v1/jobs").json(spec)).await?;
        Ok(created.id)
    }

    pub async fn job(&self, id: &str, wait: Duration) -> Result<JobView> {
        Self::json(self.req(
            reqwest::Method::GET,
            &format!("/v1/jobs/{id}?wait={}", wait.as_secs_f64()),
        ))
        .await
    }

    /// Wait until the job is no longer pending (or `timeout` passes).
    pub async fn wait(&self, id: &str, timeout: Duration) -> Result<JobView> {
        let until = tokio::time::Instant::now() + timeout;
        loop {
            let left = until.saturating_duration_since(tokio::time::Instant::now());
            let view = self.job(id, left.min(Duration::from_secs(30))).await?;
            if view.status != JobStatus::Pending || left.is_zero() {
                return Ok(view);
            }
        }
    }

    /// The output of a finished job, or an error describing why there is none.
    pub async fn output(&self, view: &JobView) -> Result<Vec<u8>> {
        match &view.outcome {
            Some(Outcome::Ok { output, .. }) => self.get_blob(output).await,
            Some(Outcome::Failed { kind, detail }) => bail!(
                "job {} failed on every replica: {kind:?} ({detail})",
                view.id
            ),
            _ => bail!(
                "job {} has no result: {:?} {}",
                view.id,
                view.status,
                view.error.as_deref().unwrap_or("")
            ),
        }
    }

    pub async fn agents(&self) -> Result<Vec<AgentView>> {
        Self::json(self.req(reqwest::Method::GET, "/v1/agents")).await
    }

    pub async fn register(&self, name: &str, caps: Caps) -> Result<AgentAuth> {
        let req = Register {
            name: name.into(),
            caps,
            protocol: PROTOCOL.into(),
        };
        let r: Registered =
            Self::json(self.req(reqwest::Method::POST, "/v1/agents").json(&req)).await?;
        Ok(AgentAuth {
            id: r.agent_id,
            secret: r.secret,
        })
    }

    pub async fn heartbeat(&self, auth: &AgentAuth) -> Result<()> {
        Self::send(self.agent_req(reqwest::Method::POST, auth, "/heartbeat"))
            .await
            .map(|_| ())
    }

    pub async fn lease(&self, auth: &AgentAuth, wait: Duration) -> Result<Option<Lease>> {
        let req = self.agent_req(
            reqwest::Method::GET,
            auth,
            &format!("/lease?wait={}", wait.as_secs_f64()),
        );
        let resp = Self::send(req).await?;
        if resp.status() == StatusCode::NO_CONTENT {
            return Ok(None);
        }
        Ok(Some(resp.json().await.context("bad lease")?))
    }

    pub async fn report(&self, auth: &AgentAuth, task_id: &str, outcome: &Outcome) -> Result<()> {
        let req = self
            .agent_req(reqwest::Method::POST, auth, &format!("/tasks/{task_id}"))
            .json(outcome);
        Self::send(req).await.map(|_| ())
    }
}
