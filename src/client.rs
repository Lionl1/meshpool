//! HTTP client for the coordinator API, used by agents, the CLI and tests.

use std::time::Duration;

use anyhow::{Result, bail};
use axum::body::Bytes;
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

const RETRIES: u32 = 5;

/// Retry a request that is safe to repeat while the network, not the coordinator, fails
/// (dropped connection, reset, timeout): normal on the internet.
async fn retry<T, F, Fut>(mut call: F) -> Result<T>
where
    F: FnMut(u32) -> Fut,
    Fut: std::future::Future<Output = Result<T>>,
{
    let mut attempt = 0;
    loop {
        match call(attempt).await {
            Err(e) if status_of(&e).is_none() && attempt + 1 < RETRIES => {
                tokio::time::sleep(Duration::from_millis(100 << attempt)).await;
                attempt += 1;
            }
            result => return result,
        }
    }
}

/// A report that was retried may have arrived the first time: 409 then means "already done".
fn reported(result: Result<Response>, attempt: u32) -> Result<()> {
    match result {
        Err(e) if attempt > 0 && status_of(&e) == Some(StatusCode::CONFLICT) => Ok(()),
        other => other.map(|_| ()),
    }
}

/// An agent's identity, sent with every agent call.
#[derive(Clone, Debug)]
pub struct AgentAuth {
    pub id: String,
    pub secret: String,
}

impl Client {
    pub fn new(base: &str, token: Option<String>) -> Self {
        Self::with_root_cert(base, token, None).expect("http client")
    }

    /// A client that also trusts `ca_pem` (PEM), e.g. a pool's self-signed certificate.
    pub fn with_root_cert(
        base: &str,
        token: Option<String>,
        ca_pem: Option<&[u8]>,
    ) -> Result<Self> {
        let mut http = reqwest::Client::builder().timeout(Duration::from_secs(120));
        if let Some(pem) = ca_pem {
            http = http.add_root_certificate(reqwest::Certificate::from_pem(pem)?);
        }
        Ok(Client {
            base: base.trim_end_matches('/').into(),
            token,
            http: http.build()?,
        })
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
        let data = Bytes::from(data); // cheap to clone for retries
        let created: Created = retry(|_| {
            Self::json(
                self.req(reqwest::Method::POST, "/v1/blobs")
                    .body(data.clone()),
            )
        })
        .await?;
        Ok(created.id)
    }

    /// Fetch a blob and check that it matches its hash.
    pub async fn get_blob(&self, hash: &str) -> Result<Vec<u8>> {
        self.fetch_blob(hash, None).await
    }

    /// Fetch a blob as a registered device (devices may read blobs without a client token).
    pub async fn get_blob_as(&self, auth: &AgentAuth, hash: &str) -> Result<Vec<u8>> {
        self.fetch_blob(hash, Some(auth)).await
    }

    async fn fetch_blob(&self, hash: &str, auth: Option<&AgentAuth>) -> Result<Vec<u8>> {
        let data = retry(|_| async {
            let mut req = self.req(reqwest::Method::GET, &format!("/v1/blobs/{hash}"));
            if let Some(auth) = auth {
                req = req
                    .header("x-agent-id", &auth.id)
                    .header("x-agent-secret", &auth.secret);
            }
            Ok(Self::send(req).await?.bytes().await?.to_vec())
        })
        .await?;
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

    /// Submit many jobs in one request; ids come back in the same order.
    pub async fn submit_batch(&self, specs: &[JobSpec]) -> Result<Vec<String>> {
        Self::json(
            self.req(reqwest::Method::POST, "/v1/jobs/batch")
                .json(specs),
        )
        .await
    }

    pub async fn job(&self, id: &str, wait: Duration) -> Result<JobView> {
        let path = format!("/v1/jobs/{id}?wait={}", wait.as_secs_f64());
        retry(|_| Self::json(self.req(reqwest::Method::GET, &path))).await
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
            Some(Outcome::Failed { kind, detail, .. }) => bail!(
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

    /// Liveness, renewing the leases of the tasks the device is still working on.
    pub async fn heartbeat(&self, auth: &AgentAuth, working_on: Vec<String>) -> Result<()> {
        let beat = Heartbeat { tasks: working_on };
        let req = self.agent_req(reqwest::Method::POST, auth, "/heartbeat");
        Self::send(req.json(&beat)).await.map(|_| ())
    }

    /// Report a successful run: the output travels in the same request.
    pub async fn report_output(
        &self,
        auth: &AgentAuth,
        task_id: &str,
        output: Vec<u8>,
        fuel_used: u64,
    ) -> Result<()> {
        let path = format!("/tasks/{task_id}/output?fuel_used={fuel_used}");
        let output = Bytes::from(output);
        let (path, output) = (&path, &output);
        retry(|attempt| async move {
            let req = self.agent_req(reqwest::Method::POST, auth, path);
            reported(Self::send(req.body(output.clone())).await, attempt)
        })
        .await
    }

    /// Up to `max` tasks, waiting up to `wait` for the first one.
    pub async fn lease(&self, auth: &AgentAuth, wait: Duration, max: usize) -> Result<Vec<Lease>> {
        let path = format!("/lease?wait={}&max={max}", wait.as_secs_f64());
        Self::json(self.agent_req(reqwest::Method::GET, auth, &path)).await
    }

    pub async fn report(&self, auth: &AgentAuth, task_id: &str, outcome: &Outcome) -> Result<()> {
        let path = &format!("/tasks/{task_id}");
        retry(|attempt| async move {
            let req = self.agent_req(reqwest::Method::POST, auth, path);
            reported(Self::send(req.json(outcome)).await, attempt)
        })
        .await
    }
}
