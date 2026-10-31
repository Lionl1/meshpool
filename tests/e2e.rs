//! End to end: a real coordinator and agents over HTTP, including devices that lie or vanish.

use std::path::Path;
use std::time::Duration;

use meshpool::agent::Agent;
use meshpool::client::{Client, status_of};
use meshpool::coordinator::{self, Config};
use meshpool::proto::*;

const WAIT: Duration = Duration::from_secs(30);

async fn coordinator(cfg: Config) -> String {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    tokio::spawn(coordinator::serve(listener, cfg));
    url
}

fn fast() -> Config {
    Config {
        agent_timeout: Duration::from_millis(600),
        ..Config::default()
    }
}

async fn honest_agent(url: &str, name: &str) {
    let agent = Agent::connect(Client::new(url, None), name, Duration::from_millis(100))
        .await
        .unwrap();
    tokio::spawn(agent.run());
}

/// Takes one task, runs nothing and reports a made-up output.
async fn lying_agent(url: &str) -> String {
    let client = Client::new(url, None);
    let auth = client.register("liar", Caps::default()).await.unwrap();
    let id = auth.id.clone();
    tokio::spawn(async move {
        let lease = client.lease(&auth, WAIT).await.unwrap().unwrap();
        let output = client.put_blob(b"forged".to_vec()).await.unwrap();
        client
            .report(
                &auth,
                &lease.task_id,
                &Outcome::Ok {
                    output,
                    fuel_used: 1,
                },
            )
            .await
            .unwrap();
    });
    id
}

async fn submit(client: &Client, task: &str, input: &[u8], replicas: u32, fuel: u64) -> String {
    let path = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("examples/tasks")
        .join(task);
    let module = match task {
        "garbage" => b"not wasm".to_vec(),
        _ => meshpool::load_module(&path).unwrap(),
    };
    let spec = JobSpec {
        module: client.put_blob(module).await.unwrap(),
        input: client.put_blob(input.to_vec()).await.unwrap(),
        fuel,
        memory_mb: 16,
        replicas,
    };
    client.submit(&spec).await.unwrap()
}

#[tokio::test]
async fn replicas_agree_and_earn_credits() {
    let url = coordinator(fast()).await;
    for name in ["a", "b", "c"] {
        honest_agent(&url, name).await;
    }
    let client = Client::new(&url, None);
    let job = submit(&client, "reverse.wat", b"hello, pool", 3, 1_000_000).await;
    let view = client.wait(&job, WAIT).await.unwrap();
    assert_eq!(client.output(&view).await.unwrap(), b"loop ,olleh");
    let credited = client
        .agents()
        .await
        .unwrap()
        .iter()
        .filter(|a| a.credits > 0)
        .count();
    assert!(credited >= 2, "the quorum is paid");
}

#[tokio::test]
async fn running_out_of_fuel_is_an_agreed_result() {
    let url = coordinator(fast()).await;
    honest_agent(&url, "a").await;
    honest_agent(&url, "b").await;
    let client = Client::new(&url, None);
    let job = submit(&client, "spin.wat", b"", 2, 100_000).await;
    let view = client.wait(&job, WAIT).await.unwrap();
    assert_eq!(view.status, JobStatus::Done);
    assert!(matches!(
        view.outcome,
        Some(Outcome::Failed {
            kind: FailKind::OutOfFuel,
            ..
        })
    ));
    assert!(client.output(&view).await.is_err());

    let job = submit(&client, "garbage", b"", 2, 100_000).await;
    let view = client.wait(&job, WAIT).await.unwrap();
    assert!(matches!(
        view.outcome,
        Some(Outcome::Failed {
            kind: FailKind::InvalidModule,
            ..
        })
    ));
}

#[tokio::test]
async fn a_lying_device_is_outvoted_and_struck() {
    let url = coordinator(fast()).await;
    let client = Client::new(&url, None);
    let liar = lying_agent(&url).await;
    honest_agent(&url, "a").await;
    let job = submit(&client, "reverse.wat", b"abc", 2, 1_000_000).await;
    // The liar and "a" disagree: a third replica is added, which needs a third device.
    while client.job(&job, Duration::ZERO).await.unwrap().votes.len() < 2 {
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert_eq!(
        client.job(&job, Duration::ZERO).await.unwrap().status,
        JobStatus::Pending
    );
    honest_agent(&url, "b").await;
    let view = client.wait(&job, WAIT).await.unwrap();
    assert_eq!(client.output(&view).await.unwrap(), b"cba");
    let liar = client
        .agents()
        .await
        .unwrap()
        .into_iter()
        .find(|a| a.id == liar)
        .unwrap();
    assert_eq!((liar.strikes, liar.credits), (1, 0));
}

#[tokio::test]
async fn work_of_a_vanished_device_moves_to_another() {
    let url = coordinator(fast()).await;
    let client = Client::new(&url, None);
    let ghost = client.register("ghost", Caps::default()).await.unwrap();
    let job = submit(&client, "reverse.wat", b"xy", 1, 1_000_000).await;
    assert!(client.lease(&ghost, WAIT).await.unwrap().is_some()); // and never heard of again
    honest_agent(&url, "a").await;
    let view = client.wait(&job, WAIT).await.unwrap();
    assert_eq!(client.output(&view).await.unwrap(), b"yx");
}

fn status<T>(r: anyhow::Result<T>) -> Option<u16> {
    r.err().and_then(|e| status_of(&e)).map(|s| s.as_u16())
}

#[tokio::test]
async fn tokens_and_device_secrets_are_enforced() {
    let cfg = Config {
        client_token: Some("client".into()),
        agent_token: Some("devices".into()),
        ..fast()
    };
    let url = coordinator(cfg).await;
    assert_eq!(
        status(Client::new(&url, None).register("x", Caps::default()).await),
        Some(401)
    );
    assert_eq!(
        status(
            Client::new(&url, Some("client".into()))
                .register("x", Caps::default())
                .await
        ),
        Some(401)
    );
    let devices = Client::new(&url, Some("devices".into()));
    let mut auth = devices.register("x", Caps::default()).await.unwrap();
    assert_eq!(status(devices.agents().await), Some(401));
    auth.secret = "stolen-id-wrong-secret".into();
    assert_eq!(status(devices.heartbeat(&auth).await), Some(401));
}
