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
    tokio::spawn(coordinator::serve(listener, cfg, None));
    url
}

/// Quick timeouts; every device runs on 127.0.0.1, so the per-address rules are off.
fn fast() -> Config {
    Config {
        agent_timeout: Duration::from_millis(600),
        distinct_ips: false,
        ..Config::default()
    }
}

async fn honest_agent(url: &str, name: &str) {
    let agent = Agent::connect(Client::new(url, None), name, Duration::from_millis(100), 1)
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
        let lease = client.lease(&auth, WAIT, 1).await.unwrap().remove(0);
        let forged = b"forged".to_vec();
        client
            .report_output(&auth, &lease.task_id, forged, 1)
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
    assert_eq!(client.lease(&ghost, WAIT, 1).await.unwrap().len(), 1); // and never heard of again
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
    // Devices cannot use the coordinator as free storage: uploads are for clients.
    assert_eq!(status(devices.put_blob(vec![0; 1024]).await), Some(401));
    let clients = Client::new(&url, Some("client".into()));
    let hash = clients.put_blob(b"data".to_vec()).await.unwrap();
    assert_eq!(devices.get_blob_as(&auth, &hash).await.unwrap(), b"data");
    assert_eq!(status(devices.get_blob(&hash).await), Some(401));
    auth.secret = "stolen-id-wrong-secret".into();
    assert_eq!(status(devices.heartbeat(&auth, vec![]).await), Some(401));
    assert_eq!(status(devices.get_blob_as(&auth, &hash).await), Some(401));
}

#[tokio::test]
async fn a_task_longer_than_the_lease_timeout_stays_with_its_live_device() {
    let cfg = Config {
        lease_timeout: Duration::from_millis(150),
        ..fast()
    };
    let url = coordinator(cfg).await;
    let client = Client::new(&url, None);
    let n: u32 = 6_000_000; // about a second: several lease timeouts
    let job = submit(&client, "primes.wat", &n.to_le_bytes(), 1, u64::MAX / 2).await;
    honest_agent(&url, "a").await;
    tokio::time::sleep(Duration::from_millis(100)).await;
    honest_agent(&url, "b").await; // would steal the task if the lease expired
    let view = client.wait(&job, WAIT).await.unwrap();
    assert_eq!(view.votes.len(), 1, "the task ran once: {:?}", view.votes);
    assert_eq!(
        client.output(&view).await.unwrap(),
        412_849u32.to_le_bytes()
    );
}

#[tokio::test]
async fn a_multi_slot_device_runs_a_batch_in_parallel() {
    let url = coordinator(fast()).await;
    let agent = Agent::connect(
        Client::new(&url, None),
        "big",
        Duration::from_millis(100),
        4,
    )
    .await
    .unwrap();
    tokio::spawn(agent.run());
    let client = Client::new(&url, None);
    let path = Path::new(env!("CARGO_MANIFEST_DIR")).join("examples/tasks/reverse.wat");
    let module = client
        .put_blob(meshpool::load_module(&path).unwrap())
        .await
        .unwrap();
    let mut specs = Vec::new();
    for i in 0..8u8 {
        specs.push(JobSpec {
            module: module.clone(),
            input: client.put_blob(vec![b'a', b'0' + i]).await.unwrap(),
            fuel: 1_000_000,
            memory_mb: 16,
            replicas: 1,
        });
    }
    let ids = client.submit_batch(&specs).await.unwrap();
    for (i, id) in ids.iter().enumerate() {
        let view = client.wait(id, WAIT).await.unwrap();
        assert_eq!(client.output(&view).await.unwrap(), [b'0' + i as u8, b'a']);
    }
}

#[tokio::test]
async fn pool_over_tls() {
    let rcgen::CertifiedKey { cert, signing_key } =
        rcgen::generate_simple_self_signed(vec!["localhost".into()]).unwrap();
    let tls = coordinator::Tls {
        cert_pem: cert.pem().into_bytes(),
        key_pem: signing_key.serialize_pem().into_bytes(),
    };
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!(
        "https://localhost:{}",
        listener.local_addr().unwrap().port()
    );
    tokio::spawn(coordinator::serve(listener, fast(), Some(tls)));
    let ca = cert.pem().into_bytes();
    let client = |ca: Option<&[u8]>| Client::with_root_cert(&url, None, ca).unwrap();

    for name in ["a", "b"] {
        let agent = Agent::connect(client(Some(&ca)), name, Duration::from_millis(100), 1)
            .await
            .unwrap();
        tokio::spawn(agent.run());
    }
    let job = submit(&client(Some(&ca)), "reverse.wat", b"secret", 2, 1_000_000).await;
    let view = client(Some(&ca)).wait(&job, WAIT).await.unwrap();
    assert_eq!(client(Some(&ca)).output(&view).await.unwrap(), b"terces");
    // Without the pool's certificate the connection is refused, not downgraded.
    assert!(client(None).agents().await.is_err());
}

#[tokio::test]
async fn pool_survives_a_coordinator_restart() {
    let dir = std::env::temp_dir().join(format!("meshpool-{}", uuid::Uuid::new_v4()));
    let cfg = Config {
        data_dir: Some(dir.clone()),
        ..fast()
    };
    let start = |cfg: Config| async move {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        (url, tokio::spawn(coordinator::serve(listener, cfg, None)))
    };

    // Before: one job done, one waiting for devices that are gone.
    let (url, first) = start(cfg.clone()).await;
    let mut agents = Vec::new();
    for name in ["a", "b"] {
        let agent = Agent::connect(Client::new(&url, None), name, Duration::from_millis(100), 1)
            .await
            .unwrap();
        agents.push(tokio::spawn(agent.run()));
    }
    let client = Client::new(&url, None);
    let done = submit(&client, "reverse.wat", b"kept", 2, 1_000_000).await;
    client.wait(&done, WAIT).await.unwrap();
    for agent in agents {
        agent.abort();
    }
    let waiting = submit(&client, "reverse.wat", b"later", 2, 1_000_000).await;
    tokio::time::sleep(coordinator::SAVE_EVERY * 2).await;
    first.abort();
    let _ = first.await;

    // After: the same data directory on a new address.
    let (url, _second) = start(cfg).await;
    let client = Client::new(&url, None);
    let view = client.job(&done, Duration::ZERO).await.unwrap();
    assert_eq!(client.output(&view).await.unwrap(), b"tpek");
    assert!(client.agents().await.unwrap().iter().all(|a| a.credits > 0));
    for name in ["c", "d"] {
        honest_agent(&url, name).await;
    }
    let view = client.wait(&waiting, WAIT).await.unwrap();
    assert_eq!(client.output(&view).await.unwrap(), b"retal");
    std::fs::remove_dir_all(dir).unwrap();
}
