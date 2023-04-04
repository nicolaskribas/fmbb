use mqtt::{ConnectOptionsBuilder, CreateOptionsBuilder, Message};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::time::SystemTime;
use std::{env, process, vec};
use tokio::sync::Notify;
use tokio::time::{self, sleep, Duration};
use tokio_util::sync::CancellationToken;

use paho_mqtt as mqtt;

static TOPIC: &str = "federated/benchmark";
static QOS: i32 = 2;
static MAX_BUFFERED: i32 = 0;

#[tokio::main]
async fn main() {
    let Some(config_file) = env::args().nth(1) else {
        eprintln!("No configuration file provided!");
        process::exit(1);
    };

    let config = config::load(&config_file).unwrap_or_else(|e| {
        eprintln!("Error in configuration file: {e}");
        process::exit(1);
    });

    let start_publishing = Arc::new(Notify::new());
    let stop_receiving = CancellationToken::new();

    let mut pubs = Vec::new();

    let mut id = 0;
    for batch in &config.publishers {
        for _ in 0..batch.clients {
            let task = publisher_task(
                batch.broker.clone(),
                id,
                batch.count,
                Duration::from_nanos(1_000_000_000 / batch.rate),
                start_publishing.clone(),
            );
            pubs.push(tokio::spawn(task));
            id += batch.count;
        }
    }
    let total_messages = id;

    let mut subs = Vec::new();

    for batch in &config.subscribers {
        for _ in 0..batch.clients {
            let task =
                subscriber_task(batch.broker.clone(), total_messages, stop_receiving.clone());
            subs.push(tokio::spawn(task));
        }
    }

    // wait some time and then start publishers
    sleep(Duration::from_secs(config.wait)).await;
    start_publishing.notify_waiters();

    for publisher in pubs {
        let res = publisher.await.unwrap().unwrap();
        println!(
            "Published {} messages with success, {} failed",
            res.0, res.1
        );
    }

    // give subscribers some time to receive all publications
    sleep(Duration::from_secs(5)).await;
    stop_receiving.cancel();

    for subscriber in subs {
        let res = subscriber.await.unwrap().unwrap();
        println!("Subscriber was expecting {} messages, received {} unique messages, {} duplicates, latency: {}ms", res.0, res.1, res.2, res.3.as_millis());
    }
}

#[derive(Serialize, Deserialize)]
struct Payload {
    pub id: usize,
    pub creation: SystemTime,
}

async fn publisher_task(
    broker: String,
    start_id: usize,
    count: usize,
    interval: Duration,
    start: Arc<Notify>,
) -> Result<(u32, u32), mqtt::Error> {
    let cli_opts = CreateOptionsBuilder::new()
        .server_uri(broker)
        .max_buffered_messages(MAX_BUFFERED)
        .finalize();
    let client = mqtt::AsyncClient::new(cli_opts)?;

    let conn_opts = ConnectOptionsBuilder::new().finalize(); // connection timeout maybe?
    client.connect(conn_opts).await?;

    start.notified().await;

    let mut succ = 0;
    let mut fail = 0;
    let mut interval = time::interval(interval);

    for id in start_id..start_id + count {
        interval.tick().await;

        let payload = Payload {
            id,
            creation: SystemTime::now(),
        };

        let payload = bincode::serialize(&payload).expect("serialization should behave nicely");
        let message = Message::new(TOPIC, payload, QOS);

        match client.try_publish(message)?.await {
            Ok(()) => succ += 1,
            Err(_) => fail += 1,
        }
    }

    // we waited for every message to be pulished so there is no messages in flight
    // disconenct immediately
    client.disconnect(None).await?;

    Ok((succ, fail))
}

async fn subscriber_task(
    broker: String,
    expecting: usize,
    receiving: CancellationToken,
) -> Result<(usize, u32, u32, Duration), mqtt::Error> {
    let mut latencies = Vec::with_capacity(expecting);
    let mut cache = vec![false; expecting];
    let mut dup = 0;
    let mut received = 0;

    let cli_opts = CreateOptionsBuilder::new()
        .server_uri(broker)
        .max_buffered_messages(MAX_BUFFERED)
        .finalize();
    let mut client = mqtt::AsyncClient::new(cli_opts)?;

    let stream = client.get_stream(None); // None = unbound channel

    let conn_opts = ConnectOptionsBuilder::new().finalize(); // connection timeout maybe?
    client.connect(conn_opts).await?;
    client.subscribe(TOPIC, QOS).await?;

    loop {
        tokio::select! {
            biased;

            _ = receiving.cancelled() => break,

            message = stream.recv() => {
                if let Some(message) = message.expect("client doesn't close the channel") {
                    let Payload { id, creation } = bincode::deserialize(message.payload())
                        .expect("receiving only messaged that our publishers produced");

                    if cache[id] {
                        dup += 1;
                    } else {
                        cache[id] = true;
                        received += 1;
                    }

                    match creation.elapsed() {
                        Ok(latency) => latencies.push(latency),
                        Err(err) => println!("time error {}", err),
                    }
                } else {
                    panic!("subscriber lost connection");
                }
            },
        }
    }

    client.disconnect(None).await?;

    let mean = latencies.iter().sum::<Duration>() / latencies.len() as u32;
    Ok((expecting, received, dup, mean))
}

mod config {
    use config::{Config, ConfigError, File, FileFormat};
    use serde::Deserialize;

    #[derive(Deserialize)]
    pub struct Settings {
        pub wait: u64, // time waited before starting publishing
        pub publishers: Vec<PubBatch>,
        pub subscribers: Vec<SubBatch>,
    }

    #[derive(Deserialize)]
    pub struct PubBatch {
        pub broker: String,
        pub clients: u32,
        pub rate: u64,    // messages per second
        pub count: usize, // total of messages to publish
    }

    #[derive(Deserialize)]
    pub struct SubBatch {
        pub broker: String,
        pub clients: u32,
    }

    pub fn load(file: &str) -> Result<Settings, ConfigError> {
        let conf = Config::builder()
            .add_source(File::new(file, FileFormat::Toml))
            .build()?;
        conf.try_deserialize()
    }
}
