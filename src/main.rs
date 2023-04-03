use mqtt::{ConnectOptionsBuilder, CreateOptionsBuilder, Message};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::time::SystemTime;
use std::{env, process, vec};
use tokio::sync::Notify;
use tokio::time::{self, sleep, Duration};

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
    // let stop_receiving = Arc::new(Notify::new());

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

    for batch in &config.subscribers {
        for _ in 0..batch.clients {
            tokio::spawn(subscriber_task(batch.broker.clone(), total_messages));
        }
    }

    // wait some time and then start publishers
    sleep(Duration::from_secs(config.wait)).await;
    start_publishing.notify_waiters();

    for publisher in pubs {
        publisher.await;
    }

    // give subscribers some time to receive all publications
    sleep(Duration::from_secs(5)).await;
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
) -> Result<(), ()> {
    let opts = CreateOptionsBuilder::new()
        .server_uri(broker)
        .max_buffered_messages(MAX_BUFFERED)
        .finalize();
    let client = mqtt::AsyncClient::new(opts).unwrap();

    start.notified().await;

    let mut interval = time::interval(interval);
    for id in start_id..start_id + count {
        interval.tick().await;

        let payload = Payload {
            id,
            creation: SystemTime::now(),
        };

        let payload = bincode::serialize(&payload).unwrap();
        let message = Message::new(TOPIC, payload, QOS);

        client.publish(message);
    }
    Ok(())
}

async fn subscriber_task(broker: String, expecting: usize) -> Result<(), ()> {
    let mut latencies = Vec::with_capacity(expecting);
    let mut cache = vec![false; expecting];
    let mut dup = 0;

    let opts = CreateOptionsBuilder::new().finalize();
    let mut client = mqtt::AsyncClient::new(opts).unwrap();

    let stream = client.get_stream(None); // None = unbound channel

    let conn_opts = ConnectOptionsBuilder::new().finalize();
    client.connect(conn_opts).await.unwrap();
    client.subscribe(TOPIC, QOS).await.unwrap();

    loop {
        let message = stream
            .recv()
            .await
            .expect("client should not close the channel");
        if let Some(message) = message {
            let pl: Payload = bincode::deserialize(message.payload())
                .expect("serialization and deserialization should occur without errors");

            if cache[pl.id] {
                dup += 1;
            } else {
                cache[pl.id] = true;
                break;
            }

            match pl.creation.elapsed() {
                Ok(latency) => {
                    latencies.push(latency);
                }
                Err(err) => println!("time error {}", err),
            }
        } else {
            println!("disconnected");
        }
    }
    Ok(())
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
        pub size: u32,    //size of the message
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
