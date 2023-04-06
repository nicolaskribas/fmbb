use mqtt::{ConnectOptionsBuilder, CreateOptionsBuilder, DeliveryToken, Message};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::time::SystemTime;
use std::{env, process, vec};
use tokio::sync::{mpsc, Barrier};
use tokio::task::JoinHandle;
use tokio::time::{self, sleep, Duration};
use tokio_util::sync::CancellationToken;

use paho_mqtt as mqtt;

static TOPIC: &str = "federated/benchmark";
static QOS: i32 = 2;
static MAX_BUFFERED: i32 = 10000; // 0 = do not buffer offline publications

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

    let stop_receiving = CancellationToken::new();

    let mut pubs = Vec::new();

    let total = config.publishers.iter().fold(0, |x, p| x + p.clients)
        + config.subscribers.iter().fold(0, |x, p| x + p.clients);

    let start_barrier = Arc::new(Barrier::new(total));

    let mut id = 0;
    for batch in &config.publishers {
        for _ in 0..batch.clients {
            let task = publisher_task(
                batch.broker.clone(),
                id,
                batch.count,
                Duration::from_nanos(1_000_000_000 / batch.rate),
                start_barrier.clone(),
                Duration::from_secs(config.wait),
            );
            pubs.push(tokio::spawn(task));
            id += batch.count;
        }
    }
    let total_messages = id;

    let mut subs = Vec::new();
    for batch in &config.subscribers {
        for _ in 0..batch.clients {
            let task = subscriber_task(
                batch.broker.clone(),
                total_messages,
                stop_receiving.clone(),
                start_barrier.clone(),
            );
            subs.push(tokio::spawn(task));
        }
    }

    for publisher in pubs {
        match publisher.await {
            Ok(Ok((succ, fail))) => {
                println!("Published {} messages with success, {} failed", succ, fail);
            }
            Err(e) => {
                eprintln!("Error in publisher: {}!!!", e);
            }
            Ok(Err(e)) => {
                eprintln!("Error in publisher: {}!()!", e);
            }
        }
    }

    stop_receiving.cancel();

    for subscriber in subs {
        let Ok(Ok(res)) = subscriber.await else {
            continue;
        };
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
    start: Arc<Barrier>,
    wait: time::Duration,
) -> Result<(usize, usize), mqtt::Error> {
    let cli_opts = CreateOptionsBuilder::new()
        .server_uri(broker)
        .max_buffered_messages(MAX_BUFFERED)
        .finalize();
    let client = mqtt::AsyncClient::new(cli_opts)?;

    let conn_opts = ConnectOptionsBuilder::new().finalize(); // connection timeout maybe?
    client.connect(conn_opts).await?;

    // create a new task to collect result of the publication
    let (tx, result) = spawn_result_collector();

    // wait every other publisher/subscriber to be setted up
    start.wait().await;

    let mut interval = time::interval(interval);
    // interval.set_missed_tick_behavior(time::MissedTickBehavior::Skip);

    for id in start_id..start_id + count {
        interval.tick().await;

        let payload = Payload {
            id,
            creation: SystemTime::now(),
        };

        let payload = bincode::serialize(&payload).expect("serialization should behave nicely");
        let message = Message::new(TOPIC, payload, QOS);

        let delivery = client.try_publish(message)?;

        if let Err(e) = tx.send(delivery) {
            eprintln!("error delivering: {}", e);
        }
    }
    println!("done publishing");

    // sleep(wait).await;
    println!("->disconnecting with timeout now: {:?}", SystemTime::now());
    client.disconnect_after(wait).await?;
    println!("->disconected: {:?}", SystemTime::now());

    drop(tx);
    Ok(result.await.unwrap())
}

fn spawn_result_collector() -> (
    mpsc::UnboundedSender<DeliveryToken>,
    JoinHandle<(usize, usize)>,
) {
    let (tx, rx) = mpsc::unbounded_channel();
    let handle = tokio::spawn(publisher_collect_result(rx));
    (tx, handle)
}

async fn publisher_collect_result(mut channel: mpsc::UnboundedReceiver<DeliveryToken>,
) -> (usize, usize) {
    let mut succ = 0;
    let mut fail = 0;

    while let Some(delivery) = channel.recv().await {
        match delivery.await {
            Ok(()) => succ += 1,
            Err(mqtt::Error::Paho(-11)) => {
                fail += 1;
                eprintln!("Publication incomplete, there was inflight messages when the client connection was closed (problably). Try increasing the waiting time.");
            }
            Err(mqtt::Error::PahoDescr(-11, e)) => {
                fail += 1;
                eprintln!("{}, there was inflight messages when the client connection was closed (problably). Try increasing the waiting time.", e);
            }
            Err(e) => {
                fail += 1;
                eprintln!("Error sending publication: {e:?}");
            }
        }
    }

    (succ, fail)
}

async fn subscriber_task(
    broker: String,
    expecting: usize,
    receiving: CancellationToken,
    start_barrier: Arc<Barrier>,
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

    // wait in the barrier, this is for publihsers to know we finished setting up and are able to
    // receive publications
    start_barrier.wait().await;

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
        pub clients: usize,
        pub rate: u64,    // messages per second
        pub count: usize, // total of messages to publish
    }

    #[derive(Deserialize)]
    pub struct SubBatch {
        pub broker: String,
        pub clients: usize,
    }

    pub fn load(file: &str) -> Result<Settings, ConfigError> {
        let conf = Config::builder()
            .add_source(File::new(file, FileFormat::Toml))
            .build()?;
        conf.try_deserialize()
    }
}
