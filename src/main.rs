use chrono::{DateTime, Utc, SecondsFormat};
use mqtt::{ConnectOptionsBuilder, CreateOptionsBuilder, DeliveryToken, Message};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::{env, process, vec};
use tokio::sync::{mpsc, Barrier};
use tokio::task::JoinHandle;
use tokio::time::{self, sleep};
use tokio_util::sync::CancellationToken;

use paho_mqtt as mqtt;

static TOPIC: &str = "federated/benchmark";
static QOS: i32 = 0;
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

    // for notifying subscribers to stop waiting for publications
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
                time::Duration::from_nanos(1_000_000_000 / batch.rate),
                start_barrier.clone(),
                time::Duration::from_secs(config.wait),
            );
            pubs.push(tokio::spawn(task));
            id += batch.count;
        }
    }
    let total_messages = id;

    let mut subs = Vec::new();
    for batch in &config.subscribers {
        for i in 0..batch.clients {
            let task = subscriber_task(
                batch.broker.clone(),
                i,
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
                eprintln!("Published {} messages with success, {} failed", succ, fail);
            }
            Err(e) => {
                eprintln!("Error in publisher: {}!!!", e);
            }
            Ok(Err(e)) => {
                eprintln!("Error in publisher: {}!()!", e);
            }
        }
    }

    tokio::spawn(async move{
        sleep(time::Duration::from_secs(config.wait)).await;
        stop_receiving.cancel();
    });

    for subscriber in subs {
        let Ok(Ok(res)) = subscriber.await else {
            eprintln!("Error in subcriber!!!");
            continue;
        };

        let broker = res.0;
        let sub_id = res.1;
        let expecting = res.2;
        let unique = res.3;
        let dup = res.4;
        let timings = res.5;

        eprintln!("Subscriber {sub_id} on broker {broker} was expecting {expecting} messages, received {unique} unique messages, {dup} duplicates");
        for times in timings {
            let time = times.0.to_rfc3339_opts(SecondsFormat::Nanos, true);
            let latency = times.1.num_nanoseconds().unwrap(); // nanosecods can overflow
            println!("{broker}, {sub_id}, {time}, {latency}");
        }
    }
}

#[derive(Serialize, Deserialize)]
struct Payload {
    pub id: usize,
    pub creation: DateTime<Utc>,
}

async fn publisher_task(
    broker: String,
    start_id: usize,
    count: usize,
    interval: time::Duration,
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
    eprintln!("started={}", Utc::now().to_rfc3339_opts(SecondsFormat::Nanos, true));

    let mut interval = time::interval(interval);
    // interval.set_missed_tick_behavior(time::MissedTickBehavior::Skip);

    for id in start_id..start_id + count {
        interval.tick().await;

        let payload = Payload {
            id,
            creation: Utc::now(),
        };

        let payload = bincode::serialize(&payload).expect("serialization should behave nicely");
        let message = Message::new(TOPIC, payload, QOS);

        let delivery = client.try_publish(message)?;

        if let Err(e) = tx.send(delivery) {
            eprintln!("error delivering: {}", e);
        }
    }

    let wait = time::Duration::from_secs(wait.as_nanos() as u64); // temporary fix https://github.com/eclipse/paho.mqtt.rust/pull/202
    client.disconnect_after(wait).await?;

    drop(tx); // droping the sender part of the channel makes the collector know that it should
    // stop waiting for publications results
    
    let result = result.await.unwrap();
    eprintln!("done publishing={}", Utc::now().to_rfc3339_opts(SecondsFormat::Nanos, true));
    Ok(result)
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
    sub_id: usize,
    expecting: usize,
    receiving: CancellationToken,
    start_barrier: Arc<Barrier>,
) -> Result<(String, usize, usize, usize, usize, Vec<(DateTime<Utc>, chrono::Duration)>), mqtt::Error> {
    let mut latencies = Vec::with_capacity(expecting);
    let mut cache = vec![false; expecting];
    let mut dup = 0;
    let mut unique = 0;

    let cli_opts = CreateOptionsBuilder::new()
        .server_uri(&broker)
        .max_buffered_messages(0) // there is nothing to buffer, subscriber only receive
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

            _ = receiving.cancelled() => {
                eprintln!("Subscriber {sub_id} on broker {broker} stopped before receiving all pubs");
                break;
            },

            message = stream.recv() => {
                if let Some(message) = message.expect("client doesn't close the channel") {
                    let now = Utc::now();

                    let Payload { id, creation } = bincode::deserialize(message.payload())
                        .expect("receiving only messaged that our publishers produced");

                    if cache[id] {
                        dup += 1;
                    } else {
                        cache[id] = true;
                        unique += 1;
                    }

                    latencies.push((now, now - creation));

                    if unique == expecting { // received all messages
                        client.stop_stream();
                        break;
                    }
                } else {
                    panic!("subscriber lost connection");
                }
            },
        }
    }

    client.disconnect(None).await?;

    Ok((broker, sub_id, expecting, unique, dup, latencies))
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
