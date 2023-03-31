use std::time::SystemTime;
use std::time::Instant;
use std::{env, process, vec};
use std::sync::Arc;

use mqtt::{CreateOptionsBuilder, Message, ConnectOptions, ConnectOptionsBuilder};
use serde::{Serialize, Deserialize};
use tokio::sync::Barrier;
use tokio::sync::Notify;
use tokio::time::{sleep, Duration, self};

use paho_mqtt as mqtt;

static TOPIC: &str = "federated/benchmark";
static QOS: i32 = 2;

#[tokio::main]
async fn main() {
    let Some(config_file) = env::args().nth(1) else {
        eprintln!("No configuration file provided!");
        process::exit(1);
    };

    let config = config::load(&config_file).unwrap_or_else(|e|{
        eprintln!("Error in configuration file: {e}");
        process::exit(1);
    });

    let start_publishing = Arc::new(Notify::new());
    let end_publishing = Arc::new(Barrier::new());

    let stop_receiving = Arc::new(Notify::new());


    let mut id = 0;
    for batch in &config.publishers {
        for _ in 0..batch.clients {
            let pubi = Publisher::new(id, batch.count, start.clone());
            tokio::spawn(pubi.run());
            id += batch.count;
        }
    }
    let total_messages = id;

    for batch in &config.subscribers {
        for _ in 0..batch.clients {
            let a = time::Instant::new();
            let sub = Subscriber::new(total_messages);
            tokio::spawn(sub.run());
        }
    }

    // wait before start
    sleep(Duration::from_secs(config.wait)).await;

    // start publishers
    start.notify_waiters();

    // wait for publishers to end
    end_publishing.wait().await;
    
    // wait some time for subscribers to receive all the possivel publications
    sleep(Duration::from_secs(5)).await;

    // stop all publications


    // 
}

#[derive(Serialize, Deserialize)]
struct Payload {
    pub id: usize,
    pub creation: SystemTime,
}

struct Publisher {
    starting_id: u32,
    count: u32,
    start: Arc<Notify>,
}

impl Publisher {
    fn new(starting_id: u32, count: u32, start: Arc<Notify>) -> Self {
        Publisher { starting_id, count, start }
    }

    async fn run(self) {
        // setup 

        let opts = CreateOptionsBuilder::new().finalize();
        let client = mqtt::AsyncClient::new(opts).unwrap();

        self.start.notified().await;


        let mut interval = time::interval(Duration::from_millis(10));
        for id in self.starting_id..self.count {
            interval.tick().await;
            let pl = Payload { id , creation: SystemTime::now()};
            let pl = bincode::serialize(&pl).unwrap();
            let msg = Message::new(TOPIC, pl, QOS);
            client.publish(msg);
        }
    }
}

struct Subscriber {
    expecting: usize,
    received: Vec<bool>,
}

impl Subscriber {
    fn new(expecting: usize) -> Self {
        Self { expecting, received: vec![false; expecting] }
    }

    pub async fn run(self) {
        let mut latencies = Vec::with_capacity(self.expecting);
        let mut cache = vec![false; self.expecting];
        let mut dup = 0;

        let opts = CreateOptionsBuilder::new().finalize();
        let mut client = mqtt::AsyncClient::new(opts).unwrap();

        let stream = client.get_stream(None); // None = unbound channel

        let conn_opts = ConnectOptionsBuilder::new().finalize();
        client.connect(conn_opts).await.unwrap();
        client.subscribe(TOPIC, QOS).await.unwrap();

        loop {
            let message = stream.recv().await.expect("client should not close the channel");
            if let Some(message) = message {
                let pl: Payload = bincode::deserialize(message.payload()).expect("serialization and deserialization should occur without errors");

                if cache[pl.id] {
                    dup += 1;
                } else {
                    cache[pl.id] = true;
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
    }
}

mod config {
    use serde::Deserialize;
    use config::{Config, ConfigError, File, FileFormat};

    #[derive(Deserialize)]
    pub struct Settings {
        pub wait: u64,
        pub publishers: Vec<PubBatch>,
        pub subscribers: Vec<SubBatch>,
    }

    #[derive(Deserialize)]
    pub struct PubBatch {
        pub broker: String,
        pub clients: u32,
        pub interval: u32,
        pub count: u32,
    }

    #[derive(Deserialize)]
    pub struct SubBatch {
        pub broker: String,
        pub clients: u32,
    }

    pub fn load(file: &str) -> Result<Settings, ConfigError> {
        let conf = Config::builder().add_source(File::new(file, FileFormat::Toml)).build()?;
        conf.try_deserialize()
    }
}
