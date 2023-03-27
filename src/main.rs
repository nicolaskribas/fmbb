use std::{env, process};

fn main() {
    let Some(config_file) = env::args().nth(1) else {
        eprintln!("No configuration file provided!");
        process::exit(1);
    };

    let config = config::load(&config_file).unwrap_or_else(|e|{
        eprintln!("Error in configuration file: {e}");
        process::exit(1);
    });

    let mut id = 0;
    for batch in &config.publishers {
        for i in 0..batch.clients {
            Publisher::new(id, batch.count);
            id += batch.count;
        }
    }
    let total_count = id;

    for batch in &config.subscribers {
        for i in 0..batch.clients {
            Subscriber::new(total_count);
        }
    }
}

struct Publisher {
    starting_id: u32,
    count: u32,
}
impl Publisher {
    fn new(starting_id: u32, count: u32) -> Self {
        Publisher { starting_id, count }

    }
}

struct Subscriber {
    expecting: u32,
}

impl Subscriber {
    fn new(expecting: u32) -> Self {
        Self { expecting }
    }
}

mod config {
    use serde::Deserialize;
    use config::{Config, ConfigError, File, FileFormat};

    #[derive(Deserialize)]
    pub struct Settings {
        pub publishers: Vec<PubBatch>,
        pub subscribers: Vec<SubBatch>,
    }

    #[derive(Deserialize)]
    pub struct PubBatch {
        broker: String,
        pub clients: u32,
        interval: u32,
        pub count: u32,
    }

    #[derive(Deserialize)]
    pub struct SubBatch {
        broker: String,
        pub clients: u32,
    }

    pub fn load(file: &str) -> Result<Settings, ConfigError> {
        let conf = Config::builder().add_source(File::new(file, FileFormat::Toml)).build()?;
        conf.try_deserialize()
    }
}
