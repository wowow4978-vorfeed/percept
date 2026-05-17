use std::path::PathBuf;

use anyhow::Result;
use clap::{Parser, Subcommand};

use crate::{config, server};

const DEFAULT_CONFIG_PATH: &str = "/etc/percept/percept.toml";

#[derive(Parser, Debug)]
#[command(name = "percept", version)]
pub struct Cli {
    #[command(subcommand)]
    pub command: Command,
}

#[derive(Subcommand, Debug)]
pub enum Command {
    /// Run the ingest server.
    Serve {
        #[arg(long, default_value = DEFAULT_CONFIG_PATH)]
        config: PathBuf,
    },
    /// Load and validate the configuration without starting the server.
    ConfigCheck {
        #[arg(long, default_value = DEFAULT_CONFIG_PATH)]
        config: PathBuf,
    },
    /// Print build version.
    Version,
}

pub fn dispatch(cli: Cli) -> Result<()> {
    match cli.command {
        Command::Version => {
            println!("percept {}", env!("CARGO_PKG_VERSION"));
            Ok(())
        }
        Command::ConfigCheck { config: path } => match config::load(&path) {
            Ok(_) => {
                println!("ok");
                Ok(())
            }
            Err(e) => {
                eprintln!("config error: {e}");
                std::process::exit(2);
            }
        },
        Command::Serve { config: path } => {
            let cfg = config::load(&path)?;
            let rt = tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .build()?;
            rt.block_on(server::run(cfg))
        }
    }
}
