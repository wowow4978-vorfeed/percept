use std::path::PathBuf;
use std::sync::Arc;

use anyhow::{Context, Result};
use clap::{Parser, Subcommand};
use percept_store::{sweep, ColdStore, VectorIndex};

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
    /// Retention administration.
    #[command(subcommand)]
    Retention(RetentionCommand),
    /// BLE administration. v1 ships only the `pair` helper — actual scan
    /// and GATT adapters land in a slice-6 follow-up.
    #[command(subcommand)]
    Ble(BleCommand),
    /// Print build version.
    Version,
}

#[derive(Subcommand, Debug)]
pub enum BleCommand {
    /// Thin wrapper over `bluetoothctl pair <mac>`. DESIGN §12.5:
    /// pairing is OS-managed, percept stores nothing.
    Pair {
        /// MAC address of the device to pair with, e.g. AA:BB:CC:DD:EE:FF.
        mac: String,
    },
}

#[derive(Subcommand, Debug)]
pub enum RetentionCommand {
    /// Report what the next sweep pass would drop, without modifying
    /// anything. Honours the [[retention]] policies in the loaded config.
    DryRun {
        #[arg(long, default_value = DEFAULT_CONFIG_PATH)]
        config: PathBuf,
    },
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
        Command::Retention(RetentionCommand::DryRun { config: path }) => retention_dry_run(&path),
        Command::Ble(BleCommand::Pair { mac }) => ble_pair(&mac),
    }
}

fn ble_pair(mac: &str) -> Result<()> {
    if !looks_like_mac(mac) {
        anyhow::bail!(
            "{:?} does not look like a MAC address (expected AA:BB:CC:DD:EE:FF)",
            mac
        );
    }
    let status = std::process::Command::new("bluetoothctl")
        .arg("pair")
        .arg(mac)
        .status()
        .with_context(|| {
            "running bluetoothctl — install BlueZ (`apt install bluez` on Debian) or run on a host with it available"
        })?;
    if !status.success() {
        anyhow::bail!("bluetoothctl pair exited with {status}");
    }
    Ok(())
}

fn looks_like_mac(s: &str) -> bool {
    let bytes: Vec<&str> = s.split(':').collect();
    bytes.len() == 6
        && bytes
            .iter()
            .all(|b| b.len() == 2 && b.chars().all(|c| c.is_ascii_hexdigit()))
}

fn retention_dry_run(path: &std::path::Path) -> Result<()> {
    let cfg = config::load(path)?;
    let data_dir = cfg
        .server
        .as_ref()
        .map(|s| s.data_dir.clone())
        .context("no [server].data_dir — nothing to sweep")?;

    let cold =
        Arc::new(ColdStore::open(std::path::Path::new(&data_dir)).context("opening cold store")?);
    let vector = match build_vector_index_handle(&cfg, &data_dir) {
        Ok(v) => v,
        Err(e) => {
            // Vector index is optional; warn and continue with cold only.
            eprintln!("note: vector index not available: {e}");
            None
        }
    };
    let policies = config::build_retention_policies(&cfg)?;
    let now_ms = percept_core::now_ms_utc();
    let report =
        sweep(&cold, vector.as_deref(), &policies, now_ms, true).context("retention dry-run")?;
    let pretty = serde_json::to_string_pretty(&report).unwrap_or_default();
    println!("{pretty}");
    Ok(())
}

/// Open the vector index using the same model id the running embedder
/// would use. Returns `None` (with the error logged) when the index
/// can't be loaded — e.g. wrong-model mismatch from a prior session.
fn build_vector_index_handle(
    cfg: &config::Config,
    data_dir: &str,
) -> Result<Option<Arc<VectorIndex>>> {
    use percept_store::HashEmbedder;
    let embed_default = cfg
        .storage
        .as_ref()
        .and_then(|s| s.embed_default)
        .unwrap_or(false);
    let any_enabled = embed_default
        || cfg.kinds.iter().any(|k| k.embed == Some(true))
        || cfg.sources.iter().any(|s| s.embed == Some(true));
    if !any_enabled {
        return Ok(None);
    }
    // Match `server::current_embedder`. When the slice-4 follow-up
    // wires the real embedder this becomes a shared helper.
    let embedder = HashEmbedder::new(64);
    let idx = VectorIndex::open(
        std::path::Path::new(data_dir),
        percept_store::Embedder::model_id(&embedder),
        percept_store::Embedder::dim(&embedder),
    )?;
    Ok(Some(Arc::new(idx)))
}

#[cfg(test)]
mod mac_tests {
    use super::looks_like_mac;

    #[test]
    fn accepts_canonical() {
        assert!(looks_like_mac("AA:BB:CC:DD:EE:FF"));
        assert!(looks_like_mac("aa:bb:cc:dd:ee:ff"));
    }

    #[test]
    fn rejects_garbage() {
        assert!(!looks_like_mac("hello"));
        assert!(!looks_like_mac("AA:BB:CC:DD:EE")); // 5 segments
        assert!(!looks_like_mac("AA-BB-CC-DD-EE-FF"));
        assert!(!looks_like_mac("ZZ:BB:CC:DD:EE:FF"));
    }
}
