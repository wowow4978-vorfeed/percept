use thiserror::Error;

#[derive(Debug, Error)]
pub enum Error {
    #[error("invalid kind reference: {0}")]
    InvalidKindRef(String),
}
