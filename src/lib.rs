use thiserror::Error;

pub mod common;
pub mod component;
pub mod pipeline;
pub mod pool;
pub mod stage;
pub mod stream;
pub mod worker;

#[allow(dead_code)]
#[derive(Error, Debug)]
pub enum LibError {
    #[error("IO error: {0}")]
    IO(String),

    #[error("Invalid input for processor: {0}")]
    InvalidInput(String),

    #[error("JSON error: {0}")]
    Json(String),

    #[error("Unknown error: {0}")]
    Unknown(String),
}
