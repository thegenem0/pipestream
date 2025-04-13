use thiserror::Error;

#[allow(dead_code)]
pub mod common;

#[allow(dead_code)]
pub mod component;

#[allow(dead_code)]
pub mod pipeline;

#[allow(dead_code)]
pub mod pool;

#[allow(dead_code)]
pub mod stage;

#[allow(dead_code)]
pub mod stream;

#[allow(dead_code)]
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
