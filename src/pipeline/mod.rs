use std::fmt::Debug;

mod builder;
mod chain;
mod pipe;

pub use builder::PipelineBuilder;
pub use chain::ChainedProcessor;
pub use pipe::Pipeline;

#[derive(Debug, Clone)]
pub struct PipelineStageInfo {
    pub name: String,
    pub input_type: String,
    pub output_type: String,
    pub workers: usize,
    pub active_jobs: usize,
    pub queue_capacity: usize,
}
