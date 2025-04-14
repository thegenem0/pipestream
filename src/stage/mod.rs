use std::time::Duration;

use crate::common::{IOParam, LibResult};
use std::fmt::Debug;

mod err;
mod inner;
mod stage;

pub use err::StageError;
pub use inner::StageImpl;
pub use stage::Stage;

/// Internal trait used to handle dynamic stage processing
pub(crate) trait DynStage<I, O>: Send + Sync + Debug
where
    I: IOParam,
    O: IOParam,
{
    fn process(&self, input: I) -> LibResult<O>;

    fn process_batch(&self, batch: Vec<(usize, I)>) -> Vec<(usize, LibResult<O>)>;

    // fn get_stage_info(&self) -> Vec<PipelineStageInfo>;
}

#[derive(Debug)]
pub struct StageInfo {
    pub active_jobs: usize,
    pub queue_capacity: usize,
    pub worker_count: usize,
    pub input_pool_available: usize,
    pub output_pool_available: usize,
}

#[derive(Debug, Clone)]
pub struct StageConfig {
    /// Number of worker threads
    pub workers: usize,
    /// Maximum batch size for parallel processing
    pub batch_size: usize,
    /// Object pool size for input buffers
    pub input_pool_size: usize,
    /// Object pool size for output buffers
    pub output_pool_size: usize,
    /// Maximum queue size for pending tasks
    pub max_queue_size: usize,
    /// Backpressure threshold (% of queue capacity)
    pub backpressure_threshold: f64,
    /// Processing timeout
    pub timeout: Option<Duration>,
}

impl Default for StageConfig {
    fn default() -> Self {
        Self {
            workers: num_cpus::get(),
            batch_size: 100,
            input_pool_size: 1000,
            output_pool_size: 1000,
            max_queue_size: 10000,
            backpressure_threshold: 0.8,
            timeout: None,
        }
    }
}
