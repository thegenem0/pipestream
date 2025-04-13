use std::error::Error;
use std::fmt::Debug;
use std::sync::Arc;
use std::time::Duration;

use crate::common::{BoxedError, IOParam};
use crate::component::base::PipelineComponent;
use crate::pool::{ObjectPool, PooledObject};
use crate::worker::WorkerPool;

#[derive(Debug, thiserror::Error)]
pub enum PipelineError {
    #[error("Pipeline component '{0}' failed: {1}")]
    ComponentError(String, Box<dyn Error + Send + Sync>),
    #[error("Pipeline error: {0}")]
    Other(String),
    #[error("Task cancelled")]
    Cancelled,
    #[error("Worker thread panicked")]
    WorkerPanic,
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

#[derive(Debug)]
pub struct PipelineStage<I, O, C>
where
    I: IOParam + Clone,
    O: IOParam,
    C: PipelineComponent<I, O> + Send + Sync + 'static,
{
    component: Arc<C>,
    worker_pool: WorkerPool<I, O>,
    input_pool: Option<Arc<ObjectPool<I>>>,
    output_pool: Option<Arc<ObjectPool<O>>>,
    config: StageConfig,
}

impl<I, O, C> PipelineStage<I, O, C>
where
    I: IOParam + Clone,
    O: IOParam,
    C: PipelineComponent<I, O> + Send + Sync + 'static,
{
    pub fn new(component: C, config: StageConfig) -> Self {
        Self {
            component: Arc::new(component),
            worker_pool: WorkerPool::new(config.workers),
            input_pool: None,
            output_pool: None,
            config,
        }
    }

    pub fn with_input_pool<F>(mut self, create_fn: F) -> Self
    where
        F: Fn() -> I + Send + Sync + 'static,
    {
        let init_capacity = self.config.input_pool_size / 4;
        let max_size = self.config.input_pool_size;

        self.input_pool = Some(Arc::new(ObjectPool::new(
            init_capacity,
            max_size,
            create_fn,
        )));

        self
    }

    pub fn with_output_pool<F>(mut self, create_fn: F) -> Self
    where
        F: Fn() -> O + Send + Sync + 'static,
    {
        let init_capacity = self.config.output_pool_size / 4;
        let max_size = self.config.output_pool_size;

        self.output_pool = Some(Arc::new(ObjectPool::new(
            init_capacity,
            max_size,
            create_fn,
        )));

        self
    }

    pub fn process(&self, input: I) -> Result<O, BoxedError> {
        let component = Arc::clone(&self.component);
        component.process(input)
    }

    /// Process a batch of inputs in parallel
    pub fn process_batch<T>(&self, inputs: Vec<(T, I)>) -> Vec<(T, Result<O, BoxedError>)>
    where
        T: Clone + Send + Sync + 'static,
    {
        let active_jobs = self.worker_pool.active_jobs();
        let capacity = self.worker_pool.queue_capacity();

        if (active_jobs as f64 / capacity as f64) > self.config.backpressure_threshold {
            return inputs
                .into_iter()
                .map(|(id, input)| {
                    let result = self.process(input);
                    (id, result)
                })
                .collect();
        }

        let results = self.worker_pool.process_batch(
            Arc::clone(&self.component),
            inputs
                .iter()
                .enumerate()
                .map(|(idx, (_, input))| (idx, input.clone()))
                .collect(),
        );

        results
            .into_iter()
            .map(|(idx, result)| (inputs[idx].0.clone(), result))
            .collect()
    }

    /* pub fn process_stream<T>(
        &self,
        inputs: impl Iterator<Item = (T, I)>,
    ) -> impl Iterator<Item = (T, Result<O, BoxedError>)>
    where
        T: Clone + Send + Sync + 'static,
    {
        let mut buffer = Vec::with_capacity(self.config.batch_size);
        let mut results = Vec::new();
        let mut exhausted = false;

        std::iter::from_fn(move || {
            if !results.is_empty() {
                return results.pop();
            }

            if exhausted && buffer.is_empty() {
                return None;
            }

            while buffer.len() < self.config.batch_size && !exhausted {
                match &inputs.next() {
                    Some(item) => buffer.push(item),
                    None => {
                        exhausted = true;
                        break;
                    }
                }
            }

            if buffer.is_empty() {
                return None;
            }

            let batch: Vec<_> = buffer.drain(..).collect();
            results = self.process_batch(batch);

            results.pop()
        })
    } */

    /// Get an object from the input pool if enabled
    pub fn get_input(&self) -> Option<PooledObject<I>> {
        self.input_pool.as_ref().map(|pool| pool.get())
    }

    /// Get an object from the output pool if enabled
    pub fn get_output(&self) -> Option<PooledObject<O>> {
        self.output_pool.as_ref().map(|pool| pool.get())
    }

    /// Get the worker pool info
    pub fn info(&self) -> StageInfo {
        StageInfo {
            active_jobs: self.worker_pool.active_jobs(),
            queue_capacity: self.worker_pool.queue_capacity(),
            worker_count: self.config.workers,
            input_pool_available: self.input_pool.as_ref().map_or(0, |p| p.available()),
            output_pool_available: self.output_pool.as_ref().map_or(0, |p| p.available()),
        }
    }
}
