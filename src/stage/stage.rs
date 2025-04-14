use std::sync::Arc;

use crate::{
    common::{IOParam, LibResult},
    concurrency::{ObjectPool, PooledObject, WorkerPool},
};

use super::{DynStage, StageConfig, StageImpl, StageInfo};

#[derive(Debug)]
pub struct Stage<I, O, C>
where
    I: IOParam,
    O: IOParam,
    C: StageImpl<I, O> + Send + Sync + 'static,
{
    component: Arc<C>,
    worker_pool: WorkerPool<I, O>,
    input_pool: Option<Arc<ObjectPool<I>>>,
    output_pool: Option<Arc<ObjectPool<O>>>,
    config: StageConfig,
}

impl<I, O, C> Stage<I, O, C>
where
    I: IOParam,
    O: IOParam,
    C: StageImpl<I, O> + Send + Sync + 'static,
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

    pub fn process(&self, input: I) -> LibResult<O> {
        let component = Arc::clone(&self.component);
        component.process(input)
    }

    /// Process a batch of inputs in parallel
    pub fn process_batch<T>(&self, inputs: Vec<(T, I)>) -> Vec<(T, LibResult<O>)>
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

impl<I, O, C> DynStage<I, O> for Stage<I, O, C>
where
    I: IOParam,
    O: IOParam,
    C: StageImpl<I, O> + Send + Sync + 'static,
{
    fn process(&self, input: I) -> LibResult<O> {
        self.process(input)
    }

    fn process_batch(&self, batch: Vec<(usize, I)>) -> Vec<(usize, LibResult<O>)> {
        self.process_batch(batch)
    }

    // fn get_stage_info(&self) -> Vec<PipelineStageInfo> {
    //     let stage_info = self.info();
    //
    //     let out = PipelineStageInfo {
    //         name: "Pipeline".to_string(),
    //         input_type: std::any::type_name::<I>().to_string(),
    //         output_type: std::any::type_name::<O>().to_string(),
    //         workers: stage_info.worker_count,
    //         active_jobs: stage_info.active_jobs,
    //         queue_capacity: stage_info.queue_capacity,
    //     };
    //
    //     vec![out]
    // }
}
