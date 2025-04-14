use std::collections::HashMap;
use std::fmt::Debug;

use crate::{
    common::{IOParam, LibResult},
    component::PipelineComponent,
    error::LibError,
    stage::PipelineStage,
};

#[derive(Debug, Clone)]
pub struct PipelineStageInfo {
    pub name: String,
    pub input_type: String,
    pub output_type: String,
    pub workers: usize,
    pub active_jobs: usize,
    pub queue_capacity: usize,
}

/// Internal trait used to handle dynamic stage processing
trait StageProcessor<I, O>: Send + Sync + Debug
where
    I: IOParam,
    O: IOParam,
{
    fn process(&self, input: I) -> LibResult<O>;

    // Type-erased batch processing method with no generics
    fn process_batch(&self, batch: Vec<(usize, I)>) -> Vec<(usize, LibResult<O>)>;

    fn get_stage_info(&self) -> Vec<PipelineStageInfo>;
}

/// Pipeline that coordinates processing through stages
#[derive(Debug)]
pub struct Pipeline<I, O>
where
    I: IOParam,
    O: IOParam,
{
    processor: Option<Box<dyn StageProcessor<I, O>>>,
    _phantom: std::marker::PhantomData<(I, O)>,
}

#[derive(Debug)]
struct ChainedProcessor<I, M, O>
where
    I: IOParam,
    M: IOParam,
    O: IOParam,
{
    first: Box<dyn StageProcessor<I, M>>,
    second: Box<dyn StageProcessor<M, O>>,
}

impl<I, M, O> StageProcessor<I, O> for ChainedProcessor<I, M, O>
where
    I: IOParam,
    M: IOParam,
    O: IOParam,
{
    fn process(&self, input: I) -> LibResult<O> {
        let intermediate = self.first.process(input)?;
        self.second.process(intermediate)
    }

    fn process_batch(&self, batch: Vec<(usize, I)>) -> Vec<(usize, LibResult<O>)> {
        // Process through first stage
        let intermediate_results = self.first.process_batch(batch);

        // Create a new batch for the second stage with successful results
        let second_stage_inputs: Vec<_> = intermediate_results
            .iter()
            .filter_map(|(id, result)| result.as_ref().ok().map(|value| (*id, value.clone())))
            .collect();

        let second_stage_results = self.second.process_batch(second_stage_inputs);

        let mut result_map = HashMap::new();
        for (id, result) in second_stage_results {
            result_map.insert(id, result);
        }

        intermediate_results
            .into_iter()
            .map(|(id, result)| match result {
                Ok(_) => match result_map.remove(&id) {
                    Some(second_result) => (id, second_result),
                    None => (
                        id,
                        Err(LibError::component(
                            std::any::type_name::<I>(),
                            format!("Missing result for ID {}", id),
                        )),
                    ),
                },
                Err(e) => (id, Err(e)),
            })
            .collect()
    }

    fn get_stage_info(&self) -> Vec<PipelineStageInfo> {
        let mut info = self.first.get_stage_info();
        info.extend(self.second.get_stage_info());
        info
    }
}

impl<I, O, C> StageProcessor<I, O> for PipelineStage<I, O, C>
where
    I: IOParam,
    O: IOParam,
    C: PipelineComponent<I, O> + Send + Sync + 'static,
{
    fn process(&self, input: I) -> LibResult<O> {
        self.process(input)
    }

    fn process_batch(&self, batch: Vec<(usize, I)>) -> Vec<(usize, LibResult<O>)> {
        self.process_batch(batch)
    }

    fn get_stage_info(&self) -> Vec<PipelineStageInfo> {
        let stage_info = self.info();

        let out = PipelineStageInfo {
            name: "Pipeline".to_string(),
            input_type: std::any::type_name::<I>().to_string(),
            output_type: std::any::type_name::<O>().to_string(),
            workers: stage_info.worker_count,
            active_jobs: stage_info.active_jobs,
            queue_capacity: stage_info.queue_capacity,
        };

        vec![out]
    }
}

impl<I, O> Pipeline<I, O>
where
    I: IOParam,
    O: IOParam,
{
    pub fn new() -> Self {
        Self {
            processor: None,
            _phantom: std::marker::PhantomData,
        }
    }

    /// Process a single input through the pipeline
    pub fn process(&self, input: I) -> LibResult<O> {
        match &self.processor {
            Some(processor) => processor.process(input),
            None => Err(LibError::config("Pipeline has no stages")),
        }
    }

    /// Process a batch of inputs through the pipeline with generic ID type
    pub fn process_batch<T>(&self, inputs: Vec<(T, I)>) -> Vec<(T, LibResult<O>)>
    where
        T: Clone + Send + Sync + 'static,
    {
        match &self.processor {
            Some(processor) => {
                // Create a mapping from user's ID type to usize
                let mut id_mapping: HashMap<usize, T> = HashMap::new();

                // Map inputs to indexed batch
                let indexed_batch: Vec<(usize, I)> = inputs
                    .into_iter()
                    .enumerate()
                    .map(|(idx, (id, input))| {
                        id_mapping.insert(idx, id);
                        (idx, input)
                    })
                    .collect();

                // Process the batch
                let results = processor.process_batch(indexed_batch);

                // Map back to original ID type
                results
                    .into_iter()
                    .map(|(idx, result)| {
                        let original_id = id_mapping.remove(&idx).expect("Missing ID mapping");
                        (original_id, result)
                    })
                    .collect()
            }
            None => inputs
                .into_iter()
                .map(|(id, _)| (id, Err(LibError::config("Pipeline has no stages"))))
                .collect(),
        }
    }

    /// Get detailed information about all stages in the pipeline
    pub fn info(&self) -> Vec<PipelineStageInfo> {
        match &self.processor {
            Some(processor) => processor.get_stage_info(),
            None => Vec::new(),
        }
    }

    /// Get a summary of the pipeline
    pub fn summary(&self) -> String {
        let info = self.info();
        if info.is_empty() {
            return "Empty pipeline (no stages)".to_string();
        }

        let mut summary = format!("Pipeline with {} stages:\n\n", info.len());

        for (idx, stage) in info.iter().enumerate() {
            summary.push_str(&format!(
                "Stage {}: {} ( {} -> {} )\n -- Workers: {}, Active Jobs: {}, Queue Capacity: {}\n\n",
                idx + 1,
                stage.name,
                stage.input_type,
                stage.output_type,
                stage.workers,
                stage.active_jobs,
                stage.queue_capacity
            ));
        }

        summary
    }
}

impl<I, O> Default for Pipeline<I, O>
where
    I: IOParam,
    O: IOParam,
{
    fn default() -> Self {
        Self::new()
    }
}

pub trait StageCompatible<I, O> {}

/// Implement compatibility for stages with matching input/output types
impl<I, M, O, C1, C2> StageCompatible<I, O> for (PipelineStage<I, M, C1>, PipelineStage<M, O, C2>)
where
    I: IOParam,
    M: IOParam,
    O: IOParam,
    C1: PipelineComponent<I, M> + Send + Sync + 'static,
    C2: PipelineComponent<M, O> + Send + Sync + 'static,
{
}

/// Builder for creating pipelines with type safety between stages
pub struct PipelineBuilder<I, O>
where
    I: IOParam,
    O: IOParam,
{
    processor: Option<Box<dyn StageProcessor<I, O>>>,
    _phantom: std::marker::PhantomData<(I, O)>,
}

impl<I, O> PipelineBuilder<I, O>
where
    I: IOParam,
    O: IOParam,
{
    pub fn new() -> Self {
        Self {
            processor: None,
            _phantom: std::marker::PhantomData,
        }
    }

    /// Start the pipeline with an initial stage
    pub fn start_with<C>(stage: PipelineStage<I, O, C>) -> Self
    where
        C: PipelineComponent<I, O> + Send + Sync + 'static,
    {
        Self {
            processor: Some(Box::new(stage)),
            _phantom: std::marker::PhantomData,
        }
    }

    /// Add the next stage to the pipeline, creating a new builder with updated output type
    pub fn then<NewO, C>(self, next_stage: PipelineStage<O, NewO, C>) -> PipelineBuilder<I, NewO>
    where
        NewO: IOParam,
        C: PipelineComponent<O, NewO> + Send + Sync + 'static,
        O: Clone,
        (
            PipelineStage<I, O, Box<dyn PipelineComponent<I, O> + Send + Sync>>,
            PipelineStage<O, NewO, C>,
        ): StageCompatible<I, NewO>,
    {
        if let Some(current) = self.processor {
            let chained = ChainedProcessor {
                first: current,
                second: Box::new(next_stage),
            };

            PipelineBuilder {
                processor: Some(Box::new(chained)),
                _phantom: std::marker::PhantomData,
            }
        } else {
            panic!("Cannot chain with an empty pipeline");
        }
    }

    /// Build the final pipeline
    pub fn build(self) -> Pipeline<I, O> {
        Pipeline {
            processor: self.processor,
            _phantom: std::marker::PhantomData,
        }
    }
}

impl<I, O> Default for PipelineBuilder<I, O>
where
    I: IOParam,
    O: IOParam,
{
    fn default() -> Self {
        Self::new()
    }
}
