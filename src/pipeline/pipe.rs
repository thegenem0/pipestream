use std::collections::HashMap;

use crate::{
    common::{IOParam, LibResult},
    error::LibError,
    stage::DynStage,
};

/// Pipeline that coordinates processing through stages
#[derive(Debug)]
pub struct Pipeline<I, O>
where
    I: IOParam,
    O: IOParam,
{
    pub(crate) processor: Option<Box<dyn DynStage<I, O>>>,
    pub(crate) _phantom: std::marker::PhantomData<(I, O)>,
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

    // /// Get detailed information about all stages in the pipeline
    // pub fn info(&self) -> Vec<PipelineStageInfo> {
    //     match &self.processor {
    //         Some(processor) => processor.get_stage_info(),
    //         None => Vec::new(),
    //     }
    // }

    // /// Get a summary of the pipeline
    // pub fn summary(&self) -> String {
    //     let info = self.info();
    //     if info.is_empty() {
    //         return "Empty pipeline (no stages)".to_string();
    //     }
    //
    //     let mut summary = format!("Pipeline with {} stages:\n\n", info.len());
    //
    //     for (idx, stage) in info.iter().enumerate() {
    //         summary.push_str(&format!(
    //             "Stage {}: {} ( {} -> {} )\n -- Workers: {}, Active Jobs: {}, Queue Capacity: {}\n\n",
    //             idx + 1,
    //             stage.name,
    //             stage.input_type,
    //             stage.output_type,
    //             stage.workers,
    //             stage.active_jobs,
    //             stage.queue_capacity
    //         ));
    //     }
    //
    //     summary
    // }
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
