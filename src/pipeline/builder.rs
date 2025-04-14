use std::sync::Arc;

use crate::{
    common::IOParam,
    stage::{DynStage, Stage, StageImpl},
};

use super::{ChainedProcessor, Pipeline};

pub trait StageCompatible<I, O> {}

/// Implement compatibility for stages with matching input/output types
impl<I, M, O, C1, C2> StageCompatible<I, O> for (Stage<I, M, C1>, Stage<M, O, C2>)
where
    I: IOParam,
    M: IOParam,
    O: IOParam,
    C1: StageImpl<I, M> + Send + Sync + 'static,
    C2: StageImpl<M, O> + Send + Sync + 'static,
{
}

/// Builder for creating pipelines with type safety between stages
pub struct PipelineBuilder<I, O>
where
    I: IOParam,
    O: IOParam,
{
    processor: Option<Box<dyn DynStage<I, O>>>,
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
    pub fn start_with<C>(stage: Stage<I, O, C>) -> Self
    where
        C: StageImpl<I, O> + Send + Sync + 'static,
    {
        Self {
            processor: Some(Box::new(stage)),
            _phantom: std::marker::PhantomData,
        }
    }

    /// Add the next stage to the pipeline, creating a new builder with updated output type
    pub fn then<NewO, C>(self, next_stage: Stage<O, NewO, C>) -> PipelineBuilder<I, NewO>
    where
        NewO: IOParam,
        C: StageImpl<O, NewO> + Send + Sync + 'static,
        O: Clone,
        (
            Stage<I, O, Box<dyn StageImpl<I, O> + Send + Sync>>,
            Stage<O, NewO, C>,
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
    pub fn build(self) -> Arc<Pipeline<I, O>> {
        Arc::new(Pipeline {
            processor: self.processor,
            _phantom: std::marker::PhantomData,
        })
    }

    pub fn build_blocking(self) -> Pipeline<I, O> {
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
