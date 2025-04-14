use std::sync::Arc;

use crate::{common::IOParam, pipeline::Pipeline};

use super::stream::PipelineStream;

pub struct StreamingPipelineBuilder<I, O>
where
    I: IOParam,
    O: IOParam,
{
    pipeline: Arc<Pipeline<I, O>>,
}

impl<I, O> StreamingPipelineBuilder<I, O>
where
    I: IOParam,
    O: IOParam,
{
    pub fn new(pipeline: Pipeline<I, O>) -> Self {
        Self {
            pipeline: Arc::new(pipeline),
        }
    }

    /// Create a stream processor for this pipeline
    pub fn create_processor<T>(&self) -> PipelineStream<I, O, T>
    where
        T: Send + Sync + Clone + 'static,
    {
        PipelineStream::new(Arc::clone(&self.pipeline))
    }
}

pub trait PipelineStreamExt<I, O>
where
    I: IOParam,
    O: IOParam,
{
    fn streaming(self) -> StreamingPipelineBuilder<I, O>;
}

impl<I, O> PipelineStreamExt<I, O> for Pipeline<I, O>
where
    I: IOParam,
    O: IOParam,
{
    fn streaming(self) -> StreamingPipelineBuilder<I, O> {
        StreamingPipelineBuilder::new(self)
    }
}

pub trait PipelineSource<I, T>
where
    I: IOParam,
    T: Send + Sync + Clone + 'static,
{
    fn into_iter(self) -> Box<dyn Iterator<Item = (T, I)> + Send>;
}
