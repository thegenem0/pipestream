use std::{sync::Arc, time::Duration};

use crate::common::{IOParam, LibResult};

use crate::error::LibError;

use super::PipelineComponent;

#[derive(Debug, thiserror::Error)]
#[error("Operation timed out after {:?}", timeout)]
pub struct TimeoutError {
    timeout: Duration,
}

#[derive(Debug)]
pub struct TimeoutableComponent<I, O, C>
where
    I: IOParam + Clone,
    O: IOParam,
    C: PipelineComponent<I, O>,
{
    inner: Arc<C>,
    timeout: Duration,
    _phantom: std::marker::PhantomData<(I, O)>,
}

impl<I, O, C> TimeoutableComponent<I, O, C>
where
    I: IOParam + Clone,
    O: IOParam,
    C: PipelineComponent<I, O> + 'static,
{
    pub fn new(component: C, timeout: Duration) -> Self {
        Self {
            inner: Arc::new(component),
            timeout,
            _phantom: std::marker::PhantomData,
        }
    }
}

impl<I, O, C> PipelineComponent<I, O> for TimeoutableComponent<I, O, C>
where
    I: IOParam + Clone,
    O: IOParam,
    C: PipelineComponent<I, O> + 'static,
{
    fn process(&self, input: I) -> LibResult<O> {
        let component = Arc::clone(&self.inner);
        let input_clone = input.clone();
        let timeout = self.timeout;

        let operation = std::thread::spawn(move || component.process(input_clone));

        match operation.join() {
            Ok(result) => result,
            Err(_) => Err(LibError::Timeout(timeout)),
        }
    }

    fn name(&self) -> String {
        format!("TimeoutableComponent({})", self.inner.name())
    }
}
