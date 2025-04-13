use std::time::Duration;

use base::PipelineComponent;
use circuitable::{CircuitBreakerConfig, CircuitableComponent};
use retriable::{RetryPolicy, RetryableComponent};
use timeoutable::TimeoutableComponent;

use crate::common::{BoxedError, IOParam};

pub mod base;
pub mod circuitable;
pub mod retriable;
pub mod timeoutable;

pub fn with_retry<I, O, C>(component: C, policy: RetryPolicy) -> RetryableComponent<I, O, C>
where
    I: IOParam + Clone,
    O: IOParam,
    C: PipelineComponent<I, O>,
{
    RetryableComponent::new(component, policy)
}

pub fn with_circuit_breaker<I, O, C>(
    component: C,
    config: CircuitBreakerConfig,
) -> CircuitableComponent<I, O, C>
where
    I: IOParam + Clone,
    O: IOParam,
    C: PipelineComponent<I, O>,
{
    CircuitableComponent::new(component, config)
}

pub fn with_timeout<I, O, C>(component: C, timeout: Duration) -> TimeoutableComponent<I, O, C>
where
    I: IOParam + Clone,
    O: IOParam,
    C: PipelineComponent<I, O> + 'static,
{
    TimeoutableComponent::new(component, timeout)
}

pub fn run_parallel<I, O1, O2, O>(
    input: I,
    component1: impl PipelineComponent<I, O1> + 'static,
    component2: impl PipelineComponent<I, O2> + 'static,
    combiner: impl Fn(O1, O2) -> Result<O, BoxedError>,
) -> Result<O, BoxedError>
where
    I: IOParam + Clone,
    O1: IOParam,
    O2: IOParam,
    O: IOParam,
{
    use std::thread;

    let input1 = input.clone();
    let input2 = input;

    let (sender1, receiver1) = std::sync::mpsc::channel();
    let (sender2, receiver2) = std::sync::mpsc::channel();

    let handle1 = thread::spawn(move || {
        let result = component1.process(input1);
        let _ = sender1.send(result);
    });

    let handle2 = thread::spawn(move || {
        let result = component2.process(input2);
        let _ = sender2.send(result);
    });

    handle1.join().map_err(|_| "Thread 1 panicked")?;
    handle2.join().map_err(|_| "Thread 2 panicked")?;

    let result1 = receiver1
        .recv()
        .map_err(|_| "Failed to receive result from thread 1")?;
    let result2 = receiver2
        .recv()
        .map_err(|_| "Failed to receive result from thread 2")?;

    let output1 = result1?;
    let output2 = result2?;

    combiner(output1, output2)
}
