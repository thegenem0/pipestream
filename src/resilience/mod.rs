use std::time::Duration;

use circuitable::{CircuitBreakerConfig, CircuitableComponent};
use retriable::{RetryPolicy, RetryableComponent};
use timeoutable::TimeoutableComponent;

use crate::{common::IOParam, component::PipelineComponent};

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
