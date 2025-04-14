use std::{thread, time::Duration};

use crate::{
    common::{IOParam, LibResult},
    error::LibError,
};

use super::StageImpl;

#[derive(Debug, Clone)]
pub struct RetryPolicy {
    max_attempts: usize,
    backoff_base: Duration,
    max_backoff: Duration,
    jitter_factor: f64,
}

impl Default for RetryPolicy {
    fn default() -> Self {
        Self {
            max_attempts: 3,
            backoff_base: Duration::from_millis(100),
            max_backoff: Duration::from_secs(10),
            jitter_factor: 0.1,
        }
    }
}

impl RetryPolicy {
    pub fn new(max_attempts: usize) -> Self {
        Self {
            max_attempts,
            ..Default::default()
        }
    }

    pub fn with_backoff_base(mut self, base: Duration, max: Duration) -> Self {
        self.backoff_base = base;
        self.max_backoff = max;
        self
    }

    pub fn with_jitter(mut self, factor: f64) -> Self {
        assert!(
            (0.0..=1.0).contains(&factor),
            "Jitter factor must be between 0.0 and 1.0"
        );
        self.jitter_factor = factor;
        self
    }

    pub fn should_retry(&self, attempt: usize) -> bool {
        attempt < self.max_attempts
    }

    pub fn delay_for_attempt(&self, attempt: usize) -> Duration {
        if attempt == 0 {
            return Duration::from_millis(0);
        }

        let exp_backoff = self.backoff_base.mul_f64(2.0_f64.powi(attempt as i32 - 1));
        let backoff = std::cmp::min(exp_backoff, self.max_backoff);

        if self.jitter_factor > 0.0 {
            let jitter_range = backoff.mul_f64(self.jitter_factor);
            let jitter = Duration::from_millis(
                (rand::random::<f64>() * jitter_range.as_millis() as f64) as u64,
            );
            backoff.saturating_add(jitter)
        } else {
            backoff
        }
    }
}

#[derive(Debug)]
pub struct RetryableComponent<I, O, C>
where
    I: IOParam + Clone,
    O: IOParam,
    C: StageImpl<I, O>,
{
    inner: C,
    policy: RetryPolicy,
    _phantom: std::marker::PhantomData<(I, O)>,
}

impl<I, O, C> RetryableComponent<I, O, C>
where
    I: IOParam + Clone,
    O: IOParam,
    C: StageImpl<I, O>,
{
    pub fn new(component: C, policy: RetryPolicy) -> Self {
        Self {
            inner: component,
            policy,
            _phantom: std::marker::PhantomData,
        }
    }
}

impl<I, O, C> StageImpl<I, O> for RetryableComponent<I, O, C>
where
    I: IOParam + Clone,
    O: IOParam,
    C: StageImpl<I, O>,
{
    fn process(&self, input: I) -> LibResult<O> {
        let mut attempt = 0;
        let mut last_error = None;

        while self.policy.should_retry(attempt) {
            if attempt > 0 {
                let delay = self.policy.delay_for_attempt(attempt);
                thread::sleep(delay);
            }

            match self.inner.process(input.clone()) {
                Ok(output) => return Ok(output),
                Err(err) => {
                    last_error = Some(err);
                    attempt += 1;
                }
            }
        }

        Err(LibError::component(
            std::any::type_name::<I>(),
            format!(
                "Failed to process input after {} attempts: {}",
                attempt,
                last_error.unwrap().to_string()
            ),
        ))
    }

    fn name(&self) -> String {
        format!("RetryableComponent<{}>", self.inner.name())
    }
}
