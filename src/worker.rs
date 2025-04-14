use crossbeam::{
    channel::{self, Receiver, Sender},
    queue::ArrayQueue,
};
use std::{
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    thread,
};

use crate::{
    common::{IOParam, LibResult},
    component::PipelineComponent,
};

type Job<T> = Box<dyn FnOnce() -> LibResult<(usize, T)> + Send + 'static>;

#[derive(Debug)]
struct Worker {
    id: usize,
    thread: Option<thread::JoinHandle<()>>,
}

impl Worker {
    fn new<I, O>(
        id: usize,
        job_rx: Arc<ArrayQueue<Job<O>>>,
        results_tx: Sender<LibResult<(usize, O)>>,
        running: Arc<AtomicBool>,
    ) -> Self
    where
        I: IOParam,
        O: IOParam,
    {
        let thread = thread::spawn(move || {
            let backoff = crossbeam::utils::Backoff::new();

            while running.load(Ordering::Relaxed) {
                match job_rx.pop() {
                    Some(job) => {
                        backoff.reset();

                        let result = job();
                        if results_tx.send(result).is_err() {
                            // Results channel is closed so worker should exit
                            break;
                        }
                    }
                    None => {
                        if backoff.is_completed() {
                            thread::yield_now();
                            backoff.reset();
                        } else {
                            backoff.snooze();
                        }
                    }
                }
            }
        });

        Worker {
            id,
            thread: Some(thread),
        }
    }
}

impl Drop for Worker {
    fn drop(&mut self) {
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
    }
}

enum WorkerMessage<I, O>
where
    I: IOParam,
    O: IOParam,
{
    Job(usize, I, Arc<dyn PipelineComponent<I, O> + Send + Sync>),
    Terminate,
}

/// A lock-free worker pool for processing jobs in parallel
#[derive(Debug)]
pub struct WorkerPool<I, O>
where
    I: IOParam,
    O: IOParam,
{
    size: usize,
    workers: Vec<Worker>,
    job_queue: Arc<ArrayQueue<Job<O>>>,
    results_rx: Receiver<LibResult<(usize, O)>>,
    running: Arc<AtomicBool>,
    active_jobs: Arc<AtomicUsize>,
    _phantom: std::marker::PhantomData<(I, O)>,
}

impl<I, O> WorkerPool<I, O>
where
    I: IOParam,
    O: IOParam,
{
    /// Create a new worker pool with the specified number of worker threads
    pub fn new(size: usize) -> Self {
        let queue_size = size * 4;

        let job_queue = Arc::new(ArrayQueue::new(queue_size));
        let (results_tx, results_rx) = channel::unbounded();
        let running = Arc::new(AtomicBool::new(true));
        let active_jobs = Arc::new(AtomicUsize::new(0));

        let mut workers = Vec::with_capacity(size);
        for id in 0..size {
            workers.push(Worker::new::<I, O>(
                id,
                Arc::clone(&job_queue),
                results_tx.clone(),
                Arc::clone(&running),
            ));
        }

        Self {
            size,
            workers,
            job_queue,
            results_rx,
            running,
            active_jobs,
            _phantom: std::marker::PhantomData,
        }
    }

    /// Process a batch of inputs in parallel using the given component
    pub fn process_batch<C>(
        &self,
        component: Arc<C>,
        inputs: Vec<(usize, I)>,
    ) -> Vec<(usize, LibResult<O>)>
    where
        C: PipelineComponent<I, O> + Send + Sync + 'static,
    {
        let total_jobs = inputs.len();
        self.active_jobs.fetch_add(total_jobs, Ordering::Relaxed);

        let backoff = crossbeam::utils::Backoff::new();

        for (idx, input) in inputs.iter() {
            let idx = *idx;
            let input = input.clone();
            let component = Arc::clone(&component);

            let job = Box::new(move || match component.process(input) {
                Ok(output) => Ok((idx, output)),
                Err(err) => Err(err),
            });

            loop {
                // Might not want to clone here???
                match self.job_queue.push(job.clone()) {
                    Ok(_) => {
                        break;
                    }
                    Err(_job) => {
                        backoff.snooze();
                        continue;
                    }
                }
            }
        }

        // Collect results from receiver channel
        let mut results = Vec::with_capacity(total_jobs);
        for _ in 0..total_jobs {
            match self.results_rx.recv() {
                Ok(result) => match result {
                    Ok((idx, output)) => {
                        results.push((idx, Ok(output)));
                    }
                    Err(e) => {
                        let failed_idx = inputs
                            .iter()
                            .find(|(i, _)| *i == results.len())
                            .map(|(i, _)| *i)
                            .unwrap_or(results.len());

                        results.push((failed_idx, Err(e)));
                    }
                },
                Err(_) => {
                    // Channel is closed
                    // This should never happen
                    break;
                }
            }
        }

        self.active_jobs.fetch_sub(total_jobs, Ordering::Relaxed);

        results.sort_by_key(|(idx, _)| *idx);
        results
    }

    /// Get the number of active jobs currently being processed
    pub fn active_jobs(&self) -> usize {
        self.active_jobs.load(Ordering::Relaxed)
    }

    /// Get the capacity of the job queue
    pub fn queue_capacity(&self) -> usize {
        self.job_queue.capacity()
    }
}

impl<I, O> Drop for WorkerPool<I, O>
where
    I: IOParam + Clone,
    O: IOParam,
{
    fn drop(&mut self) {
        // Signal workers to stop
        self.running.store(false, Ordering::Relaxed);
    }
}

/// Parallel processor for processing batches of inputs across multiple worker threads
pub struct WorkerParallelExecutor<I, O, C>
where
    I: IOParam,
    O: IOParam,
    C: PipelineComponent<I, O> + Send + Sync + 'static,
{
    component: Arc<C>,
    pool: WorkerPool<I, O>,
    _phantom: std::marker::PhantomData<(I, O)>,
}

impl<I, O, C> WorkerParallelExecutor<I, O, C>
where
    I: IOParam,
    O: IOParam,
    C: PipelineComponent<I, O> + Send + Sync + 'static,
{
    /// Create a new parallel processor with the given component and number of worker threads
    pub fn new(component: C, num_workers: usize) -> Self {
        Self {
            component: Arc::new(component),
            pool: WorkerPool::new(num_workers),
            _phantom: std::marker::PhantomData,
        }
    }

    /// Process a batch of inputs in parallel
    pub fn process_batch<T>(&self, inputs: Vec<(T, I)>) -> Vec<(T, LibResult<O>)>
    where
        T: Clone + Send + Sync + 'static,
    {
        let indexed_inputs: Vec<(usize, I)> = inputs
            .iter()
            .enumerate()
            .map(|(idx, (_, input))| (idx, input.clone()))
            .collect();

        let results = self
            .pool
            .process_batch(Arc::clone(&self.component), indexed_inputs);

        results
            .into_iter()
            .enumerate()
            .map(|(idx, (_, result))| (inputs[idx].0.clone(), result))
            .collect()
    }

    /// Get the number of worker threads in the pool
    pub fn worker_count(&self) -> usize {
        self.pool.size
    }

    /// Get the number of active jobs currently being processed
    pub fn active_jobs(&self) -> usize {
        self.pool.active_jobs()
    }

    /// Get the capacity of the job queue
    pub fn queue_capacity(&self) -> usize {
        self.pool.queue_capacity()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;

    #[derive(Debug, Clone)]
    struct TestInput(usize);

    #[derive(Debug, Clone)]
    struct TestOutput(usize);

    #[derive(Debug)]
    struct TestComponent;

    impl PipelineComponent<TestInput, TestOutput> for TestComponent {
        fn process(&self, input: TestInput) -> LibResult<TestOutput> {
            // Simulate some work
            std::thread::sleep(Duration::from_millis(10));
            Ok(TestOutput(input.0 * 2))
        }
    }

    #[test]
    fn test_worker_pool_processes_batch() {
        let component = TestComponent;
        let pool = WorkerPool::<TestInput, TestOutput>::new(4);

        let inputs = vec![
            (0, TestInput(1)),
            (1, TestInput(2)),
            (2, TestInput(3)),
            (3, TestInput(4)),
        ];

        let results = pool.process_batch(Arc::new(component), inputs);

        assert_eq!(results.len(), 4);
        for (idx, result) in results.iter() {
            match result {
                Ok(output) => {
                    assert_eq!(output.0, (*idx + 1) * 2);
                }
                Err(_) => panic!("Expected success"),
            }
        }
    }

    #[test]
    fn test_parallel_executor() {
        let executor = WorkerParallelExecutor::new(TestComponent, 4);

        let inputs = vec![
            ("a", TestInput(1)),
            ("b", TestInput(2)),
            ("c", TestInput(3)),
            ("d", TestInput(4)),
        ];

        let results = executor.process_batch(inputs);

        assert_eq!(results.len(), 4);

        let a_result = results.iter().find(|(key, _)| *key == "a").unwrap();
        match &a_result.1 {
            Ok(output) => assert_eq!(output.0, 2),
            Err(_) => panic!("Expected success"),
        }

        let d_result = results.iter().find(|(key, _)| *key == "d").unwrap();
        match &d_result.1 {
            Ok(output) => assert_eq!(output.0, 8),
            Err(_) => panic!("Expected success"),
        }
    }

    #[test]
    fn test_high_concurrency() {
        let processed = Arc::new(AtomicUsize::new(0));

        #[derive(Debug)]
        struct CountingComponent {
            counter: Arc<AtomicUsize>,
        }

        impl PipelineComponent<TestInput, TestOutput> for CountingComponent {
            fn process(&self, input: TestInput) -> LibResult<TestOutput> {
                self.counter.fetch_add(1, Ordering::Relaxed);
                Ok(TestOutput(input.0))
            }
        }

        let component = CountingComponent {
            counter: Arc::clone(&processed),
        };

        let executor = WorkerParallelExecutor::new(component, 8);

        // Create a large batch of work
        let inputs: Vec<(usize, TestInput)> = (0..1000).map(|i| (i, TestInput(i))).collect();

        let results = executor.process_batch(inputs);

        // All inputs should be processed
        assert_eq!(results.len(), 1000);
        assert_eq!(processed.load(Ordering::Relaxed), 1000);
    }
}
