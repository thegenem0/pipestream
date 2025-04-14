use std::{
    sync::{Arc, Condvar, Mutex},
    time::Duration,
};

use crossbeam::channel::Sender;

use crate::{
    common::{IOParam, LibResult},
    pipeline::Pipeline,
};

use super::state::{ItemId, StreamingTracker};

#[derive(Debug)]
pub(super) struct InputProcessor<I, O>
where
    I: IOParam,
    O: IOParam,
{
    pipeline: Arc<Pipeline<I, O>>,
    tx: Sender<(ItemId, I)>,
    tracker: Arc<(Mutex<StreamingTracker<O>>, Condvar)>,
    running: Arc<Mutex<bool>>,
}

impl<I, O> InputProcessor<I, O>
where
    I: IOParam,
    O: IOParam,
{
    pub(super) fn new(pipeline: Arc<Pipeline<I, O>>) -> Self {
        let (input_tx, input_rx) = crossbeam::channel::unbounded::<(ItemId, I)>();
        let (output_tx, output_rx) = crossbeam::channel::unbounded::<(ItemId, LibResult<O>)>();

        let tracker: Arc<(Mutex<StreamingTracker<O>>, Condvar)> =
            Arc::new((Mutex::new(StreamingTracker::new()), Condvar::new()));

        let processor_tracker = Arc::clone(&tracker);
        let processor_pipeline = Arc::clone(&pipeline);
        let running = Arc::new(Mutex::new(true));
        let thread_running = Arc::clone(&running);

        std::thread::spawn(move || {
            const BATCH_SIZE: usize = 32;
            let mut batch = Vec::with_capacity(BATCH_SIZE);

            loop {
                if !*thread_running.lock().unwrap() {
                    break;
                }

                batch.clear();

                match input_rx.recv_timeout(Duration::from_millis(100)) {
                    Ok((id, input)) => {
                        {
                            let (tracker, _) = &*processor_tracker;
                            let mut tracker = tracker.lock().unwrap();
                            tracker.start_processing(id);
                        }

                        batch.push((id, input));

                        while batch.len() < BATCH_SIZE {
                            match input_rx.try_recv() {
                                Ok((id, input)) => {
                                    {
                                        let (tracker, _) = &*processor_tracker;
                                        let mut tracker = tracker.lock().unwrap();
                                        tracker.start_processing(id);
                                    }
                                    batch.push((id, input));
                                }
                                Err(_) => break,
                            }
                        }

                        let results = processor_pipeline.process_batch(batch.clone());

                        for (id, result) in results {
                            let _ = output_tx.send((id, result));
                        }
                    }
                    Err(crossbeam::channel::RecvTimeoutError::Timeout) => {
                        continue;
                    }
                    Err(crossbeam::channel::RecvTimeoutError::Disconnected) => {
                        break;
                    }
                }
            }
        });

        let results_tracker = Arc::clone(&tracker);

        std::thread::spawn(move || {
            while let Ok((id, result)) = output_rx.recv() {
                let (tracker, condvar) = &*results_tracker;
                let mut tracker_guard = tracker.lock().unwrap();

                match result {
                    Ok(output) => tracker_guard.complete_item(id, output),
                    Err(err) => tracker_guard.fail_item(id, err),
                }

                condvar.notify_all();
            }
        });

        Self {
            pipeline,
            tx: input_tx,
            tracker,
            running,
        }
    }

    /// Queue an input item for processing
    pub(super) fn process(&self, input: I) -> ItemId {
        let (tracker, _) = &*self.tracker;
        let mut tracker_guard = tracker.lock().unwrap();
        let id = tracker_guard.next_id();
        tracker_guard.queue_item(id);

        let _ = self.tx.send((id, input));

        id
    }

    pub(super) fn wait_for_next_completed(&self, timeout: Option<Duration>) -> Option<(ItemId, LibResult<O>)> {
        let (tracker, condvar) = &*self.tracker;
        let mut tracker_guard = tracker.lock().unwrap();

        if let Some(item) = tracker_guard.take_next_completed() {
            return Some(item);
        }

        if !tracker_guard.has_pending() {
            return None;
        }

        match timeout {
            Some(timeout) => {
                let (guard, result) = condvar
                    .wait_timeout_while(tracker_guard, timeout, |t| t.completed_count() == 0)
                    .unwrap();

                tracker_guard = guard;

                if result.timed_out() && tracker_guard.completed_count() == 0 {
                    return None;
                }
            }
            None => {
                tracker_guard = condvar
                    .wait_while(tracker_guard, |t| t.completed_count() == 0)
                    .unwrap();
            }
        }

        tracker_guard.take_next_completed()
    }

    fn is_done(&self) -> bool {
        let (tracker, _) = &*self.tracker;
        let tracker_guard = tracker.lock().unwrap();
        tracker_guard.completed_count() == 0 && !tracker_guard.has_pending()
    }
}

impl<I, O> Drop for InputProcessor<I, O>
where
    I: IOParam,
    O: IOParam,
{
    fn drop(&mut self) {
        *self.running.lock().unwrap() = false;
    }
}
