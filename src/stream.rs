use std::{
    collections::HashMap,
    sync::{Arc, Condvar, Mutex},
    time::Duration,
};

use crossbeam::{
    channel::{Receiver, Sender},
    sync::ShardedLock,
};

use crate::{
    common::{IOParam, LibResult},
    error::LibError,
    pipeline::Pipeline,
};

pub type ItemId = u64;

#[derive(Debug)]
enum ItemState<O>
where
    O: IOParam + Clone,
{
    Queued,
    Processing,
    Completed(O),
    Failed(LibError),
}

impl<O> Clone for ItemState<O>
where
    O: IOParam + Clone,
{
    fn clone(&self) -> Self {
        match self {
            Self::Queued => Self::Queued,
            Self::Processing => Self::Processing,
            Self::Completed(output) => Self::Completed(output.clone()),
            Self::Failed(err) => Self::Failed(err.clone()),
        }
    }
}

#[derive(Debug)]
pub struct StreamingTracker<O>
where
    O: IOParam,
{
    next_id: ItemId,
    items: HashMap<ItemId, ItemState<O>>,
}

impl<O: Clone> StreamingTracker<O>
where
    O: IOParam,
{
    fn new() -> Self {
        Self {
            next_id: 0,
            items: HashMap::new(),
        }
    }

    fn next_id(&mut self) -> ItemId {
        let id = self.next_id;
        self.next_id += 1;
        id
    }

    fn queue_item(&mut self, id: ItemId) {
        self.items.insert(id, ItemState::Queued);
    }

    fn start_processing(&mut self, id: ItemId) {
        if let Some(state) = self.items.get_mut(&id) {
            *state = ItemState::Processing;
        }
    }

    fn complete_item(&mut self, id: ItemId, output: O) {
        if let Some(state) = self.items.get_mut(&id) {
            *state = ItemState::Completed(output);
        }
    }

    fn fail_item(&mut self, id: ItemId, error: LibError) {
        if let Some(state) = self.items.get_mut(&id) {
            *state = ItemState::Failed(error);
        }
    }

    fn take_next_completed(&mut self) -> Option<(ItemId, LibResult<O>)> {
        let completed_id = self.items.iter().find_map(|(id, state)| match state {
            ItemState::Completed(_) | ItemState::Failed(_) => Some(*id),
            _ => None,
        });

        if let Some(id) = completed_id {
            match self.items.remove(&id).unwrap() {
                ItemState::Completed(output) => Some((id, Ok(output))),
                ItemState::Failed(error) => Some((id, Err(error))),
                _ => unreachable!(),
            }
        } else {
            None
        }
    }

    fn has_pending(&self) -> bool {
        self.items
            .iter()
            .any(|(_, state)| matches!(state, ItemState::Queued | ItemState::Processing))
    }

    /// Get number of pending items
    fn pending_count(&self) -> usize {
        self.items
            .iter()
            .filter(|(_, state)| matches!(state, ItemState::Queued | ItemState::Processing))
            .count()
    }

    /// Get number of completed items waiting to be consumed
    fn completed_count(&self) -> usize {
        self.items
            .iter()
            .filter(|(_, state)| matches!(state, ItemState::Completed(_) | ItemState::Failed(_)))
            .count()
    }
}

#[derive(Debug)]
struct InputProcessor<I, O>
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
    fn new(pipeline: Arc<Pipeline<I, O>>) -> Self {
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
    fn process(&self, input: I) -> ItemId {
        let (tracker, _) = &*self.tracker;
        let mut tracker_guard = tracker.lock().unwrap();
        let id = tracker_guard.next_id();
        tracker_guard.queue_item(id);

        let _ = self.tx.send((id, input));

        id
    }

    fn wait_for_next_completed(&self, timeout: Option<Duration>) -> Option<(ItemId, LibResult<O>)> {
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

pub struct PipelineStream<I, O, T>
where
    I: IOParam + Clone,
    O: IOParam + Clone,
    T: Send + Sync + Clone + 'static,
{
    processor: Arc<InputProcessor<I, O>>,
    map: Arc<ShardedLock<HashMap<ItemId, T>>>,
}

impl<I, O, T> PipelineStream<I, O, T>
where
    I: IOParam,
    O: IOParam,
    T: Send + Sync + Clone + 'static,
{
    pub fn new(pipeline: Arc<Pipeline<I, O>>) -> Self {
        Self {
            processor: Arc::new(InputProcessor::new(pipeline)),
            map: Arc::new(ShardedLock::new(HashMap::new())),
        }
    }

    pub fn process_stream<IntoIter>(
        &self,
        input_stream: IntoIter,
        buffer_size: usize,
    ) -> Receiver<(T, LibResult<O>)>
    where
        IntoIter: IntoIterator<Item = (T, I)> + Send + 'static,
        IntoIter::IntoIter: Send + 'static,
    {
        let (output_tx, output_rx) = if buffer_size > 0 {
            crossbeam::channel::bounded(buffer_size)
        } else {
            crossbeam::channel::unbounded()
        };

        let processor = Arc::clone(&self.processor);
        let map = Arc::clone(&self.map);

        let input_processor = Arc::clone(&processor);
        let input_map = Arc::clone(&map);

        std::thread::spawn(move || {
            for (id, input) in input_stream {
                let item_id = input_processor.process(input);

                let mut id_map = input_map.write().unwrap();
                id_map.insert(item_id, id);
            }
        });

        std::thread::spawn(move || {
            while let Some((item_id, result)) = processor.wait_for_next_completed(None) {
                let id = {
                    let id_map = map.read().unwrap();
                    id_map.get(&item_id).cloned()
                };

                if let Some(id) = id {
                    {
                        let mut id_map = map.write().unwrap();
                        id_map.remove(&item_id);
                    }

                    if output_tx.send((id, result)).is_err() {
                        break;
                    }
                }
            }
        });

        output_rx
    }

    pub fn process_batch<IntoIter>(&self, inputs: IntoIter) -> Vec<(T, LibResult<O>)>
    where
        IntoIter: IntoIterator<Item = (T, I)>,
    {
        let inputs_vec: Vec<_> = inputs.into_iter().collect();
        let input_count = inputs_vec.len();

        for (id, input) in &inputs_vec {
            let item_id = self.processor.process(input.clone());

            let mut id_map = self.map.write().unwrap();
            id_map.insert(item_id, id.clone());
        }

        let mut results = Vec::with_capacity(input_count);
        while results.len() < input_count {
            if let Some((item_id, result)) = self.processor.wait_for_next_completed(None) {
                let id = {
                    let id_map = self.map.read().unwrap();
                    id_map.get(&item_id).cloned()
                };

                if let Some(id) = id {
                    {
                        let mut id_map = self.map.write().unwrap();
                        id_map.remove(&item_id);
                    }

                    results.push((id, result));
                }
            } else {
                break;
            }
        }

        results
    }

    pub fn process(&self, input: I) -> LibResult<O> {
        let item_id = self.processor.process(input);

        let marker_id = ItemId::MAX;

        {
            let mut id_map = self.map.write().unwrap();
            let placeholder: T = unsafe { std::mem::zeroed() };
            id_map.insert(item_id, placeholder);
        }

        while let Some((processed_id, result)) = self.processor.wait_for_next_completed(None) {
            if processed_id == marker_id {
                {
                    let mut id_map = self.map.write().unwrap();
                    id_map.remove(&marker_id);
                }
                return result;
            }
        }

        Err(LibError::component(
            std::any::type_name::<I>(),
            "Failed to process input".to_string(),
        ))
    }
}

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
