use std::{collections::HashMap, sync::Arc};

use crossbeam::{channel::Receiver, sync::ShardedLock};

use crate::{
    common::{IOParam, LibResult},
    error::LibError,
    pipeline::Pipeline,
};

use super::{input::InputProcessor, state::ItemId};

pub struct PipelineStream<I, O, T>
where
    I: IOParam,
    O: IOParam,
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
