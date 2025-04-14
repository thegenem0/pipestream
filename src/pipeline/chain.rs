use std::collections::HashMap;

use crate::{
    common::{IOParam, LibResult},
    error::LibError,
    stage::DynStage,
};

#[derive(Debug)]
pub struct ChainedProcessor<I, M, O>
where
    I: IOParam,
    M: IOParam,
    O: IOParam,
{
    pub(crate) first: Box<dyn DynStage<I, M>>,
    pub(crate) second: Box<dyn DynStage<M, O>>,
}

impl<I, M, O> DynStage<I, O> for ChainedProcessor<I, M, O>
where
    I: IOParam,
    M: IOParam,
    O: IOParam,
{
    fn process(&self, input: I) -> LibResult<O> {
        let intermediate = self.first.process(input)?;
        self.second.process(intermediate)
    }

    fn process_batch(&self, batch: Vec<(usize, I)>) -> Vec<(usize, LibResult<O>)> {
        // Process through first stage
        let intermediate_results = self.first.process_batch(batch);

        // Create a new batch for the second stage with successful results
        let second_stage_inputs: Vec<_> = intermediate_results
            .iter()
            .filter_map(|(id, result)| result.as_ref().ok().map(|value| (*id, value.clone())))
            .collect();

        let second_stage_results = self.second.process_batch(second_stage_inputs);

        let mut result_map = HashMap::new();
        for (id, result) in second_stage_results {
            result_map.insert(id, result);
        }

        intermediate_results
            .into_iter()
            .map(|(id, result)| match result {
                Ok(_) => match result_map.remove(&id) {
                    Some(second_result) => (id, second_result),
                    None => (
                        id,
                        Err(LibError::component(
                            std::any::type_name::<I>(),
                            format!("Missing result for ID {}", id),
                        )),
                    ),
                },
                Err(e) => (id, Err(e)),
            })
            .collect()
    }

    // fn get_stage_info(&self) -> Vec<PipelineStageInfo> {
    //     let mut info = self.first.get_stage_info();
    //     info.extend(self.second.get_stage_info());
    //     info
    // }
}
