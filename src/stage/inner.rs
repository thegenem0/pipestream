use crate::common::{IOParam, LibResult};
use std::fmt::Debug;

pub trait StageImpl<I: IOParam, O: IOParam>: Send + Sync + Debug {
    fn process(&self, input: I) -> LibResult<O>;

    fn name(&self) -> String {
        std::any::type_name::<Self>()
            .split("::")
            .last()
            .unwrap_or("Unknown")
            .to_string()
    }
}

impl<I, O> StageImpl<I, O> for Box<dyn StageImpl<I, O> + Send + Sync>
where
    I: IOParam + Clone,
    O: IOParam,
{
    fn process(&self, input: I) -> LibResult<O> {
        (**self).process(input)
    }

    fn name(&self) -> String {
        (**self).name()
    }
}
