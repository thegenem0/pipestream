use std::error::Error;
use std::fmt::Debug;

pub type BoxedError = Box<dyn Error + Send + Sync>;


/// Trait for types that ca be used as input and output of components
pub trait IOParam: Send + Sync + Debug + 'static {}

/// Automatically implement `IOParam` for any type that implements `Send + Sync + Debug + 'static`
impl<T: Send + Sync + Debug + 'static> IOParam for T {}
