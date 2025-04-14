use std::fmt::Debug;

use crate::error::LibError;

pub type LibResult<T> = std::result::Result<T, LibError>;

/// Trait for types that ca be used as input and output of components
pub trait IOParam: Send + Sync + Clone + Debug + 'static {}

/// Automatically implement `IOParam` for any type that implements `Send + Sync + Debug + 'static`
impl<T: Send + Sync + Debug + Clone + 'static> IOParam for T {}
