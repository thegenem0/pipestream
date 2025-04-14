use std::collections::HashMap;

use crate::{
    common::{IOParam, LibResult},
    error::LibError,
};

pub type ItemId = u64;

#[derive(Debug)]
enum ItemState<O>
where
    O: IOParam,
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
    pub(super) fn new() -> Self {
        Self {
            next_id: 0,
            items: HashMap::new(),
        }
    }

    pub(super) fn next_id(&mut self) -> ItemId {
        let id = self.next_id;
        self.next_id += 1;
        id
    }

    pub(super) fn queue_item(&mut self, id: ItemId) {
        self.items.insert(id, ItemState::Queued);
    }

    pub(super) fn start_processing(&mut self, id: ItemId) {
        if let Some(state) = self.items.get_mut(&id) {
            *state = ItemState::Processing;
        }
    }

    pub(super) fn complete_item(&mut self, id: ItemId, output: O) {
        if let Some(state) = self.items.get_mut(&id) {
            *state = ItemState::Completed(output);
        }
    }

    pub(super) fn fail_item(&mut self, id: ItemId, error: LibError) {
        if let Some(state) = self.items.get_mut(&id) {
            *state = ItemState::Failed(error);
        }
    }

    pub(super) fn take_next_completed(&mut self) -> Option<(ItemId, LibResult<O>)> {
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

    pub(super) fn has_pending(&self) -> bool {
        self.items
            .iter()
            .any(|(_, state)| matches!(state, ItemState::Queued | ItemState::Processing))
    }

    /// Get number of pending items
    pub(super) fn pending_count(&self) -> usize {
        self.items
            .iter()
            .filter(|(_, state)| matches!(state, ItemState::Queued | ItemState::Processing))
            .count()
    }

    /// Get number of completed items waiting to be consumed
    pub(super) fn completed_count(&self) -> usize {
        self.items
            .iter()
            .filter(|(_, state)| matches!(state, ItemState::Completed(_) | ItemState::Failed(_)))
            .count()
    }
}
