pub mod request;
pub mod response;

use std::hash::Hash;
use std::hash::Hasher;

use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Task {
    // The unique ID of the task (UUIDv4).
    pub id: Uuid,
    // The task prompt.
    pub prompt: String,
}

pub trait HasUuid {
    fn id(&self) -> &Uuid;
}

impl HasUuid for Task {
    fn id(&self) -> &Uuid {
        &self.id
    }
}

impl PartialEq for Task {
    fn eq(&self, other: &Self) -> bool {
        // Compare only the id fields
        self.id == other.id
    }
}

impl Eq for Task {}

impl Hash for Task {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.id.hash(state);
    }
}
