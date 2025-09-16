pub mod request;
pub mod response;

use std::hash::Hash;
use std::hash::Hasher;
use std::sync::Arc;

use crate::common::image::serialize_image_base64;
use bytes::Bytes;
use serde::Serialize;
use uuid::Uuid;

#[derive(Debug, Clone, Serialize)]
pub struct Task {
    // The unique ID of the task (UUIDv4).
    pub id: Uuid,
    // The task prompt
    #[serde(skip_serializing_if = "Option::is_none")]
    pub prompt: Option<Arc<String>>,
    // The task image data
    #[serde(
        skip_serializing_if = "Option::is_none",
        serialize_with = "serialize_image_base64"
    )]
    pub image: Option<Bytes>,
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
