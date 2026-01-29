use std::sync::Arc;

use tokio::sync::{OwnedSemaphorePermit, Semaphore};

use crate::http3::error::ServerError;

#[derive(Clone)]
pub struct ImageUploadLimiter {
    semaphore: Arc<Semaphore>,
}

impl ImageUploadLimiter {
    pub fn new(max_concurrent_uploads: usize) -> Self {
        let max = max_concurrent_uploads.max(1);
        Self {
            semaphore: Arc::new(Semaphore::new(max)),
        }
    }

    pub async fn acquire(&self) -> Result<OwnedSemaphorePermit, ServerError> {
        self.semaphore
            .clone()
            .acquire_owned()
            .await
            .map_err(|e| ServerError::Internal(format!("Image upload limiter closed: {}", e)))
    }
}
