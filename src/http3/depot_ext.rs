use salvo::Depot;

use crate::http3::error::ServerError;

pub(crate) trait DepotExt {
    fn require<T: Send + Sync + 'static>(&self) -> Result<&T, ServerError>;
}

impl DepotExt for Depot {
    fn require<T: Send + Sync + 'static>(&self) -> Result<&T, ServerError> {
        self.get_typed::<T>().map_err(|e| {
            ServerError::Internal(format!(
                "Failed to obtain {}: {:?}",
                std::any::type_name::<T>(),
                e
            ))
        })
    }
}
