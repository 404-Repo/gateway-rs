use anyhow::{anyhow, Result};
use blake3::Hash;
use rustls::crypto::CryptoProvider;
use std::sync::Arc;

pub fn init_crypto_provider() -> Result<()> {
    rustls::crypto::aws_lc_rs::default_provider()
        .install_default()
        .or_else(|_| {
            CryptoProvider::get_default()
                .map(|_| ())
                .ok_or_else(|| anyhow::anyhow!("Failed to locate any crypto provider"))
        })?;
    Ok(())
}

#[derive(Clone)]
pub struct ApiKeyHasher {
    key: Arc<[u8; 32]>,
}

impl ApiKeyHasher {
    pub fn new(secret: &str) -> Result<Self> {
        let bytes = secret.as_bytes();
        if bytes.len() != 32 {
            return Err(anyhow!(
                "api_key_secret must be exactly 32 bytes (got {} bytes)",
                bytes.len()
            ));
        }
        let mut key = [0u8; 32];
        key.copy_from_slice(bytes);
        Ok(Self { key: Arc::new(key) })
    }

    fn compute_hash(&self, api_key: &str) -> Hash {
        blake3::keyed_hash(&self.key, api_key.as_bytes())
    }

    pub fn compute_hash_array(&self, api_key: &str) -> [u8; 32] {
        *self.compute_hash(api_key).as_bytes()
    }
}
