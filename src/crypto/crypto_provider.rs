use anyhow::{Result, anyhow};
use blake3::Hash;
use rustls::crypto::CryptoProvider;
use std::sync::Arc;

pub fn init_crypto_provider() -> Result<()> {
    if CryptoProvider::get_default().is_some() {
        return Ok(());
    }
    rustls::crypto::aws_lc_rs::default_provider()
        .install_default()
        .map_err(|_| anyhow::anyhow!("Failed to install aws-lc-rs as default provider"))?;
    Ok(())
}

#[derive(Clone)]
pub struct ApiKeyHasher {
    key: Arc<[u8; 32]>,
}

fn load_key(secret: &str, field_name: &str) -> Result<Arc<[u8; 32]>> {
    let bytes = secret.as_bytes();
    if bytes.len() != 32 {
        return Err(anyhow!(
            "{field_name} must be exactly 32 bytes (got {} bytes)",
            bytes.len()
        ));
    }
    let mut key = [0u8; 32];
    key.copy_from_slice(bytes);
    Ok(Arc::new(key))
}

impl ApiKeyHasher {
    pub fn new(secret: &str) -> Result<Self> {
        Ok(Self {
            key: load_key(secret, "api_key_secret")?,
        })
    }

    fn compute_hash(&self, api_key: &str) -> Hash {
        blake3::keyed_hash(&self.key, api_key.as_bytes())
    }

    pub fn compute_hash_array(&self, api_key: &str) -> [u8; 32] {
        *self.compute_hash(api_key).as_bytes()
    }
}

#[derive(Clone)]
pub struct GuestIpHasher {
    key: Arc<[u8; 32]>,
}

impl GuestIpHasher {
    pub fn new(secret: &str) -> Result<Self> {
        Ok(Self {
            key: load_key(secret, "api_key_secret")?,
        })
    }

    pub fn compute_hash_128(&self, subject: &str) -> [u8; 16] {
        let mut hasher = blake3::Hasher::new_keyed(self.key.as_ref());
        hasher.update(b"guest-ip:v1:");
        hasher.update(subject.as_bytes());
        let digest = hasher.finalize();
        let mut truncated = [0u8; 16];
        truncated.copy_from_slice(&digest.as_bytes()[..16]);
        truncated
    }
}
