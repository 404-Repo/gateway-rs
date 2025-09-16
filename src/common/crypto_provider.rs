use anyhow::{anyhow, Result};
use argon2::{
    password_hash::{PasswordHash, PasswordVerifier},
    Argon2,
};
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
    argon2: Arc<Argon2<'static>>,
}

impl ApiKeyHasher {
    pub fn new() -> Result<Self> {
        use argon2::{Algorithm, Params, Version};

        let params = Params::new(
            9216, // memory cost (memoryCost: 9216)
            4,    // time cost (timeCost: 4)
            1,    // parallelism (parallelism: 1)
            None,
        )
        .map_err(|e| anyhow!("Invalid Argon2 parameters: {}", e))?;

        let argon2 = Argon2::new(Algorithm::Argon2d, Version::V0x13, params);

        Ok(Self {
            argon2: Arc::new(argon2),
        })
    }

    pub fn verify_api_key(&self, api_key: &str, hash_str: &str) -> Result<bool> {
        let parsed_hash = PasswordHash::new(hash_str)
            .map_err(|e| anyhow!("Failed to parse password hash: {}", e))?;

        match self
            .argon2
            .verify_password(api_key.as_bytes(), &parsed_hash)
        {
            Ok(()) => Ok(true),
            Err(argon2::password_hash::Error::Password) => Ok(false),
            Err(e) => Err(anyhow!("Error verifying password: {}", e)),
        }
    }
}
