use anyhow::Result;
use rustls::crypto::CryptoProvider;

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
