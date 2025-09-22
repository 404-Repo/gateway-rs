use anyhow::{anyhow, Result};
use base64::{engine::general_purpose, Engine};
use blake2::{Blake2b512, Digest};
use parity_scale_codec::{Decode, Encode};
use schnorrkel::{context::signing_context, PublicKey, Signature};
use std::time::{SystemTime, UNIX_EPOCH};

use super::hotkey::Hotkey;

const GATEWAY: &str = "404_GATEWAY_";
const SIGNING_CTX: &[u8] = b"substrate";

#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug, Encode, Decode)]
pub struct AccountId32(pub [u8; 32]);

impl TryFrom<&[u8]> for AccountId32 {
    type Error = anyhow::Error;

    fn try_from(slice: &[u8]) -> Result<Self, Self::Error> {
        if slice.len() != 32 {
            Err(anyhow!(
                "Invalid slice length: expected 32, got {}",
                slice.len()
            ))
        } else {
            let arr = slice
                .try_into()
                .map_err(|_| anyhow!("Failed to convert slice into AccountId32"))?;
            Ok(AccountId32(arr))
        }
    }
}

impl AsRef<[u8; 32]> for AccountId32 {
    fn as_ref(&self) -> &[u8; 32] {
        &self.0
    }
}

pub struct KeypairSignature(pub [u8; 64]);

pub fn ss58_encode(pubkey: &[u8; 32]) -> String {
    const SS58_PREFIX: &[u8] = b"SS58PRE";
    const VERSION: u8 = 42;
    const PREFIX_LEN: usize = SS58_PREFIX.len();
    const CHECKSUM_INPUT_SIZE: usize = PREFIX_LEN + 1 + 32;
    const PAYLOAD_SIZE: usize = 1 + 32 + 2;

    let mut payload = [0u8; PAYLOAD_SIZE];
    payload[0] = VERSION;
    payload[1..33].copy_from_slice(pubkey);

    let mut checksum_input = [0u8; CHECKSUM_INPUT_SIZE];
    checksum_input[..PREFIX_LEN].copy_from_slice(SS58_PREFIX);
    checksum_input[PREFIX_LEN] = VERSION;
    checksum_input[PREFIX_LEN + 1..].copy_from_slice(pubkey);

    let hash = Blake2b512::digest(checksum_input);
    let checksum = &hash[..2];

    payload[33..].copy_from_slice(checksum);

    bs58::encode(&payload).into_string()
}

pub fn ss58_decode(address: &str) -> Result<AccountId32> {
    let decoded = bs58::decode(address).into_vec()?;
    if decoded.len() != 35 {
        return Err(anyhow::anyhow!(
            "Invalid SS58 address length: expected 35, got {} bytes",
            decoded.len()
        ));
    }

    if decoded[0] != 42 {
        return Err(anyhow::anyhow!(
            "Invalid SS58 version: expected 42, found {}",
            decoded[0]
        ));
    }

    let mut checksum_input = Vec::with_capacity(b"SS58PRE".len() + 33);
    checksum_input.extend_from_slice(b"SS58PRE");
    checksum_input.extend_from_slice(&decoded[..33]);

    let hash = Blake2b512::digest(&checksum_input);
    let expected_checksum = &hash[..2];
    if decoded[33..35] != expected_checksum[..] {
        return Err(anyhow::anyhow!("Checksum mismatch in SS58 address"));
    }

    let pubkey: AccountId32 = decoded[1..33]
        .try_into()
        .map_err(|_| anyhow::anyhow!("Failed to convert pubkey slice into AccountId32"))?;
    Ok(pubkey)
}

pub fn verify_signature(
    public_key: &AccountId32,
    signature: &KeypairSignature,
    message: impl AsRef<[u8]>,
) -> Result<()> {
    let pk =
        PublicKey::from_bytes(&public_key.0).map_err(|e| anyhow!("Invalid public key: {:?}", e))?;
    let sig =
        Signature::from_bytes(&signature.0).map_err(|e| anyhow!("Invalid signature: {:?}", e))?;

    let ctx = signing_context(SIGNING_CTX);
    pk.verify(ctx.bytes(message.as_ref()), &sig)
        .map_err(|e| anyhow!("Signature verification failed: {:?}", e))
}

pub fn verify_hotkey(
    timestamp: &str,
    validator_hotkey: &Hotkey,
    signature: &str,
    freshness_threshold_sec: u64,
) -> Result<()> {
    // The timestamp must be in the format "404_GATEWAY_<number>"
    if !timestamp.starts_with(GATEWAY) {
        return Err(anyhow!(
            "Timestamp does not start with required prefix '{}'",
            GATEWAY
        ));
    }
    // Remove the prefix and parse the numeric part.
    let ts_str = &timestamp[GATEWAY.len()..];
    let ts: u64 = ts_str
        .parse()
        .map_err(|e| anyhow!("Failed to parse timestamp part '{}': {}", ts_str, e))?;

    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|e| anyhow!("System time error: {}", e))?
        .as_secs();

    if ts > now {
        return Err(anyhow!("Timestamp is in the future"));
    }
    if now - ts > freshness_threshold_sec {
        return Err(anyhow!(
            "Timestamp is older than allowed threshold ({} seconds)",
            freshness_threshold_sec
        ));
    }

    // Decode hotkey and signature.
    let hotkey =
        ss58_decode(validator_hotkey).map_err(|e| anyhow!("Failed to decode hotkey: {}", e))?;

    let mut signature_bytes = [0u8; 64];
    let decoded_len = general_purpose::STANDARD
        .decode_slice(signature, &mut signature_bytes)
        .map_err(|e| anyhow!("Failed to decode signature: {}", e))?;
    if decoded_len != 64 {
        return Err(anyhow!("Invalid signature length"));
    }

    // Use the entire timestamp string (including "404_GATEWAY_") as the message.
    verify_signature(
        &hotkey,
        &KeypairSignature(signature_bytes),
        timestamp.as_bytes(),
    )
}

#[cfg(test)]
mod tests {
    use super::ss58_encode;
    use crate::bittensor::crypto::{verify_hotkey, GATEWAY, SIGNING_CTX};
    use base64::{engine::general_purpose, Engine};
    use schnorrkel::{context::signing_context, ExpansionMode, MiniSecretKey};
    use std::time::{SystemTime, UNIX_EPOCH};

    fn current_timestamp() -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs()
    }

    #[test]
    fn test_valid_signature() {
        let seed = [1u8; 32];
        let mini = MiniSecretKey::from_bytes(&seed).unwrap();
        let keypair = mini.expand_to_keypair(ExpansionMode::Uniform);
        let ts = current_timestamp();
        // Build timestamp string with required prefix.
        let timestamp_string = format!("{}{}", GATEWAY, ts);
        let ctx = signing_context(SIGNING_CTX);
        let signature = keypair.sign(ctx.bytes(timestamp_string.as_bytes()));
        assert!(verify_hotkey(
            &timestamp_string,
            &ss58_encode(&keypair.public.to_bytes()).parse().unwrap(),
            &general_purpose::STANDARD.encode(signature.to_bytes()),
            300
        )
        .is_ok());
    }

    #[test]
    fn test_invalid_signature() {
        let seed = [1u8; 32];
        let mini = MiniSecretKey::from_bytes(&seed).unwrap();
        let keypair = mini.expand_to_keypair(ExpansionMode::Uniform);
        let ts = current_timestamp();
        let timestamp_string = format!("{}{}", GATEWAY, ts);
        let ctx = signing_context(SIGNING_CTX);
        let signature = keypair.sign(ctx.bytes(timestamp_string.as_bytes()));
        let mut sig_bytes = signature.to_bytes();
        sig_bytes[0] ^= 0x01; // Tamper with the signature.
        assert!(verify_hotkey(
            &timestamp_string,
            &ss58_encode(&keypair.public.to_bytes()).parse().unwrap(),
            &general_purpose::STANDARD.encode(sig_bytes),
            300
        )
        .is_err());
    }

    #[test]
    fn test_old_timestamp() {
        let seed = [2u8; 32];
        let mini = MiniSecretKey::from_bytes(&seed).unwrap();
        let keypair = mini.expand_to_keypair(ExpansionMode::Uniform);
        // Subtract a value greater than the allowed threshold.
        let ts = current_timestamp().saturating_sub(301);
        let timestamp_string = format!("{}{}", GATEWAY, ts);
        let ctx = signing_context(SIGNING_CTX);
        let signature = keypair.sign(ctx.bytes(timestamp_string.as_bytes()));
        assert!(verify_hotkey(
            &timestamp_string,
            &ss58_encode(&keypair.public.to_bytes()).parse().unwrap(),
            &general_purpose::STANDARD.encode(signature.to_bytes()),
            300
        )
        .is_err());
    }

    #[test]
    fn test_invalid_timestamp_format() {
        // Timestamp string without the required prefix.
        let bad_timestamp = "dead_code_123456".to_string();
        assert!(verify_hotkey(
            &bad_timestamp,
            &ss58_encode(&[0u8; 32]).parse().unwrap(),
            &general_purpose::STANDARD.encode([0u8; 64]),
            300
        )
        .is_err());
    }
}
