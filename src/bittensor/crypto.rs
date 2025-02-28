use anyhow::{anyhow, Result};
use base64::{engine::general_purpose, Engine};
use blake2::{Blake2b512, Digest};
use schnorrkel::{context::signing_context, PublicKey, Signature};
use std::time::{SystemTime, UNIX_EPOCH};

use crate::api::request::GetTasksRequest;

const GATEWAY: &str = "404_GATEWAY_";

pub struct AccountId(pub [u8; 32]);

impl TryFrom<&[u8]> for AccountId {
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
                .map_err(|_| anyhow!("Failed to convert slice into AccountId"))?;
            Ok(AccountId(arr))
        }
    }
}

impl AsRef<[u8; 32]> for AccountId {
    fn as_ref(&self) -> &[u8; 32] {
        &self.0
    }
}

pub struct KeypairSignature(pub [u8; 64]);

pub fn _ss58_encode(pubkey: &[u8; 32]) -> String {
    const SS58_PREFIX: &[u8] = b"SS58PRE";
    let version: u8 = 42;
    let mut payload = Vec::with_capacity(1 + 32);
    payload.push(version);
    payload.extend_from_slice(pubkey);
    let mut checksum_input = Vec::with_capacity(SS58_PREFIX.len() + payload.len());
    checksum_input.extend_from_slice(SS58_PREFIX);
    checksum_input.extend_from_slice(&payload);
    let hash = Blake2b512::digest(&checksum_input);
    let checksum = &hash[..2];
    payload.extend_from_slice(checksum);
    bs58::encode(payload).into_string()
}

pub fn ss58_decode(address: &str) -> Result<AccountId> {
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

    let pubkey: AccountId = decoded[1..33]
        .try_into()
        .map_err(|_| anyhow::anyhow!("Failed to convert pubkey slice into AccountId"))?;
    Ok(pubkey)
}

pub fn verify_signature(
    account_id: &AccountId,
    signature: &KeypairSignature,
    message: impl AsRef<[u8]>,
) -> Result<()> {
    let pk =
        PublicKey::from_bytes(&account_id.0).map_err(|e| anyhow!("Invalid public key: {:?}", e))?;
    let sig =
        Signature::from_bytes(&signature.0).map_err(|e| anyhow!("Invalid signature: {:?}", e))?;

    let ctx = signing_context(b"");
    pk.verify(ctx.bytes(message.as_ref()), &sig)
        .map_err(|e| anyhow!("Signature verification failed: {:?}", e))
}

pub fn verify_hotkey(request: &GetTasksRequest, freshness_threshold_sec: u64) -> Result<()> {
    // The timestamp must be in the format "404_GATEWAY_<number>"
    if !request.timestamp.starts_with(GATEWAY) {
        return Err(anyhow!(
            "Timestamp does not start with required prefix '{}'",
            GATEWAY
        ));
    }
    // Remove the prefix and parse the numeric part.
    let ts_str = &request.timestamp[GATEWAY.len()..];
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
        ss58_decode(&request.hotkey).map_err(|e| anyhow!("Failed to decode hotkey: {}", e))?;

    let signature = general_purpose::STANDARD
        .decode(&request.signature)
        .map_err(|e| anyhow!("Failed to decode signature: {}", e))?;
    if signature.len() != 64 {
        return Err(anyhow!("Invalid signature length"));
    }

    let mut hotkey_arr = [0u8; 32];
    hotkey_arr.copy_from_slice(hotkey.as_ref());

    let mut sig_arr = [0u8; 64];
    sig_arr.copy_from_slice(&signature);

    // Use the entire timestamp string (including "404_GATEWAY_") as the message.
    verify_signature(
        &AccountId(hotkey_arr),
        &KeypairSignature(sig_arr),
        request.timestamp.as_bytes(),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use schnorrkel::{ExpansionMode, MiniSecretKey};
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
        let ctx = signing_context(b"");
        let signature = keypair.sign(ctx.bytes(timestamp_string.as_bytes()));
        let request = GetTasksRequest {
            hotkey: _ss58_encode(&keypair.public.to_bytes()),
            signature: general_purpose::STANDARD.encode(signature.to_bytes()),
            timestamp: timestamp_string,
            requested_task_count: 5,
        };
        assert!(verify_hotkey(&request, 300).is_ok());
    }

    #[test]
    fn test_invalid_signature() {
        let seed = [1u8; 32];
        let mini = MiniSecretKey::from_bytes(&seed).unwrap();
        let keypair = mini.expand_to_keypair(ExpansionMode::Uniform);
        let ts = current_timestamp();
        let timestamp_string = format!("{}{}", GATEWAY, ts);
        let ctx = signing_context(b"");
        let signature = keypair.sign(ctx.bytes(timestamp_string.as_bytes()));
        let mut sig_bytes = signature.to_bytes();
        sig_bytes[0] ^= 0x01; // Tamper with the signature.
        let request = GetTasksRequest {
            hotkey: _ss58_encode(&keypair.public.to_bytes()),
            signature: general_purpose::STANDARD.encode(sig_bytes),
            timestamp: timestamp_string,
            requested_task_count: 5,
        };
        assert!(verify_hotkey(&request, 300).is_err());
    }

    #[test]
    fn test_old_timestamp() {
        let seed = [2u8; 32];
        let mini = MiniSecretKey::from_bytes(&seed).unwrap();
        let keypair = mini.expand_to_keypair(ExpansionMode::Uniform);
        // Subtract a value greater than the allowed threshold.
        let ts = current_timestamp().saturating_sub(301);
        let timestamp_string = format!("{}{}", GATEWAY, ts);
        let ctx = signing_context(b"");
        let signature = keypair.sign(ctx.bytes(timestamp_string.as_bytes()));
        let request = GetTasksRequest {
            hotkey: _ss58_encode(&keypair.public.to_bytes()),
            signature: general_purpose::STANDARD.encode(signature.to_bytes()),
            timestamp: timestamp_string,
            requested_task_count: 5,
        };
        assert!(verify_hotkey(&request, 300).is_err());
    }

    #[test]
    fn test_invalid_timestamp_format() {
        // Timestamp string without the required prefix.
        let bad_timestamp = "dead_code_123456".to_string();
        let request = GetTasksRequest {
            // Again, use a valid SS58 encoded hotkey.
            hotkey: _ss58_encode(&[0u8; 32]),
            signature: general_purpose::STANDARD.encode([0u8; 64]),
            timestamp: bad_timestamp,
            requested_task_count: 5,
        };
        assert!(verify_hotkey(&request, 300).is_err());
    }
}
