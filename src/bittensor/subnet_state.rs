#![allow(dead_code)]

use anyhow::anyhow;
use anyhow::Result;
use async_tungstenite::tokio::connect_async_with_config;
use async_tungstenite::tungstenite::protocol::WebSocketConfig;
use async_tungstenite::tungstenite::Message;
use foldhash::HashMap;
use foldhash::HashMapExt;
use futures_util::StreamExt;
use hex;
use itertools::izip;
use parity_scale_codec::{Compact, Decode, Encode};
use serde_json::{json, Value};
use std::fmt;
use std::time::Duration;
use tokio::sync::watch;
use tracing::error;
use tracing::info;

use super::crypto::ss58_decode;

pub type AccountId = [u8; 32];

// Other fields will be used later
#[derive(Debug, Clone)]
pub struct HotkeyData {
    emission: u64,
    alpha_stake: u64,
    total_stake: u64,
    tao_stake: u64,
    coldkey: AccountId,
}

#[derive(Decode, Encode, PartialEq, Eq, Clone, Debug)]
pub struct State {
    pub netuid: Compact<u16>,
    pub hotkeys: Vec<AccountId>,
    pub coldkeys: Vec<AccountId>,
    pub active: Vec<bool>,
    pub validator_permit: Vec<bool>,
    pub pruning_score: Vec<Compact<u16>>,
    pub last_update: Vec<Compact<u64>>,
    pub emission: Vec<Compact<u64>>,
    pub dividends: Vec<Compact<u16>>,
    pub incentives: Vec<Compact<u16>>,
    pub consensus: Vec<Compact<u16>>,
    pub trust: Vec<Compact<u16>>,
    pub rank: Vec<Compact<u16>>,
    pub block_at_registration: Vec<Compact<u64>>,
    pub alpha_stake: Vec<Compact<u64>>,
    pub tao_stake: Vec<Compact<u64>>,
    pub total_stake: Vec<Compact<u64>>,
    pub emission_history: Vec<Vec<Compact<u64>>>,
}

pub struct SubnetState {
    receiver: watch::Receiver<HashMap<AccountId, HotkeyData>>,
    join_handle: tokio::task::JoinHandle<()>,
}

impl SubnetState {
    pub fn new(
        wss_bittensor: String,
        netuid: u16,
        block: Option<u64>,
        poll_interval: Duration,
        wss_max_message_size: usize,
    ) -> Self {
        let (tx, rx) = watch::channel(HashMap::with_capacity(256));

        let join_handle = tokio::spawn(async move {
            loop {
                info!("Updating subnet state {netuid} with wss: {wss_bittensor}");
                match get_subnet_state(&wss_bittensor, netuid, block, wss_max_message_size).await {
                    Ok(new_state) => {
                        if let Err(e) = tx.send(new_state) {
                            error!("Failed to send updated subnet state: {:?}", e);
                        }
                    }
                    Err(e) => {
                        error!("Error updating subnet state: {:?}", e);
                    }
                }
                tokio::time::sleep(poll_interval).await;
            }
        });

        SubnetState {
            receiver: rx,
            join_handle,
        }
    }

    pub fn validate_hotkey(&self, hotkey: &str) -> Result<()> {
        let account_id = ss58_decode(hotkey)?;

        let total_stake = {
            let state = self.receiver.borrow();
            let hotkey_data = state
                .get(&account_id.0)
                .ok_or_else(|| anyhow!("Hotkey not found in the subnet"))?;
            hotkey_data.total_stake
        };

        // 10000 TAO
        let required_stake: u64 = 10000 * 1_000_000_000;

        if total_stake > required_stake {
            Ok(())
        } else {
            Err(anyhow!(
                "Hotkey found but does not have enough total stake: {} < {}",
                total_stake,
                required_stake
            ))
        }
    }

    pub fn current_state(&self) -> tokio::sync::watch::Ref<'_, HashMap<AccountId, HotkeyData>> {
        self.receiver.borrow()
    }

    pub fn abort(&self) {
        self.join_handle.abort();
    }
}

// Useful for debugging
pub struct Balance {
    pub rao: u64,
}

impl Balance {
    pub fn new(rao: u64) -> Self {
        Self { rao }
    }
}

impl fmt::Display for Balance {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        const RAO_PER_TAO: u64 = 1_000_000_000;
        let whole_tao = self.rao / RAO_PER_TAO;
        let frac_rao = self.rao % RAO_PER_TAO;
        if frac_rao == 0 {
            write!(f, "{} TAO ({} RAO)", whole_tao, self.rao)
        } else {
            let mut frac_str = format!("{:09}", frac_rao);
            frac_str = frac_str.trim_end_matches('0').to_string();
            write!(f, "{}.{} TAO ({} RAO)", whole_tao, frac_str, self.rao)
        }
    }
}

async fn get_text_message<S>(stream: &mut S, expected_id: u64) -> Result<String>
where
    S: StreamExt<Item = Result<Message, async_tungstenite::tungstenite::Error>> + Unpin,
{
    while let Some(msg) = stream.next().await {
        let msg = msg?;
        if let Message::Text(text) = msg {
            if text.trim().is_empty() {
                continue;
            }
            let resp: Value = serde_json::from_str(&text)?;
            if resp["id"].as_u64() == Some(expected_id) {
                return Ok(text.to_string());
            }
        }
    }
    Err(anyhow!(
        "No valid response with id {} received",
        expected_id
    ))
}

fn build_hotkeys_state(subnet_state: State) -> Result<HashMap<AccountId, HotkeyData>> {
    let State {
        hotkeys,
        coldkeys,
        emission,
        alpha_stake,
        total_stake,
        tao_stake,
        ..
    } = subnet_state;

    // Although it shouldn't happen, verify that all vectors are the same length just in case.
    let len = hotkeys.len();
    if [
        coldkeys.len(),
        emission.len(),
        alpha_stake.len(),
        total_stake.len(),
        tao_stake.len(),
    ]
    .iter()
    .any(|&l| l != len)
    {
        return Err(anyhow!("Mismatched vector lengths in SubnetState"));
    }

    let hashmap: HashMap<_, _> = izip!(
        hotkeys,
        coldkeys,
        emission,
        alpha_stake,
        total_stake,
        tao_stake
    )
    .map(
        |(hotkey, coldkey, emission, alpha_stake, total_stake, tao_stake)| {
            let data = HotkeyData {
                emission: emission.0,
                alpha_stake: alpha_stake.0,
                total_stake: total_stake.0,
                tao_stake: tao_stake.0,
                coldkey,
            };
            (hotkey, data)
        },
    )
    .collect();

    Ok(hashmap)
}

async fn get_subnet_state(
    bittensor_wss: &str,
    netuid: u16,
    block: Option<u64>,
    wss_max_message_size: usize,
) -> Result<HashMap<AccountId, HotkeyData>> {
    let ws_config = WebSocketConfig::default().max_message_size(Some(wss_max_message_size));

    let (mut socket, _) = tokio::time::timeout(
        Duration::from_secs(10),
        connect_async_with_config(bittensor_wss, Some(ws_config)),
    )
    .await??;

    let encoded_netuid = netuid.encode();
    let params_hex = format!("0x{}", hex::encode(encoded_netuid));

    let block_param = if let Some(b) = block {
        json!(b)
    } else {
        json!(null)
    };

    let request = json!({
        "jsonrpc": "2.0",
        "id": 1,
        "method": "state_call",
        "params": ["SubnetInfoRuntimeApi_get_subnet_state", params_hex, block_param]
    });

    socket.send(Message::text(request.to_string())).await?;
    let resp_text = get_text_message(&mut socket, 1).await?;
    let resp: Value = serde_json::from_str(&resp_text)?;

    if let Some(result_hex) = resp["result"].as_str() {
        let result_bytes = hex::decode(result_hex.trim_start_matches("0x"))?;
        let subnet_state = State::decode(&mut &result_bytes[..])
            .map_err(|e| anyhow::anyhow!("SCALE decoding failed: {:?}", e))?;

        build_hotkeys_state(subnet_state)
    } else if let Some(error) = resp.get("error") {
        Err(anyhow::anyhow!("Error in response: {:?}", error))
    } else {
        Err(anyhow::anyhow!(
            "No 'result' field in response; raw response: {}",
            resp_text
        ))
    }
}

#[cfg(test)]
mod tests {
    use crate::bittensor::{crypto::ss58_decode, subnet_state::SubnetState};
    use rustls::crypto::CryptoProvider;
    use std::time::Duration;

    #[tokio::test]
    #[ignore]
    async fn test_subnet_state_updates() {
        rustls::crypto::aws_lc_rs::default_provider()
            .install_default()
            .or_else(|_| {
                CryptoProvider::get_default()
                    .map(|_| ())
                    .ok_or_else(|| anyhow::anyhow!("Failed to locate any crypto provider"))
            })
            .unwrap();

        let poll_interval = Duration::from_millis(1000);
        let netuid = 17;
        let block = None;

        let subnet_state = SubnetState::new(
            "wss://entrypoint-finney.opentensor.ai:443".to_string(),
            netuid,
            block,
            poll_interval,
            2097152,
        );

        tokio::time::sleep(Duration::from_secs(2)).await;

        let current_state = subnet_state.current_state();
        let otf_hotkey = "5F4tQyWrhfGVcNhoqeiNsR6KjD4wMZ2kfhLj4oHYuyHbZAc3";
        let decoded_hotkey = ss58_decode(otf_hotkey).expect("Failed to decode SS58 hotkey");

        let hotkey_arr: &[u8; 32] = decoded_hotkey
            .0
            .as_slice()
            .try_into()
            .expect("Hotkey slice was not 32 bytes");

        assert!(
            current_state.get(hotkey_arr).is_some(),
            "Expected hotkey not found in state."
        );
    }
}
