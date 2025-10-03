use anyhow::{anyhow, Result};
use async_tungstenite::tokio::connect_async_with_config;
use async_tungstenite::tungstenite::protocol::WebSocketConfig;
use async_tungstenite::tungstenite::Message;
use foldhash::fast::RandomState;
use futures_util::StreamExt;
use hex;
use itertools::izip;
use parity_scale_codec::{Compact, Decode, Encode};
use scc::HashMap;
use serde_json::{json, Value};
use std::fmt;
use std::sync::Arc;
use std::time::Duration;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tracing::{error, info};

use super::crypto::{ss58_decode, AccountId32};
use super::hotkey::Hotkey;

const WSS_TIMEOUT_SECS: u64 = 10;
const MIN_REQUIRED_STAKE: u64 = 10_000 * 1_000_000_000;

#[derive(Debug, Clone, PartialEq)]
pub struct HotkeyData {
    pub emission: u64,
    pub alpha_stake: u64,
    pub total_stake: u64,
    pub tao_stake: u64,
    pub coldkey: AccountId32,
}

#[derive(Decode, Encode, PartialEq, Eq, Clone, Debug)]
pub struct State {
    pub netuid: Compact<u16>,
    pub hotkeys: Vec<AccountId32>,
    pub coldkeys: Vec<AccountId32>,
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

struct Inner {
    map: HashMap<AccountId32, HotkeyData, RandomState>,
    join_handle: JoinHandle<()>,
}

#[derive(Clone)]
pub struct SubnetState {
    inner: Arc<Inner>,
}

impl SubnetState {
    pub fn new(
        wss_bittensor: String,
        netuid: u16,
        block: Option<u64>,
        poll_interval: Duration,
        wss_max_message_size: usize,
        shutdown: CancellationToken,
    ) -> Self {
        let inner = Arc::new_cyclic(|weak: &std::sync::Weak<Inner>| {
            let map = HashMap::with_capacity_and_hasher(256, RandomState::default());
            let join_handle = tokio::spawn({
                let weak = weak.clone();
                let wss_bittensor = wss_bittensor.clone();
                let shutdown = shutdown.child_token();
                async move {
                    while let Some(inner) = weak.upgrade() {
                        info!("Updating subnet state {} via WSS {}", netuid, wss_bittensor);
                        tokio::select! {
                            _ = shutdown.cancelled() => break,
                            _ = async {
                                match get_subnet_state(&wss_bittensor, netuid, block, wss_max_message_size)
                                    .await
                                {
                                    Ok(new_state) => {
                                        let map = &inner.map;
                                        map.retain_async(|k, _| new_state.contains_key(k)).await;
                                        for (hotkey, data) in new_state {
                                            map.entry_async(hotkey)
                                                .await
                                                .and_modify(|v| {
                                                    if *v != data {
                                                        *v = data.clone()
                                                    }
                                                })
                                                .or_insert(data);
                                        }
                                    }
                                    Err(e) => error!("Error updating subnet state: {:?}", e),
                                }
                                tokio::time::sleep(poll_interval).await;
                            } => {}
                        }
                    }
                }
            });
            Inner { map, join_handle }
        });
        SubnetState { inner }
    }

    // Check that the hotkey exists and has at least 10k TAO
    pub async fn validate_hotkey(&self, hotkey: &Hotkey) -> Result<()> {
        let account_id = ss58_decode(hotkey)?;
        if let Some(hk) = self.inner.map.get_async(&account_id).await {
            let total_stake = hk.total_stake;
            if total_stake > MIN_REQUIRED_STAKE {
                Ok(())
            } else {
                Err(anyhow!(
                    "Hotkey found but insufficient stake: {} < {}",
                    total_stake,
                    MIN_REQUIRED_STAKE
                ))
            }
        } else {
            Err(anyhow!("Hotkey not found in the subnet"))
        }
    }

    #[allow(dead_code)]
    pub async fn get(&self, hotkey: &AccountId32) -> Option<HotkeyData> {
        self.inner
            .map
            .get_async(hotkey)
            .await
            .map(|entry| entry.get().clone())
    }

    pub fn abort(&self) {
        self.inner.join_handle.abort();
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

fn build_hotkeys_state(subnet_state: State) -> Result<foldhash::HashMap<AccountId32, HotkeyData>> {
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

    let hashmap: foldhash::HashMap<_, _> = izip!(
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
) -> Result<foldhash::HashMap<AccountId32, HotkeyData>> {
    let ws_config = WebSocketConfig::default().max_message_size(Some(wss_max_message_size));
    let (mut socket, _) = tokio::time::timeout(
        Duration::from_secs(WSS_TIMEOUT_SECS),
        connect_async_with_config(bittensor_wss, Some(ws_config)),
    )
    .await??;
    let encoded_netuid = netuid.encode();
    let params_hex = format!("0x{}", hex::encode(encoded_netuid));
    let block_param = block.map_or(json!(null), |b| json!(b));
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
            .map_err(|e| anyhow!("SCALE decoding failed: {:?}", e))?;
        Ok(build_hotkeys_state(subnet_state)?)
    } else if let Some(error) = resp.get("error") {
        Err(anyhow!("Error in response: {:?}", error))
    } else {
        Err(anyhow!(
            "No 'result' field in response; raw response: {}",
            resp_text
        ))
    }
}

// Useful for debugging
#[allow(dead_code)]
pub struct Balance {
    pub rao: u64,
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

#[cfg(test)]
mod tests {
    use crate::bittensor::{crypto::ss58_decode, subnet_state::SubnetState};
    use rustls::crypto::CryptoProvider;
    use std::time::Duration;
    use tokio_util::sync::CancellationToken;

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
            CancellationToken::new(),
        );

        tokio::time::sleep(Duration::from_secs(2)).await;

        let otf_hotkey = "5GTmkzxbXSFh8ApLU24fzWUu2asZs89V5eJnN3ufubTg9Pj7";
        let decoded_hotkey = ss58_decode(otf_hotkey).expect("Failed to decode SS58 hotkey");

        assert!(
            subnet_state.get(&decoded_hotkey).await.is_some(),
            "Expected hotkey not found in state."
        );
    }
}
