//! RPC extension that overrides eth_callMany to return execution logs.
//!
//! This crate overrides the standard `eth_callMany` to return execution logs
//! in addition to the standard response fields.
//!
//! Supports transactionIndex for mid-block state simulation.

use alloy_consensus::BlockHeader;
use alloy_eips::BlockId;
use alloy_primitives::{Bytes, U64};
use alloy_rpc_types::{state::StateOverride, BlockOverrides};
use alloy_rpc_types_eth::{state::EvmOverrides, Log};
use jsonrpsee::{
    core::{async_trait, RpcResult},
    proc_macros::rpc,
};
use op_alloy_network::Optimism;
use op_alloy_rpc_types::OpTransactionRequest;
use reth_primitives_traits::BlockBody as _;
use reth_rpc_eth_api::helpers::FullEthApi;
use reth_rpc_eth_types::EthApiError;
use reth_revm::database::StateProviderDatabase;
use revm::{context_interface::result::ExecutionResult, DatabaseCommit};
use revm_database::State;
use serde::{Deserialize, Serialize};
use tracing::debug;

/// Bundle of transactions for eth_callMany
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Bundle {
    /// Transactions in this bundle
    pub transactions: Vec<OpTransactionRequest>,
    /// Optional block overrides for this bundle
    #[serde(default, rename = "blockOverride")]
    pub block_override: Option<BlockOverrides>,
}

/// Transaction index specification for state context
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(untagged)]
pub enum TransactionIndex {
    /// Index as integer
    Index(u64),
    /// Index as hex string
    HexIndex(U64),
    /// No index specified
    #[default]
    None,
}

impl TransactionIndex {
    /// Get the index value if specified
    pub fn index(&self) -> Option<usize> {
        match self {
            Self::Index(i) => Some(*i as usize),
            Self::HexIndex(i) => Some(i.to::<u64>() as usize),
            Self::None => None,
        }
    }
}

/// State context for call_many - specifies block and transaction index
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct StateContext {
    /// Block number or tag
    #[serde(default, rename = "blockNumber")]
    pub block_number: Option<BlockId>,
    /// Transaction index within the block (for mid-block state)
    #[serde(default, rename = "transactionIndex")]
    pub transaction_index: Option<TransactionIndex>,
}

/// Extended response from eth_callMany that includes logs.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EthCallResponseWithLogs {
    /// Return value from the call (hex encoded)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub value: Option<Bytes>,
    /// Error message if call failed
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
    /// Logs emitted during execution
    #[serde(skip_serializing_if = "Option::is_none")]
    pub logs: Option<Vec<Log>>,
    /// Gas used by the call
    #[serde(skip_serializing_if = "Option::is_none", rename = "gasUsed")]
    pub gas_used: Option<String>,
}

/// RPC trait that overrides eth_callMany to include logs
#[cfg_attr(not(test), rpc(server, namespace = "eth"))]
#[cfg_attr(test, rpc(server, client, namespace = "eth"))]
pub trait CallWithLogsApi {
    /// Execute multiple transaction bundles and return results with logs.
    ///
    /// This OVERRIDES the standard eth_callMany to include execution logs.
    /// Supports transactionIndex for mid-block state simulation.
    #[method(name = "callMany")]
    async fn call_many(
        &self,
        bundles: Vec<Bundle>,
        state_context: Option<StateContext>,
        state_override: Option<StateOverride>,
    ) -> RpcResult<Vec<Vec<EthCallResponseWithLogs>>>;
}

/// Implementation of CallWithLogsApi
#[derive(Debug, Clone)]
pub struct CallWithLogsApiImpl<Eth> {
    eth_api: Eth,
}

impl<Eth> CallWithLogsApiImpl<Eth> {
    /// Create a new instance
    pub fn new(eth_api: Eth) -> Self {
        Self { eth_api }
    }
}

#[async_trait]
impl<Eth> CallWithLogsApiServer for CallWithLogsApiImpl<Eth>
where
    Eth: FullEthApi<NetworkTypes = Optimism> + Send + Sync + 'static,
    jsonrpsee::types::error::ErrorObject<'static>: From<Eth::Error>,
{
    async fn call_many(
        &self,
        bundles: Vec<Bundle>,
        state_context: Option<StateContext>,
        mut state_override: Option<StateOverride>,
    ) -> RpcResult<Vec<Vec<EthCallResponseWithLogs>>> {
        use reth_evm::ConfigureEvm;
        use reth_storage_api::BlockIdReader;

        if bundles.is_empty() {
            return Err(EthApiError::InvalidParams(String::from("bundles are empty.")).into());
        }

        let StateContext { transaction_index, block_number } =
            state_context.unwrap_or_default();
        let transaction_index = transaction_index.unwrap_or_default();

        let mut target_block = block_number.unwrap_or_default();
        let is_block_target_pending = target_block.is_pending();

        debug!(
            message = "rpc::call_many (with logs)",
            bundles = bundles.len(),
            ?target_block,
            ?transaction_index,
        );

        // If not pending, use block_hash for consistency
        if !is_block_target_pending {
            target_block = self
                .eth_api
                .provider()
                .block_hash_for_id(target_block)
                .map_err(|_| EthApiError::HeaderNotFound(target_block))?
                .ok_or_else(|| EthApiError::HeaderNotFound(target_block))?
                .into();
        }

        let ((evm_env, _), block) = futures::try_join!(
            self.eth_api.evm_env_at(target_block),
            self.eth_api.recovered_block(target_block)
        )?;

        let block = block.ok_or(EthApiError::HeaderNotFound(target_block))?;

        // Determine if we need to replay transactions
        let mut at = block.parent_hash();
        let mut replay_block_txs = true;

        let num_txs = transaction_index
            .index()
            .unwrap_or_else(|| block.body().transactions().len());

        // If all transactions are to be replayed, we can use block state directly
        // (unless targeting pending block)
        if !is_block_target_pending && num_txs == block.body().transactions().len() {
            at = block.hash();
            replay_block_txs = false;
        }

        let eth_api = self.eth_api.clone();
        self.eth_api
            .spawn_with_state_at_block(at.into(), move |state| {
                let mut all_results = Vec::with_capacity(bundles.len());
                let mut db = State::builder()
                    .with_database(StateProviderDatabase::new(state))
                    .build();

                // Replay block transactions up to transaction_index if needed
                if replay_block_txs {
                    let block_transactions = block.transactions_recovered().take(num_txs);
                    for tx in block_transactions {
                        let tx_env = eth_api.evm_config().tx_env(tx);
                        let res = eth_api.transact(&mut db, evm_env.clone(), tx_env)?;
                        db.commit(res.state);
                    }
                }

                // Execute all bundles and capture logs
                for bundle in bundles.into_iter() {
                    let Bundle { transactions, block_override } = bundle;
                    if transactions.is_empty() {
                        continue;
                    }

                    let mut bundle_results = Vec::with_capacity(transactions.len());
                    let block_overrides = block_override.map(Box::new);

                    for tx in transactions.into_iter() {
                        // Prepare call environment with overrides
                        let overrides =
                            EvmOverrides::new(state_override.take(), block_overrides.clone());

                        let (current_evm_env, prepared_tx) =
                            eth_api.prepare_call_env(evm_env.clone(), tx, &mut db, overrides)?;

                        let res = eth_api.transact(&mut db, current_evm_env, prepared_tx)?;

                        // Extract logs from execution result
                        let response = match res.result {
                            ExecutionResult::Success { output, gas_used, logs, .. } => {
                                let output_bytes = match output {
                                    revm::context_interface::result::Output::Call(bytes) => bytes,
                                    revm::context_interface::result::Output::Create(bytes, _) => {
                                        bytes
                                    }
                                };
                                EthCallResponseWithLogs {
                                    value: Some(output_bytes),
                                    error: None,
                                    logs: Some(
                                        logs.into_iter()
                                            .enumerate()
                                            .map(|(idx, log)| Log {
                                                inner: alloy_primitives::Log {
                                                    address: log.address,
                                                    data: alloy_primitives::LogData::new(
                                                        log.topics().to_vec(),
                                                        log.data.data.clone(),
                                                    )
                                                    .unwrap_or_default(),
                                                },
                                                block_hash: None,
                                                block_number: None,
                                                block_timestamp: None,
                                                transaction_hash: None,
                                                transaction_index: None,
                                                log_index: Some(idx as u64),
                                                removed: false,
                                            })
                                            .collect(),
                                    ),
                                    gas_used: Some(format!("0x{:x}", gas_used)),
                                }
                            }
                            ExecutionResult::Revert { output, gas_used } => {
                                EthCallResponseWithLogs {
                                    value: None,
                                    error: Some(format!(
                                        "execution reverted: 0x{}",
                                        hex::encode(&output)
                                    )),
                                    logs: Some(Vec::new()),
                                    gas_used: Some(format!("0x{:x}", gas_used)),
                                }
                            }
                            ExecutionResult::Halt { reason, gas_used } => EthCallResponseWithLogs {
                                value: None,
                                error: Some(format!("execution halted: {:?}", reason)),
                                logs: Some(Vec::new()),
                                gas_used: Some(format!("0x{:x}", gas_used)),
                            },
                        };

                        bundle_results.push(response);

                        // Commit state changes for subsequent calls
                        db.commit(res.state);
                    }

                    all_results.push(bundle_results);
                }

                Ok(all_results)
            })
            .await
            .map_err(Into::into)
    }
}
