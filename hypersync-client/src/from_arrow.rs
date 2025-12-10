use crate::{
    arrow_reader::{self, BlockReader, LogReader, TraceReader, TransactionReader},
    simple_types::{Block, Log, Trace, Transaction},
};
use anyhow::Context;
use arrayvec::ArrayVec;
use arrow::array::RecordBatch;

fn to_opt<T>(val: Result<T, arrow_reader::ReadError>) -> anyhow::Result<Option<T>> {
    match val {
        Ok(val) => Ok(Some(val)),
        // Only column not found error is valid for flattening to a None as if it
        // were null since the user can deselect a the column
        Err(arrow_reader::ReadError::ColumnError(arrow_reader::ColumnError {
            err: arrow_reader::ColumnErrorType::NotFound,
            ..
        })) => Ok(None),
        // All other errors are unexpected and should be surfaced
        Err(e) => Err(anyhow::Error::new(e)),
    }
}

fn to_nested_opt<T>(val: Result<Option<T>, arrow_reader::ReadError>) -> anyhow::Result<Option<T>> {
    match to_opt(val) {
        Ok(Some(Some(val))) => Ok(Some(val)),
        Ok(Some(None)) => Ok(None),
        Ok(None) => Ok(None),
        Err(e) => Err(e),
    }
}

impl TryFrom<LogReader<'_>> for Log {
    type Error = anyhow::Error;

    fn try_from(reader: LogReader<'_>) -> Result<Self, Self::Error> {
        let removed = to_nested_opt(reader.removed()).context("read field removed")?;
        let log_index = to_opt(reader.log_index()).context("read field log_index")?;
        let transaction_index =
            to_opt(reader.transaction_index()).context("read field transaction_index")?;
        let transaction_hash =
            to_opt(reader.transaction_hash()).context("read field transaction_hash")?;
        let block_hash = to_opt(reader.block_hash()).context("read field block_hash")?;
        let block_number = to_opt(reader.block_number()).context("read field block_number")?;
        let address = to_opt(reader.address()).context("read field address")?;
        let data = to_opt(reader.data()).context("read field data")?;
        let mut topics = ArrayVec::new();
        let topic0 = to_nested_opt(reader.topic0()).context("read field topic0")?;
        topics.push(topic0);
        let topic1 = to_nested_opt(reader.topic1()).context("read field topic1")?;
        topics.push(topic1);
        let topic2 = to_nested_opt(reader.topic2()).context("read field topic2")?;
        topics.push(topic2);
        let topic3 = to_nested_opt(reader.topic3()).context("read field topic3")?;
        topics.push(topic3);
        Ok(Self {
            removed,
            log_index,
            transaction_index,
            transaction_hash,
            block_hash,
            block_number,
            address,
            data,
            topics,
        })
    }
}

impl TryFrom<BlockReader<'_>> for Block {
    type Error = anyhow::Error;

    fn try_from(reader: BlockReader<'_>) -> Result<Self, Self::Error> {
        let number = to_opt(reader.number()).context("read field number")?;
        let hash = to_opt(reader.hash()).context("read field hash")?;
        let parent_hash = to_opt(reader.parent_hash()).context("read field parent_hash")?;
        let nonce = to_nested_opt(reader.nonce()).context("read field nonce")?;
        let sha3_uncles = to_opt(reader.sha3_uncles()).context("read field sha3_uncles")?;
        let logs_bloom = to_opt(reader.logs_bloom()).context("read field logs_bloom")?;
        let transactions_root =
            to_opt(reader.transactions_root()).context("read field transactions_root")?;
        let state_root = to_opt(reader.state_root()).context("read field state_root")?;
        let receipts_root = to_opt(reader.receipts_root()).context("read field receipts_root")?;
        let miner = to_opt(reader.miner()).context("read field miner")?;
        let difficulty = to_nested_opt(reader.difficulty()).context("read field difficulty")?;
        let total_difficulty =
            to_nested_opt(reader.total_difficulty()).context("read field total_difficulty")?;
        let extra_data = to_opt(reader.extra_data()).context("read field extra_data")?;
        let size = to_opt(reader.size()).context("read field size")?;
        let gas_limit = to_opt(reader.gas_limit()).context("read field gas_limit")?;
        let gas_used = to_opt(reader.gas_used()).context("read field gas_used")?;
        let timestamp = to_opt(reader.timestamp()).context("read field timestamp")?;
        let uncles = to_nested_opt(reader.uncles()).context("read field uncles")?;
        let base_fee_per_gas =
            to_nested_opt(reader.base_fee_per_gas()).context("read field base_fee_per_gas")?;
        let blob_gas_used =
            to_nested_opt(reader.blob_gas_used()).context("read field blob_gas_used")?;
        let excess_blob_gas =
            to_nested_opt(reader.excess_blob_gas()).context("read field excess_blob_gas")?;
        let parent_beacon_block_root = to_nested_opt(reader.parent_beacon_block_root())
            .context("read field parent_beacon_block_root")?;
        let withdrawals_root =
            to_nested_opt(reader.withdrawals_root()).context("read field withdrawals_root")?;
        let withdrawals = to_nested_opt(reader.withdrawals()).context("read field withdrawals")?;
        let l1_block_number =
            to_nested_opt(reader.l1_block_number()).context("read field l1_block_number")?;
        let send_count = to_nested_opt(reader.send_count()).context("read field send_count")?;
        let send_root = to_nested_opt(reader.send_root()).context("read field send_root")?;
        let mix_hash = to_nested_opt(reader.mix_hash()).context("read field mix_hash")?;

        Ok(Self {
            number,
            hash,
            parent_hash,
            nonce,
            sha3_uncles,
            logs_bloom,
            transactions_root,
            state_root,
            receipts_root,
            miner,
            difficulty,
            total_difficulty,
            extra_data,
            size,
            gas_limit,
            gas_used,
            timestamp,
            uncles,
            base_fee_per_gas,
            blob_gas_used,
            excess_blob_gas,
            parent_beacon_block_root,
            withdrawals_root,
            withdrawals,
            l1_block_number,
            send_count,
            send_root,
            mix_hash,
        })
    }
}

impl TryFrom<TransactionReader<'_>> for Transaction {
    type Error = anyhow::Error;

    fn try_from(reader: TransactionReader<'_>) -> Result<Self, Self::Error> {
        let block_hash = to_opt(reader.block_hash()).context("read field block_hash")?;
        let block_number = to_opt(reader.block_number()).context("read field block_number")?;
        let from = to_nested_opt(reader.from()).context("read field from")?;
        let gas = to_opt(reader.gas()).context("read field gas")?;
        let gas_price = to_nested_opt(reader.gas_price()).context("read field gas_price")?;
        let hash = to_opt(reader.hash()).context("read field hash")?;
        let input = to_opt(reader.input()).context("read field input")?;
        let nonce = to_opt(reader.nonce()).context("read field nonce")?;
        let to = to_nested_opt(reader.to()).context("read field to")?;
        let transaction_index =
            to_opt(reader.transaction_index()).context("read field transaction_index")?;
        let value = to_opt(reader.value()).context("read field value")?;
        let v = to_nested_opt(reader.v()).context("read field v")?;
        let r = to_nested_opt(reader.r()).context("read field r")?;
        let s = to_nested_opt(reader.s()).context("read field s")?;
        let y_parity = to_nested_opt(reader.y_parity()).context("read field y_parity")?;
        let max_priority_fee_per_gas = to_nested_opt(reader.max_priority_fee_per_gas())
            .context("read field max_priority_fee_per_gas")?;
        let max_fee_per_gas =
            to_nested_opt(reader.max_fee_per_gas()).context("read field max_fee_per_gas")?;
        let chain_id = to_nested_opt(reader.chain_id()).context("read field chain_id")?;
        let access_list = to_nested_opt(reader.access_list()).context("read field access_list")?;
        let authorization_list =
            to_nested_opt(reader.authorization_list()).context("read field authorization_list")?;
        let max_fee_per_blob_gas = to_nested_opt(reader.max_fee_per_blob_gas())
            .context("read field max_fee_per_blob_gas")?;
        let blob_versioned_hashes = to_nested_opt(reader.blob_versioned_hashes())
            .context("read field blob_versioned_hashes")?;
        let cumulative_gas_used =
            to_opt(reader.cumulative_gas_used()).context("read field cumulative_gas_used")?;
        let effective_gas_price =
            to_opt(reader.effective_gas_price()).context("read field effective_gas_price")?;
        let gas_used = to_opt(reader.gas_used()).context("read field gas_used")?;
        let contract_address =
            to_nested_opt(reader.contract_address()).context("read field contract_address")?;
        let logs_bloom = to_opt(reader.logs_bloom()).context("read field logs_bloom")?;
        let type_ = to_nested_opt(reader.type_()).context("read field type_")?;
        let root = to_nested_opt(reader.root()).context("read field root")?;
        let status = to_nested_opt(reader.status()).context("read field status")?;
        let l1_fee = to_nested_opt(reader.l1_fee()).context("read field l1_fee")?;
        let l1_gas_price =
            to_nested_opt(reader.l1_gas_price()).context("read field l1_gas_price")?;
        let l1_gas_used = to_nested_opt(reader.l1_gas_used()).context("read field l1_gas_used")?;
        let l1_fee_scalar =
            to_nested_opt(reader.l1_fee_scalar()).context("read field l1_fee_scalar")?;
        let gas_used_for_l1 =
            to_nested_opt(reader.gas_used_for_l1()).context("read field gas_used_for_l1")?;
        let blob_gas_price =
            to_nested_opt(reader.blob_gas_price()).context("read field blob_gas_price")?;
        let blob_gas_used =
            to_nested_opt(reader.blob_gas_used()).context("read field blob_gas_used")?;
        let deposit_nonce =
            to_nested_opt(reader.deposit_nonce()).context("read field deposit_nonce")?;
        let deposit_receipt_version = to_nested_opt(reader.deposit_receipt_version())
            .context("read field deposit_receipt_version")?;
        let l1_base_fee_scalar =
            to_nested_opt(reader.l1_base_fee_scalar()).context("read field l1_base_fee_scalar")?;
        let l1_blob_base_fee =
            to_nested_opt(reader.l1_blob_base_fee()).context("read field l1_blob_base_fee")?;
        let l1_blob_base_fee_scalar = to_nested_opt(reader.l1_blob_base_fee_scalar())
            .context("read field l1_blob_base_fee_scalar")?;
        let l1_block_number =
            to_nested_opt(reader.l1_block_number()).context("read field l1_block_number")?;
        let mint = to_nested_opt(reader.mint()).context("read field mint")?;
        let sighash = to_nested_opt(reader.sighash()).context("read field sighash")?;
        let source_hash = to_nested_opt(reader.source_hash()).context("read field source_hash")?;

        Ok(Self {
            block_hash,
            block_number,
            from,
            gas,
            gas_price,
            hash,
            input,
            nonce,
            to,
            transaction_index,
            value,
            v,
            r,
            s,
            y_parity,
            max_priority_fee_per_gas,
            max_fee_per_gas,
            chain_id,
            access_list,
            authorization_list,
            max_fee_per_blob_gas,
            blob_versioned_hashes,
            cumulative_gas_used,
            effective_gas_price,
            gas_used,
            contract_address,
            logs_bloom,
            type_,
            root,
            status,
            l1_fee,
            l1_gas_price,
            l1_gas_used,
            l1_fee_scalar,
            gas_used_for_l1,
            blob_gas_price,
            blob_gas_used,
            deposit_nonce,
            deposit_receipt_version,
            l1_base_fee_scalar,
            l1_blob_base_fee,
            l1_blob_base_fee_scalar,
            l1_block_number,
            mint,
            sighash,
            source_hash,
        })
    }
}

impl Block {
    /// Convert an arrow RecordBatch into a vector of Block structs
    pub fn from_arrow(batch: &RecordBatch) -> anyhow::Result<Vec<Self>> {
        let mut blocks = Vec::new();
        for block_reader in BlockReader::iter(batch) {
            blocks.push(
                block_reader
                    .try_into()
                    .context("convert block reader to block")?,
            );
        }
        Ok(blocks)
    }
}

impl Transaction {
    /// Convert an arrow RecordBatch into a vector of Transaction structs
    pub fn from_arrow(batch: &RecordBatch) -> anyhow::Result<Vec<Self>> {
        let mut transactions = Vec::new();
        for transaction_reader in TransactionReader::iter(batch) {
            transactions.push(
                transaction_reader
                    .try_into()
                    .context("convert transaction reader to transaction")?,
            );
        }
        Ok(transactions)
    }
}

impl TryFrom<TraceReader<'_>> for Trace {
    type Error = anyhow::Error;

    fn try_from(reader: TraceReader<'_>) -> Result<Self, Self::Error> {
        let from = to_nested_opt(reader.from()).context("read field from")?;
        let to = to_nested_opt(reader.to()).context("read field to")?;
        let call_type = to_nested_opt(reader.call_type()).context("read field call_type")?;
        let gas = to_nested_opt(reader.gas()).context("read field gas")?;
        let input = to_nested_opt(reader.input()).context("read field input")?;
        let init = to_nested_opt(reader.init()).context("read field init")?;
        let value = to_nested_opt(reader.value()).context("read field value")?;
        let author = to_nested_opt(reader.author()).context("read field author")?;
        let reward_type = to_nested_opt(reader.reward_type()).context("read field reward_type")?;
        let block_hash = to_opt(reader.block_hash()).context("read field block_hash")?;
        let block_number = to_opt(reader.block_number()).context("read field block_number")?;
        let address = to_nested_opt(reader.address()).context("read field address")?;
        let code = to_nested_opt(reader.code()).context("read field code")?;
        let gas_used = to_nested_opt(reader.gas_used()).context("read field gas_used")?;
        let output = to_nested_opt(reader.output()).context("read field output")?;
        let subtraces = to_nested_opt(reader.subtraces()).context("read field subtraces")?;
        let trace_address =
            to_nested_opt(reader.trace_address()).context("read field trace_address")?;
        let transaction_hash =
            to_nested_opt(reader.transaction_hash()).context("read field transaction_hash")?;
        let transaction_position = to_nested_opt(reader.transaction_position())
            .context("read field transaction_position")?;
        let type_ = to_nested_opt(reader.type_()).context("read field type_")?;
        let error = to_nested_opt(reader.error()).context("read field error")?;
        let sighash = to_nested_opt(reader.sighash()).context("read field sighash")?;
        let action_address =
            to_nested_opt(reader.action_address()).context("read field action_address")?;
        let balance = to_nested_opt(reader.balance()).context("read field balance")?;
        let refund_address =
            to_nested_opt(reader.refund_address()).context("read field refund_address")?;

        Ok(Self {
            from,
            to,
            call_type,
            gas,
            input,
            init,
            value,
            author,
            reward_type,
            block_hash,
            block_number,
            address,
            code,
            gas_used,
            output,
            subtraces,
            trace_address,
            transaction_hash,
            transaction_position,
            type_,
            error,
            sighash,
            action_address,
            balance,
            refund_address,
        })
    }
}

impl Log {
    /// Convert an arrow RecordBatch into a vector of Log structs
    pub fn from_arrow(batch: &RecordBatch) -> anyhow::Result<Vec<Self>> {
        let mut logs = Vec::new();
        for log_reader in LogReader::iter(batch) {
            logs.push(log_reader.try_into().context("convert log reader to log")?);
        }
        Ok(logs)
    }
}

impl Trace {
    /// Convert an arrow RecordBatch into a vector of Trace structs
    pub fn from_arrow(batch: &RecordBatch) -> anyhow::Result<Vec<Self>> {
        let mut traces = Vec::new();
        for trace_reader in TraceReader::iter(batch) {
            traces.push(
                trace_reader
                    .try_into()
                    .context("convert trace reader to trace")?,
            );
        }
        Ok(traces)
    }
}
