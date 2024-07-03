//! Preset queries for common use cases.
use std::collections::BTreeSet;

use arrayvec::ArrayVec;
use hypersync_format::{Address, LogArgument};
use hypersync_net_types::{FieldSelection, LogSelection, Query, TransactionSelection};

/// Returns a query for all Blocks and Transactions within the block range (from_block, to_block]
/// If to_block is None then query runs to the head of the chain.
/// Note: this is only for quickstart purposes.  For the best performance, create a custom query
/// that only includes the fields you'll use in `field_selection`.
pub fn blocks_and_transactions(from_block: u64, to_block: Option<u64>) -> Query {
    let all_block_fields: BTreeSet<String> = hypersync_schema::block_header()
        .fields
        .iter()
        .map(|x| x.name.clone())
        .collect();

    let all_tx_fields: BTreeSet<String> = hypersync_schema::transaction()
        .fields
        .iter()
        .map(|x| x.name.clone())
        .collect();

    Query {
        from_block,
        to_block,
        include_all_blocks: true,
        transactions: vec![TransactionSelection::default()],
        field_selection: FieldSelection {
            block: all_block_fields,
            transaction: all_tx_fields,
            ..Default::default()
        },
        ..Default::default()
    }
}

/// Returns a query object for all Blocks and hashes of the Transactions within the block range
/// (from_block, to_block].  Also returns the block_hash and block_number fields on each Transaction
/// so it can be mapped to a block.  If to_block is None then query runs to the head of the chain.
/// Note: this is only for quickstart purposes.  For the best performance, create a custom query
/// that only includes the fields you'll use in `field_selection`.
pub fn blocks_and_transaction_hashes(from_block: u64, to_block: Option<u64>) -> Query {
    let mut tx_field_selection = BTreeSet::new();
    tx_field_selection.insert("block_hash".to_owned());
    tx_field_selection.insert("block_number".to_owned());
    tx_field_selection.insert("hash".to_owned());

    let all_block_fields: BTreeSet<String> = hypersync_schema::block_header()
        .fields
        .iter()
        .map(|x| x.name.clone())
        .collect();

    Query {
        from_block,
        to_block,
        include_all_blocks: true,
        transactions: vec![TransactionSelection::default()],
        field_selection: FieldSelection {
            block: all_block_fields,
            transaction: tx_field_selection,
            ..Default::default()
        },
        ..Default::default()
    }
}

/// Returns a query object for all Logs within the block range (from_block, to_block] from
/// the given address.  If to_block is None then query runs to the head of the chain.
/// Note: this is only for quickstart purposes.  For the best performance, create a custom query
/// that only includes the fields you'll use in `field_selection`.
pub fn logs(from_block: u64, to_block: Option<u64>, contract_address: Address) -> Query {
    let all_log_fields: BTreeSet<String> = hypersync_schema::log()
        .fields
        .iter()
        .map(|x| x.name.clone())
        .collect();

    Query {
        from_block,
        to_block,
        logs: vec![LogSelection {
            address: vec![contract_address],
            ..Default::default()
        }],
        field_selection: FieldSelection {
            log: all_log_fields,
            ..Default::default()
        },
        ..Default::default()
    }
}

/// Returns a query for all Logs within the block range (from_block, to_block] from the
/// given address with a matching topic0 event signature.  Topic0 is the keccak256 hash
/// of the event signature.  If to_block is None then query runs to the head of the chain.
/// Note: this is only for quickstart purposes.  For the best performance, create a custom query
/// that only includes the fields you'll use in `field_selection`.
pub fn logs_of_event(
    from_block: u64,
    to_block: Option<u64>,
    topic0: LogArgument,
    contract_address: Address,
) -> Query {
    let mut topics = ArrayVec::<Vec<LogArgument>, 4>::new();
    topics.insert(0, vec![topic0]);

    let all_log_fields: BTreeSet<String> = hypersync_schema::log()
        .fields
        .iter()
        .map(|x| x.name.clone())
        .collect();

    Query {
        from_block,
        to_block,
        logs: vec![LogSelection {
            address: vec![contract_address],
            topics,
            ..Default::default()
        }],
        field_selection: FieldSelection {
            log: all_log_fields,
            ..Default::default()
        },
        ..Default::default()
    }
}

/// Returns a query object for all transactions within the block range (from_block, to_block].
/// If to_block is None then query runs to the head of the chain.
/// Note: this is only for quickstart purposes.  For the best performance, create a custom query
/// that only includes the fields you'll use in `field_selection`.
pub fn transactions(from_block: u64, to_block: Option<u64>) -> Query {
    let all_txn_fields: BTreeSet<String> = hypersync_schema::transaction()
        .fields
        .iter()
        .map(|x| x.name.clone())
        .collect();

    Query {
        from_block,
        to_block,
        transactions: vec![TransactionSelection::default()],
        field_selection: FieldSelection {
            transaction: all_txn_fields,
            ..Default::default()
        },
        ..Default::default()
    }
}

/// Returns a query object for all transactions from an address within the block range
/// (from_block, to_block].  If to_block is None then query runs to the head of the chain.
/// Note: this is only for quickstart purposes.  For the best performance, create a custom query
/// that only includes the fields you'll use in `field_selection`.
pub fn transactions_from_address(
    from_block: u64,
    to_block: Option<u64>,
    address: Address,
) -> Query {
    let all_txn_fields: BTreeSet<String> = hypersync_schema::transaction()
        .fields
        .iter()
        .map(|x| x.name.clone())
        .collect();

    Query {
        from_block,
        to_block,
        transactions: vec![TransactionSelection {
            from: vec![address],
            ..Default::default()
        }],
        field_selection: FieldSelection {
            transaction: all_txn_fields,
            ..Default::default()
        },
        ..Default::default()
    }
}
