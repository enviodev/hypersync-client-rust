//! # HyperSync Format
//!
//! Core data types and serialization formats for the HyperSync protocol.
//!
//! This crate provides the fundamental data structures used to represent blockchain
//! data in a standardized format, including blocks, transactions, logs, and traces.
//!
//! ## Features
//!
//! - **Standard blockchain types**: Comprehensive data structures for Ethereum-compatible chains
//! - **Efficient serialization**: Optimized JSON and binary serialization
//! - **Type safety**: Strong typing for addresses, hashes, and numeric values
//! - **Hex encoding**: Automatic handling of hexadecimal encoding/decoding
//!
//! ## Key Types
//!
//! - [`Block`] - Complete block data including header and body
//! - [`Transaction`] - Transaction data with all fields
//! - [`Log`] - Event log from contract execution  
//! - [`Trace`] - Execution trace information
//! - [`Address`] - 20-byte Ethereum address
//! - [`Hash`] - 32-byte cryptographic hash
//! - [`Hex`] - Wrapper for hexadecimal data
//!
//! ## Example
//!
//! ```
//! use hypersync_format::{Address, Hash};
//!
//! // Parse an Ethereum address  
//! let addr: Address = "0x742d35Cc6634C0532925a3b8D400ACDCD5C94C33".parse()?;
//! println!("Address: {}", addr);
//!
//! // Create a hash from hex string  
//! let hash: Hash = "0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef".parse()?;
//! println!("Hash: {}", hash);
//! # Ok::<(), Box<dyn std::error::Error>>(())
//! ```

mod error;
mod types;

pub use error::{Error, Result};
pub use types::{
    uint::UInt, AccessList, Address, Authorization, Block, BlockHeader, BlockNumber, BloomFilter,
    Data, DebugBlockTrace, DebugTxTrace, FilterWrapper, FixedSizeData, Hash, Hex, Log, LogArgument,
    LogIndex, Nonce, Quantity, Trace, TraceAction, TraceResult, Transaction, TransactionIndex,
    TransactionReceipt, TransactionStatus, TransactionType, Withdrawal,
};
