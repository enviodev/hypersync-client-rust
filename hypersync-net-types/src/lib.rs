use std::collections::BTreeSet;

use arrayvec::ArrayVec;
use hypersync_format::{Address, FilterWrapper, FixedSizeData, Hash, LogArgument};
use schemars::{
    gen::SchemaGenerator,
    schema::{ArrayValidation, InstanceType, Schema, SchemaObject, StringValidation},
    JsonSchema,
};
use serde::{Deserialize, Serialize};

pub type Sighash = FixedSizeData<4>;

pub mod hypersync_net_types_capnp {
    include!(concat!(env!("OUT_DIR"), "/hypersync_net_types_capnp.rs"));
}

/// Wrapper for `Hash` to implement `JsonSchema`
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub struct HashWrapper(pub Hash);

impl JsonSchema for HashWrapper {
    fn schema_name() -> String {
        "Hash".to_string()
    }

    fn json_schema(_gen: &mut SchemaGenerator) -> Schema {
        Schema::Object(SchemaObject {
            instance_type: Some(InstanceType::String.into()),
            string: Some(Box::new(StringValidation {
                pattern: Some("^[0-9a-fA-F]{64}$".to_string()),
                ..Default::default()
            })),
            metadata: Some(Box::new(schemars::schema::Metadata {
                description: Some(
                    "A 32-byte hash represented as a 64-character hexadecimal string.".to_string(),
                ),
                ..Default::default()
            })),
            ..Default::default()
        })
    }
}

/// Wrapper for `Address` to implement `JsonSchema`
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub struct AddressWrapper(pub Address);

impl JsonSchema for AddressWrapper {
    fn schema_name() -> String {
        "Address".to_string()
    }

    fn json_schema(_gen: &mut SchemaGenerator) -> Schema {
        Schema::Object(SchemaObject {
            instance_type: Some(InstanceType::String.into()),
            string: Some(Box::new(StringValidation {
                pattern: Some("^(0x)?[0-9a-fA-F]{40}$".to_string()),
                ..Default::default()
            })),
            metadata: Some(Box::new(schemars::schema::Metadata {
                description: Some(
                    "An Ethereum address represented as a 40-character hexadecimal string."
                        .to_string(),
                ),
                ..Default::default()
            })),
            ..Default::default()
        })
    }
}

/// Wrapper for `Sighash` to implement `JsonSchema`
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub struct SighashWrapper(pub Sighash);

impl JsonSchema for SighashWrapper {
    fn schema_name() -> String {
        "Sighash".to_string()
    }

    fn json_schema(_gen: &mut SchemaGenerator) -> Schema {
        Schema::Object(SchemaObject {
            instance_type: Some(InstanceType::String.into()),
            string: Some(Box::new(StringValidation {
                pattern: Some("^[0-9a-fA-F]{8}$".to_string()),
                ..Default::default()
            })),
            metadata: Some(Box::new(schemars::schema::Metadata {
                description: Some(
                    "A 4-byte sighash represented as an 8-character hexadecimal string."
                        .to_string(),
                ),
                ..Default::default()
            })),
            ..Default::default()
        })
    }
}

/// Wrapper for `FilterWrapper` to implement `JsonSchema`
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub struct FilterWrapperSchema(pub FilterWrapper);

impl JsonSchema for FilterWrapperSchema {
    fn schema_name() -> String {
        "FilterWrapper".to_string()
    }

    fn json_schema(_gen: &mut SchemaGenerator) -> Schema {
        // Define the schema as needed
        Schema::Object(SchemaObject {
            instance_type: Some(InstanceType::String.into()),
            metadata: Some(Box::new(schemars::schema::Metadata {
                description: Some("A filter wrapper represented as a string.".to_string()),
                ..Default::default()
            })),
            ..Default::default()
        })
    }
}

/// Wrapper for `LogArgument` to implement `JsonSchema`
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub struct LogArgumentSchema(pub LogArgument);

impl JsonSchema for LogArgumentSchema {
    fn schema_name() -> String {
        "LogArgument".to_string()
    }

    fn json_schema(_gen: &mut SchemaGenerator) -> Schema {
        // Define the schema as needed
        Schema::Object(SchemaObject {
            instance_type: Some(InstanceType::String.into()),
            metadata: Some(Box::new(schemars::schema::Metadata {
                description: Some("A log argument represented as a string.".to_string()),
                ..Default::default()
            })),
            ..Default::default()
        })
    }
}

// Since we cannot implement JsonSchema for ArrayVec due to orphan rules,
// we'll represent it as a Vec in the schema.

#[derive(Default, Serialize, Deserialize, Clone, Debug, PartialEq, JsonSchema)]
pub struct BlockSelection {
    /// Hash of a block, any blocks that have one of these hashes will be returned.
    /// Empty means match all.
    #[serde(default)]
    pub hash: Vec<HashWrapper>,
    /// Miner address of a block, any blocks that have one of these miners will be returned.
    /// Empty means match all.
    #[serde(default)]
    pub miner: Vec<AddressWrapper>,
}

#[derive(Default, Serialize, Deserialize, Clone, Debug, PartialEq, JsonSchema)]
pub struct LogSelection {
    /// Address of the contract, any logs that has any of these addresses will be returned.
    /// Empty means match all.
    #[serde(default)]
    pub address: Vec<AddressWrapper>,
    #[serde(default)]
    pub address_filter: Option<FilterWrapperSchema>,
    /// Topics to match, each member of the top-level array is another array.
    /// If the nth topic matches any topic specified in nth element of topics, the log will be returned.
    /// Empty means match all.
    #[serde(default)]
    #[schemars(schema_with = "log_topics_schema")]
    pub topics: ArrayVec<Vec<LogArgumentSchema>, 4>,
}

/// Custom schema generator for `topics` field
fn log_topics_schema(gen: &mut SchemaGenerator) -> Schema {
    let log_argument_schema = gen.subschema_for::<LogArgumentSchema>();
    let inner_array_schema = Schema::Object(SchemaObject {
        instance_type: Some(InstanceType::Array.into()),
        array: Some(Box::new(ArrayValidation {
            items: Some(log_argument_schema.into()),
            ..Default::default()
        })),
        ..Default::default()
    });

    Schema::Object(SchemaObject {
        instance_type: Some(InstanceType::Array.into()),
        array: Some(Box::new(ArrayValidation {
            items: Some(inner_array_schema.into()),
            ..Default::default()
        })),
        ..Default::default()
    })
}

#[derive(Default, Serialize, Deserialize, Clone, Debug, JsonSchema)]
pub struct TransactionSelection {
    // Fields as before...
    #[serde(default)]
    pub from: Vec<AddressWrapper>,
    #[serde(default)]
    pub from_filter: Option<FilterWrapperSchema>,
    // ... other fields
    #[serde(default)]
    pub hash: Vec<HashWrapper>,
}

#[derive(Default, Serialize, Deserialize, Clone, Debug, JsonSchema)]
pub struct TraceSelection {
    // Fields as before...
    #[serde(default)]
    pub from: Vec<AddressWrapper>,
    #[serde(default)]
    pub from_filter: Option<FilterWrapperSchema>,
    // ... other fields
    #[serde(default)]
    pub sighash: Vec<SighashWrapper>,
}

#[derive(Default, Serialize, Deserialize, Clone, Debug, JsonSchema)]
pub struct Query {
    // Fields as before...
    pub from_block: u64,
    pub to_block: Option<u64>,
    #[serde(default)]
    pub logs: Vec<LogSelection>,
    #[serde(default)]
    pub transactions: Vec<TransactionSelection>,
    #[serde(default)]
    pub traces: Vec<TraceSelection>,
    #[serde(default)]
    pub blocks: Vec<BlockSelection>,
    #[serde(default)]
    pub include_all_blocks: bool,
    #[serde(default)]
    pub field_selection: FieldSelection,
    #[serde(default)]
    pub max_num_blocks: Option<usize>,
    #[serde(default)]
    pub max_num_transactions: Option<usize>,
    #[serde(default)]
    pub max_num_logs: Option<usize>,
    #[serde(default)]
    pub max_num_traces: Option<usize>,
    #[serde(default)]
    pub join_mode: JoinMode,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq, Copy, JsonSchema)]
pub enum JoinMode {
    Default,
    JoinAll,
    JoinNothing,
}

impl Default for JoinMode {
    fn default() -> Self {
        Self::Default
    }
}

#[derive(Default, Serialize, Deserialize, Clone, Debug, JsonSchema)]
pub struct FieldSelection {
    #[serde(default)]
    pub block: BTreeSet<String>,
    #[serde(default)]
    pub transaction: BTreeSet<String>,
    #[serde(default)]
    pub log: BTreeSet<String>,
    #[serde(default)]
    pub trace: BTreeSet<String>,
}

#[derive(Clone, Copy, Deserialize, Serialize, Debug, JsonSchema)]
pub struct ArchiveHeight {
    pub height: Option<u64>,
}

#[derive(Clone, Copy, Deserialize, Serialize, Debug, JsonSchema)]
pub struct ChainId {
    pub chain_id: u64,
}

/// Guard for detecting rollbacks
#[derive(Debug, Clone, Serialize, JsonSchema)]
pub struct RollbackGuard {
    /// Block number of last block scanned in memory
    pub block_number: u64,
    /// Block timestamp of last block scanned in memory
    pub timestamp: i64,
    /// Block hash of last block scanned in memory
    pub hash: HashWrapper,
    /// Block number of first block scanned in memory
    pub first_block_number: u64,
    /// Parent hash of first block scanned in memory
    pub first_parent_hash: HashWrapper,
}
