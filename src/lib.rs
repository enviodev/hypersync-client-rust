//! This crate is simply a re-export of the client we use internally for hypersync ("skar")
//! with some [examples](https://github.com/enviodev/hypersync-client-rust/tree/main/examples)
//!
//! Find docs on hypersync client functionality in `skar-client`, types (Block, Transaction, Address, etc) in `skar-format`, and query parameters in `skar-net-types`.

pub use skar_client as client;
pub use skar_format as format;
pub use skar_net_types as net_types;
