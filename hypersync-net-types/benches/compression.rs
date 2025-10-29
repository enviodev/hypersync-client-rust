use arrayvec::ArrayVec;
use hypersync_format::{Address, FixedSizeData};
use hypersync_net_types::{log::LogField, FieldSelection, LogFilter, LogSelection, Query};
use pretty_assertions::assert_eq;
use std::{
    collections::HashMap,
    io::{Read, Write},
    time::Duration,
};

fn main() {
    // Collect rankings in each benchmark to find out which is the overall best
    let mut bytes_ranks = HashMap::new();
    let mut decode_time_ranks = HashMap::new();
    let mut encode_time_ranks = HashMap::new();

    let mut add_to_ranks = |mut encodings: Vec<Encoding>| {
        // sort by len fist
        encodings.sort_by_key(|a| a.bytes.len());
        let mut prev_val = 0;
        let mut current_pos = 0;
        for encoding in encodings.iter() {
            if encoding.bytes.len() > prev_val {
                current_pos += 1;
                prev_val = encoding.bytes.len();
            }
            let current = bytes_ranks.get(&encoding.name).unwrap_or(&0);
            bytes_ranks.insert(encoding.name.clone(), current + current_pos);
        }

        encodings.sort_by_key(|a| a.decode_time);
        let mut prev_val = Duration::from_secs(0);
        current_pos = 0;
        for encoding in encodings.iter() {
            if encoding.decode_time > prev_val {
                current_pos += 1;
                prev_val = encoding.decode_time;
            }
            let current = decode_time_ranks.get(&encoding.name).unwrap_or(&0);
            decode_time_ranks.insert(encoding.name.clone(), current + current_pos);
        }
        encodings.sort_by_key(|a| a.encode_time);
        prev_val = Duration::from_secs(0);
        current_pos = 0;
        for encoding in encodings.iter() {
            if encoding.encode_time > prev_val {
                current_pos += 1;
                prev_val = encoding.encode_time;
            }
            let current = encode_time_ranks.get(&encoding.name).unwrap_or(&0);
            encode_time_ranks.insert(encoding.name.clone(), current + current_pos);
        }
    };

    // Benchmark different payload sizes of logs queries (similar to an indexer)
    let logs = mock_logs_query(build_mock_logs(3, 3, 1));
    let res = benchmark_compression(logs, "small payload");
    add_to_ranks(res);

    let logs = build_mock_logs(5, 6, 3);
    let mock_query = mock_logs_query(logs);
    let res = benchmark_compression(mock_query, "moderate payload");
    add_to_ranks(res);

    let logs = build_mock_logs(3, 5, 6);
    let mock_query = mock_logs_query(logs);
    let res = benchmark_compression(mock_query, "moderate payload 2");
    add_to_ranks(res);

    let logs = build_mock_logs(4, 3, 5);
    let mock_query = mock_logs_query(logs);
    let res = benchmark_compression(mock_query, "moderate payload 3");
    add_to_ranks(res);

    let logs = build_mock_logs(3, 7, 50);
    let mock_query = mock_logs_query(logs);
    let res = benchmark_compression(mock_query, "medium large payload");
    add_to_ranks(res);

    let logs = build_mock_logs(3, 6, 200);
    let mock_query = mock_logs_query(logs);
    let res = benchmark_compression(mock_query, "large payload");
    add_to_ranks(res);

    let logs = build_mock_logs(5, 6, 1000);
    let mock_query = mock_logs_query(logs);
    let res = benchmark_compression(mock_query, "huge payload");
    add_to_ranks(res);

    // let logs = build_mock_logs(1, 3, 10000);
    // let res = benchmark_compression(mock_logs_query(logs), "huge payload less contracts");
    // add_to_ranks(res);

    // Print rankings
    let mut bytes_ranks = bytes_ranks.into_iter().collect::<Vec<_>>();
    bytes_ranks.sort_by_key(|a| a.1);
    println!("\nBytes ranks");
    for (name, rank) in bytes_ranks.iter() {
        println!("{name}: {rank}");
    }

    let mut decode_time_ranks = decode_time_ranks.into_iter().collect::<Vec<_>>();
    decode_time_ranks.sort_by_key(|a| a.1);
    println!("\nDecode time ranks");
    for (name, rank) in decode_time_ranks.iter() {
        println!("{name}: {rank}");
    }
    println!("\nEncode time ranks");
    let mut encode_time_ranks = encode_time_ranks.into_iter().collect::<Vec<_>>();
    encode_time_ranks.sort_by_key(|a| a.1);
    for (name, rank) in encode_time_ranks.iter() {
        println!("{name}: {rank}");
    }
}

fn build_mock_logs(
    num_contracts: usize,
    num_topic_0_per_contract: usize,
    num_addresses_per_contract: usize,
) -> Vec<LogSelection> {
    fn mock_topic(input_a: usize, input_b: usize, seed: &str) -> FixedSizeData<32> {
        use sha3::{Digest, Sha3_256};
        let mut hasher = Sha3_256::new();
        hasher.update(seed.as_bytes());
        hasher.update((input_a as u64).to_le_bytes());
        hasher.update((input_b as u64).to_le_bytes());
        let result = hasher.finalize();
        let val: [u8; 32] = result.into();
        FixedSizeData::from(val)
    }

    fn mock_address(input_a: usize, input_b: usize) -> Address {
        let topic = mock_topic(input_a, input_b, "ADDRESS");
        let address: [u8; 20] = topic.as_ref()[0..20].try_into().unwrap();
        Address::from(address)
    }
    let mut logs: Vec<LogSelection> = Vec::new();

    for contract_idx in 0..num_contracts {
        let mut topics = ArrayVec::new();
        topics.push(vec![]);
        let mut log_selection = LogFilter {
            address: vec![],
            address_filter: None,
            topics,
        };

        for topic_idx in 0..num_topic_0_per_contract {
            log_selection.topics[0].push(mock_topic(contract_idx, topic_idx, "TOPICS"));
        }

        for addr_idx in 0..num_addresses_per_contract {
            let address = mock_address(contract_idx, addr_idx);
            log_selection.address.push(address);
        }
        logs.push(log_selection.into());
    }
    logs
}

fn mock_logs_query(logs: Vec<LogSelection>) -> Query {
    Query {
        from_block: 50,
        to_block: Some(500),
        logs,
        field_selection: FieldSelection {
            log: LogField::all(),
            ..Default::default()
        },
        ..Default::default()
    }
}
#[derive(Debug, Clone)]
struct Encoding {
    name: String,
    bytes: Vec<u8>,
    encode_time: std::time::Duration,
    decode_time: std::time::Duration,
}

use tabled::{Table, Tabled};

#[derive(Tabled)]
struct EncodingRow {
    name: String,
    bytes_len: String,
    encode_time: String,
    decode_time: String,
}

impl Encoding {
    fn to_table(rows: Vec<Encoding>) -> Table {
        let smallest_bytes = rows.iter().map(|r| r.bytes.len()).min().unwrap();
        let shortest_encode_time = rows.iter().map(|r| r.encode_time).min().unwrap();
        let shortest_decode_time = rows.iter().map(|r| r.decode_time).min().unwrap();

        let mut table_rows = Vec::new();
        for encoding in rows {
            table_rows.push(encoding.to_encoding_row(
                smallest_bytes,
                shortest_encode_time,
                shortest_decode_time,
            ));
        }
        Table::new(table_rows)
    }
    fn to_encoding_row(
        &self,
        smallest_bytes: usize,
        shortest_encode_time: Duration,
        shortest_decode_time: Duration,
    ) -> EncodingRow {
        fn percentage_incr(a: f64, b: f64) -> f64 {
            ((a - b) / b * 100.0).round()
        }
        fn add_percentage(val: Duration, lowest: Duration) -> String {
            if val == lowest {
                format!("{val:?}")
            } else {
                let percentage = percentage_incr(val.as_micros() as f64, lowest.as_micros() as f64);
                format!("{val:?} ({percentage}%)")
            }
        }

        let bytes_len = if self.bytes.len() == smallest_bytes {
            format!("{}", self.bytes.len())
        } else {
            let percentage = percentage_incr(self.bytes.len() as f64, smallest_bytes as f64);
            format!("{} ({percentage}%)", self.bytes.len())
        };

        EncodingRow {
            name: self.name.clone(),
            bytes_len,
            encode_time: add_percentage(self.encode_time, shortest_encode_time),
            decode_time: add_percentage(self.decode_time, shortest_decode_time),
        }
    }
    fn new<T: PartialEq + std::fmt::Debug>(
        input: &T,
        name: String,
        encode: impl FnOnce(&T) -> Vec<u8>,
        decode: impl FnOnce(&[u8]) -> T,
    ) -> Encoding {
        let encode_start = std::time::Instant::now();
        let val = encode(input);
        let encode_time = encode_start.elapsed();

        let decode_start = std::time::Instant::now();
        let decoded = decode(&val);
        let decode_time = decode_start.elapsed();
        assert_eq!(input, &decoded);

        Encoding {
            name,
            bytes: val,
            encode_time,
            decode_time,
        }
    }

    fn with_compression(
        &self,
        name: &'static str,
        compress: impl FnOnce(&[u8], u32) -> Vec<u8>,
        decompress: impl FnOnce(&[u8], u32) -> Vec<u8>,
        level: u32,
    ) -> Encoding {
        let name = format!("{}-{}{}", self.name, name, level);
        let mut compressed_data = Self::new(
            &self.bytes,
            name,
            |bytes| compress(bytes, level),
            |bytes| decompress(bytes, level),
        );

        compressed_data.encode_time += self.encode_time;
        compressed_data.decode_time += self.decode_time;

        compressed_data
    }

    fn add_to_table_with_compressions(self, table_vec: &mut Vec<Encoding>) {
        table_vec.push(self.clone());

        fn zlib_encode(bytes: &[u8], level: u32) -> Vec<u8> {
            use flate2::{write::ZlibEncoder, Compression};
            let mut enc = ZlibEncoder::new(Vec::new(), Compression::new(level));
            enc.write_all(bytes).unwrap();
            enc.finish().unwrap()
        }
        fn zlib_decode(bytes: &[u8], _level: u32) -> Vec<u8> {
            use flate2::read::ZlibDecoder;
            let mut dec = ZlibDecoder::new(bytes);
            let mut buf = Vec::new();
            dec.read_to_end(&mut buf).unwrap();
            buf
        }

        fn zstd_encode(bytes: &[u8], level: u32) -> Vec<u8> {
            zstd::encode_all(bytes, level as i32).unwrap()
        }
        fn zstd_decode(bytes: &[u8], _level: u32) -> Vec<u8> {
            zstd::decode_all(bytes).unwrap()
        }
        table_vec.push(self.with_compression("zstd", zstd_encode, zstd_decode, 3));
        table_vec.push(self.with_compression("zlib", zlib_encode, zlib_decode, 3));
        table_vec.push(self.with_compression("zstd", zstd_encode, zstd_decode, 9));
        table_vec.push(self.with_compression("zlib", zlib_encode, zlib_decode, 9));
        table_vec.push(self.with_compression("zstd", zstd_encode, zstd_decode, 6));
        table_vec.push(self.with_compression("lz4", lz4_encode, lz4_decode, 0));
        table_vec.push(self.with_compression("zstd", zstd_encode, zstd_decode, 12));

        fn lz4_encode(bytes: &[u8], _level: u32) -> Vec<u8> {
            lz4_flex::compress(bytes)
        }
        fn lz4_decode(bytes: &[u8], _level: u32) -> Vec<u8> {
            lz4_flex::decompress(bytes, u32::MAX as usize).unwrap()
        }
    }
}
fn benchmark_compression(query: Query, label: &str) -> Vec<Encoding> {
    let capnp_packed = {
        fn to_capnp_bytes_packed(query: &Query) -> Vec<u8> {
            query.to_capnp_bytes_packed().unwrap()
        }

        fn from_capnp_bytes_packed(bytes: &[u8]) -> Query {
            Query::from_capnp_bytes_packed(bytes).unwrap()
        }

        Encoding::new(
            &query,
            "capnp-packed".to_string(),
            to_capnp_bytes_packed,
            from_capnp_bytes_packed,
        )
    };

    let capnp = {
        fn to_capnp_bytes(query: &Query) -> Vec<u8> {
            query.to_capnp_bytes().unwrap()
        }

        fn from_capnp_bytes(bytes: &[u8]) -> Query {
            Query::from_capnp_bytes(bytes).unwrap()
        }

        Encoding::new(
            &query,
            "capnp".to_string(),
            to_capnp_bytes,
            from_capnp_bytes,
        )
    };

    let json = {
        fn to_json(query: &Query) -> Vec<u8> {
            serde_json::to_vec(query).unwrap()
        }
        fn from_json(bytes: &[u8]) -> Query {
            serde_json::from_slice(bytes).unwrap()
        }
        Encoding::new(&query, "json".to_string(), to_json, from_json)
    };
    Encoding::new(
        &query,
        "json".to_string(),
        |q| serde_json::to_vec(q).unwrap(),
        |bytes| serde_json::from_slice(bytes).unwrap(),
    );

    let mut table_rows = Vec::new();

    for encoding in [capnp, capnp_packed, json] {
        encoding.add_to_table_with_compressions(&mut table_rows);
    }

    table_rows.sort_by_key(|a| a.bytes.len());

    let table = Encoding::to_table(table_rows.clone());

    println!("Benchmark {label}\n{table}\n");
    table_rows
}
