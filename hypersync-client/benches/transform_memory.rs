use anyhow::{Context, Result};
use bytesize::ByteSize;
use hypersync_client::{simple_types, zero_copy_types::TransactionReader, Client, FromArrow};
use hypersync_net_types::{
    BlockField, LogField, LogFilter, Query, TransactionField, TransactionFilter,
};
use polars_arrow::array::BinaryArray;
use ruint::Uint;
use std::{
    alloc::{GlobalAlloc, Layout, System},
    sync::atomic::{AtomicUsize, Ordering},
    time::{Duration, Instant},
};

// --- Memory tracker -------------------------------------------------------

struct MemoryTracker {
    /// Total live bytes ever tracked (monotonic up/down with alloc/dealloc).
    total: AtomicUsize,
    /// Baseline snapshot at last reset().
    baseline: AtomicUsize,
    /// Peak "current since baseline" value.
    peak_since_baseline: AtomicUsize,
}

impl MemoryTracker {
    const fn new() -> Self {
        Self {
            total: AtomicUsize::new(0),
            baseline: AtomicUsize::new(0),
            peak_since_baseline: AtomicUsize::new(0),
        }
    }

    /// Start a new measurement window.
    fn reset(&self) {
        let now = self.total.load(Ordering::Relaxed);
        self.baseline.store(now, Ordering::Relaxed);
        self.peak_since_baseline.store(0, Ordering::Relaxed);
    }

    /// Current bytes "since reset".
    fn current(&self) -> usize {
        let total = self.total.load(Ordering::Relaxed);
        let base = self.baseline.load(Ordering::Relaxed);
        total.saturating_sub(base)
    }

    /// Peak bytes "since reset".
    fn peak(&self) -> usize {
        self.peak_since_baseline.load(Ordering::Relaxed)
    }

    #[inline]
    fn on_alloc(&self, size: usize) {
        let new_total = self.total.fetch_add(size, Ordering::Relaxed) + size;

        // compute "current since baseline"
        let base = self.baseline.load(Ordering::Relaxed);
        let current = new_total.saturating_sub(base);

        // update peak_since_baseline if needed
        let mut peak = self.peak_since_baseline.load(Ordering::Relaxed);
        while current > peak {
            match self.peak_since_baseline.compare_exchange_weak(
                peak,
                current,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => break,
                Err(p) => peak = p,
            }
        }
    }

    #[inline]
    fn on_dealloc(&self, size: usize) {
        // We don't touch peak here; just move total down.
        self.total.fetch_sub(size, Ordering::Relaxed);
    }
}

unsafe impl GlobalAlloc for MemoryTracker {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let ptr = System.alloc(layout);
        if !ptr.is_null() {
            self.on_alloc(layout.size());
        }
        ptr
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        self.on_dealloc(layout.size());
        System.dealloc(ptr, layout);
    }

    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        let ptr = System.alloc_zeroed(layout);
        if !ptr.is_null() {
            self.on_alloc(layout.size());
        }
        ptr
    }

    unsafe fn realloc(&self, ptr: *mut u8, old_layout: Layout, new_size: usize) -> *mut u8 {
        let new_ptr = System.realloc(ptr, old_layout, new_size);
        if !new_ptr.is_null() {
            // realloc semantics: think of it as "free old, alloc new"
            let old_size = old_layout.size();
            if new_size >= old_size {
                self.on_alloc(new_size - old_size);
            } else {
                self.on_dealloc(old_size - new_size);
            }
        }
        new_ptr
    }
}

#[global_allocator]
static TRACKER: MemoryTracker = MemoryTracker::new();

// --- Benchmark wrapper ----------------------------------------------------

#[derive(Debug)]
pub struct MemoryBenchmark {
    pub name: String,
    pub memory_used: usize, // live at end of function
    pub peak_memory: usize, // max live during function
    pub duration: Duration,
}

pub fn benchmark_memory<T, F>(name: &str, f: F) -> (T, MemoryBenchmark)
where
    F: FnOnce() -> T,
{
    TRACKER.reset();
    std::hint::black_box([0u8; 64]); // force a tiny bit of work

    let start = Instant::now();
    let result = f();
    let duration = start.elapsed();

    std::hint::black_box(&result);

    let bench = MemoryBenchmark {
        name: name.to_string(),
        memory_used: TRACKER.current(),
        peak_memory: TRACKER.peak(),
        duration,
    };

    (result, bench)
}

impl MemoryBenchmark {
    pub fn print(&self) {
        println!(
            "{}: {} current, {} peak, {:?}",
            self.name,
            ByteSize(self.memory_used.try_into().unwrap()),
            ByteSize(self.peak_memory.try_into().unwrap()),
            self.duration
        );
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let _ = dotenvy::dotenv();
    println!("Setting up client ...");

    let client = Client::builder()
        .chain_id(1)
        .api_token(dotenvy::var("HYPERSYNC_API_TOKEN")?)
        .build()
        .context("Failed to build client")?;

    let query = Query::new()
        .from_block(4_000_000)
        .to_block_excl(4_000_100)
        .include_all_blocks()
        .where_transactions(TransactionFilter::all())
        .where_logs(LogFilter::all())
        .select_log_fields(LogField::all())
        .select_block_fields(BlockField::all())
        .select_transaction_fields(TransactionField::all());
    // .select_transaction_fields([TransactionField::GasUsed, TransactionField::Hash]);

    println!("Querying data ...");
    let res = client.collect_arrow(query, Default::default()).await?;
    println!("Data collected!");

    println!("\n Running benchmarks ...");

    let transactions = res.data.transactions.clone();
    let (gas_used_1, bench) = benchmark_memory("tally tx gas used", || {
        let mut total_gas_used: Uint<256, 4> = Uint::ZERO;
        for tx_batch in transactions {
            for tx in simple_types::Transaction::from_arrow(&tx_batch) {
                total_gas_used += Uint::from_be_slice(&tx.gas_used.expect("gas_used not found"));
            }
        }
        total_gas_used
    });
    bench.print();

    let transactions = res.data.transactions.clone();
    let (gas_used_2, bench) = benchmark_memory("tally tx gas used from zero copy", || {
        let mut total_gas_used: Uint<256, 4> = Uint::ZERO;
        for tx_batch in transactions {
            for tx_reader in TransactionReader::iter(&tx_batch) {
                total_gas_used +=
                    Uint::from_be_slice(&tx_reader.gas_used().expect("gas_used not found"));
            }
        }
        total_gas_used
    });
    bench.print();

    assert_eq!(gas_used_1, gas_used_2);
    // let transactions = res.data.transactions.clone();
    // let (gas_used_2, bench) = benchmark_memory("tally tx gas used from arrow", || {
    //     let mut total_gas_used: Uint<256, 4> = Uint::ZERO;
    //     for tx_batch in transactions {
    //         let gas_used_col = tx_batch
    //             .column::<BinaryArray<i32>>("gas_used")
    //             .expect("gas_used incorrect");
    //         for gu in gas_used_col.iter() {
    //             total_gas_used += Uint::from_be_slice(gu.expect("gas_used not found"));
    //         }
    //     }
    //     total_gas_used
    // });
    // bench.print();
    //
    // assert_eq!(gas_used_1, gas_used_2);
    let ((num_txs, num_blocks, numb_logs, _), bench) =
        benchmark_memory("convert all types", || {
            let mut num_txs = 0;
            let mut last_tx_hash = None;
            for tx_batch in res.data.transactions {
                for tx in simple_types::Transaction::from_arrow(&tx_batch) {
                    num_txs += 1;
                    last_tx_hash = Some(tx.hash.expect("hash not found"));
                }
            }
            let mut num_blocks = 0;
            let mut last_block_hash = None;
            for block_batch in res.data.blocks {
                for block in simple_types::Block::from_arrow(&block_batch) {
                    num_blocks += 1;
                    last_block_hash = Some(block.hash.expect("hash not found"));
                }
            }
            let mut num_logs = 0;
            let mut last_log_hash = None;
            for log_batch in res.data.logs {
                for log in simple_types::Log::from_arrow(&log_batch) {
                    num_logs += 1;
                    last_log_hash = Some(log.transaction_hash.expect("hash not found"));
                }
            }

            (
                num_txs,
                num_blocks,
                num_logs,
                (last_tx_hash, last_block_hash, last_log_hash),
            )
        });
    bench.print();
    println!("Collected {} transactions", num_txs);
    println!("Collected {} blocks", num_blocks);
    println!("Collected {} logs", numb_logs);

    Ok(())
}
