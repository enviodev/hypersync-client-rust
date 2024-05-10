use std::cmp;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

pub struct BlockIterator {
    offset: u64,
    end: u64,
    step: Arc<AtomicU64>,
}

impl BlockIterator {
    // fn new() -> BlockIterator {
    //     BlockIterator {
    //         offset: 0,
    //         end: 0,
    //         step: Arc::new(AtomicU64::new(0)),
    //     }
    // }

    pub fn build(offset: u64, end: u64, step: Arc<AtomicU64>) -> BlockIterator {
        BlockIterator { offset, end, step }
    }
}

impl Iterator for BlockIterator {
    type Item = (u64, u64);

    fn next(&mut self) -> Option<Self::Item> {
        // Increment our count. This is why we started at zero.
        if self.offset == self.end {
            return None;
        }
        let start = self.offset;
        self.offset = cmp::min(self.offset + self.step.load(Ordering::SeqCst), self.end);
        Some((start, self.offset))
    }
}

pub const TX_COEFFICIENT: usize = 1;
pub const LOG_COEFFICIENT: usize = 1;
pub const BLOCK_COEFFICIENT: usize = 1;
pub const TRACE_COEFFICIENT: usize = 1;

pub const TARGET_SIZE: u64 = 10000;
