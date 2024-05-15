use std::cmp;
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::task::{Context, Poll};
use tokio::sync::mpsc;
use tokio_stream::Stream;

pub struct BlockIterator {
    offset: u64,
    end: u64,
    step: Arc<AtomicU64>,
    rx_unbounded: mpsc::Receiver<Option<u64>>, // channel used to receive
    unbounded_counter: usize,                  // Number of block ranges produced
    unbounded_concurrency: usize, // if unbounded counter % unbounded_concurrency == 0 iterator should produce unbounded block range
    unbounded_call_issued: bool,
}

impl BlockIterator {
    pub fn build(
        offset: u64,
        end: u64,
        step: Arc<AtomicU64>,
        rx_unbounded: mpsc::Receiver<Option<u64>>,
        unbounded_concurrency: usize,
    ) -> BlockIterator {
        BlockIterator {
            offset,
            end,
            step,
            rx_unbounded,
            unbounded_counter: 0,
            unbounded_concurrency,
            unbounded_call_issued: false,
        }
    }
}

impl Stream for BlockIterator {
    type Item = (u64, Option<u64>);

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if self.offset == self.end {
            return Poll::Ready(None);
        }
        if self.unbounded_counter % self.unbounded_concurrency == 0 {
            self.unbounded_counter += 1;
            self.unbounded_call_issued = true;
            return Poll::Ready(Some((self.offset, None)));
        } else if self.unbounded_call_issued {
            match self.rx_unbounded.poll_recv(cx) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(None) => return Poll::Ready(None),
                Poll::Ready(value) => self.offset = value.unwrap().unwrap(),
            }
            // unbounded call processed
            self.unbounded_call_issued = false;
        }
        let start = self.offset;
        self.offset = cmp::min(self.offset + self.step.load(Ordering::SeqCst), self.end);
        self.unbounded_counter += 1;
        Poll::Ready(Some((start, Some(self.offset))))
    }
}
