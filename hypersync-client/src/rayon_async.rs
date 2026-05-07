//! Bridge for running CPU-bound work concurrently with async I/O.
//!
//! On native we farm the closure off to a rayon worker so the tokio reactor
//! can keep servicing other futures while the work runs. On wasm there is
//! only one thread, so we just invoke the closure synchronously and return
//! a ready future. The call site in `stream::map_responses` does
//! `spawn(...).await.unwrap()`, which works for both shapes because we use
//! `tokio::sync::oneshot` (which is `Send`-free and wasm-clean).

use tokio::sync::oneshot;

#[cfg(not(target_arch = "wasm32"))]
pub fn spawn<F, T>(func: F) -> oneshot::Receiver<T>
where
    F: 'static + FnOnce() -> T + Send,
    T: 'static + Send + Sync,
{
    let (tx, rx) = oneshot::channel();

    rayon::spawn(move || {
        let res = func();
        tx.send(res).ok();
    });

    rx
}

#[cfg(target_arch = "wasm32")]
pub fn spawn<F, T>(func: F) -> oneshot::Receiver<T>
where
    F: FnOnce() -> T,
{
    let (tx, rx) = oneshot::channel();
    let res = func();
    tx.send(res).ok();
    rx
}
