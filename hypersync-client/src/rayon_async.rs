use tokio::sync::oneshot;

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
