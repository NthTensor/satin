use satin::prelude::*;

static POOL1: forte::ThreadPool = forte::ThreadPool::new();
static POOL2: forte::ThreadPool = forte::ThreadPool::new();

#[test]
#[cfg_attr(any(target_os = "emscripten", target_family = "wasm"), ignore)]
fn cross_pool_busy() {
    POOL1.resize_to(1);
    POOL2.resize_to(1);

    let n: i32 = 100;
    let sum: i32 = POOL1.with_worker(move |_| {
        // Each item will block on pool2, but pool1 can continue processing other work from the
        // parallel iterator in the meantime. There's a chance that pool1 will still be awake to
        // see the latch set without being tickled, and then it will drop that stack job. The latch
        // internals must not assume that the job will still be alive after it's set!
        (1..=n)
            .into_par_iter()
            .map(|i| POOL2.with_worker(move |_| i))
            .sum()
    });
    assert_eq!(sum, n * (n + 1) / 2);
}
