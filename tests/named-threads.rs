use satin::prelude::*;

// NOTE: forte does not support custom thread names via a pool builder.
// This test verifies that parallel iteration works correctly across multiple threads.

static NAMED_POOL: forte::ThreadPool = forte::ThreadPool::new();

#[test]
#[cfg_attr(any(target_os = "emscripten", target_family = "wasm"), ignore)]
fn parallel_threads() {
    NAMED_POOL.resize_to(2);

    const N: usize = 10000;

    let sum: usize = NAMED_POOL.with_worker(|_| {
        (0..N).into_par_iter().sum()
    });

    assert_eq!(sum, N * (N - 1) / 2);
}
