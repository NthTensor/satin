use satin::prelude::*;
use std::iter::once_with;

const N: usize = 100_000;

static PAR_BRIDGE_POOL: forte::ThreadPool = forte::ThreadPool::new();

#[test]
#[cfg_attr(any(target_os = "emscripten", target_family = "wasm"), ignore)]
fn par_bridge_recursion() {
    PAR_BRIDGE_POOL.resize_to(10);

    let seq: Vec<_> = (0..N).map(|i| (i, i.to_string())).collect();

    PAR_BRIDGE_POOL.broadcast(|_| {
        let mut par: Vec<_> = (0..N)
            .into_par_iter()
            .flat_map(|i| {
                once_with(move || {
                    // Using satin within the serial iterator creates an opportunity for
                    // work-stealing to make par_bridge's mutex accidentally recursive.
                    forte::join(move |_| i, move |_| i.to_string())
                })
                .par_bridge()
            })
            .collect();
        par.par_sort_unstable();
        assert_eq!(seq, par);
    });
}
