# Satin

This is a fork of `rayon` that uses `forte` in place of `rayon-core`.

Compaired to `rayon`, we aim to:
1. Sacrafice no more than 10% of rayon's speedup vs serial for any particular workload.
2. Exactly match rayon's max throughput at load.
1. Significantly lower CPU utilization across the board.
2. Reduce fixed overhead and improve performance with large numbers of tiny tasks.
3. Allow async IO to interoperate seamlessly with parallel iteration.

This is currently a proof of concept, and should not be used in production.

## License

Rayon is distributed under the terms of both the MIT license and the
Apache License (Version 2.0). See [LICENSE-APACHE](LICENSE-APACHE) and
[LICENSE-MIT](LICENSE-MIT) for details. Opening a pull request is
assumed to signal agreement with these licensing terms.
