# rust-1B-row-challenge
A Rust implementation of [The One Billion Row Challenge](https://github.com/gunnarmorling/1brc)

This is currently the fastest implementation of the challenge. It runs for about 7-8 seconds in MBP 2019 (2.6 GHz 6-Core Intel Core i7).

As a comparison, the current [top 3 Java implementations](https://github.com/gunnarmorling/1brc#results) run in my machine for more than 15 seconds. Other implementations (C, C++, Rust) shared in the [Discussions](https://github.com/gunnarmorling/1brc/discussions) run for more than 12 seconds.

Summary of the approach:
- Uses [a general-purpose float parser](https://crates.io/crates/fast-float) (not an input-specific one)
- Uses [a general-purpose hash function](https://crates.io/crates/ahash) (not an input-specific one)
- Uses [hashbrown](https://crates.io/crates/hashbrown)'s `HashMap`, which is exactly the same as `HashMap` in the stdlib, except the former has the [`entry_ref` method](https://docs.rs/hashbrown/latest/hashbrown/struct.HashMap.html#method.entry_ref). It turns out using `entry_ref` has practically the same performance as using stdlib's `HashMap` + the `get_mut`-then-`insert` approach. That's because the input data has a very few unique city names, compared to the total input lines.
- Uses [crossbeam_channel](https://crates.io/crates/crossbeam-channel) for sending data from the main thread (which only reads the input file) to the worker threads. Tried using [`rayon`](https://crates.io/crates/rayon) before, but the lock contention became a bottleneck.

The current implementation reads the input file sequentially. This can be further improved by reading it in parallel.

To run:

```bash
cargo run --release -- ./path/to/measurements.txt
```
