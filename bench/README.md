# `_io` buffer benchmark

Benchmarks `pyrage.encrypt_io` / `pyrage.decrypt_io` for each `buffer_blocks`
value (8K*, 1, 2, 4, 8, 16, 32, 64) on 1, 5, 10, 20 and 100 MiB files. `8K*` is
a simulated 8 KiB buffer (see below). Results: [RESULTS.md](RESULTS.md).

```sh
make bench                                  # prepare test files + full matrix
make bench BENCH_ARGS="--loop asyncio"      # stdlib loop instead of uvloop
uv run --with uvloop python bench/bench_io.py run --sizes 100 --blocks 8k,1,4 --ops decrypt
uv run python bench/bench_io.py report      # best/worst per size from the last run
uv run python bench/bench_io.py report --markdown bench/RESULTS_TABLES.md
```

The full matrix is 80 cases and takes about 3 minutes.

## Buffer sizes

A block is one age chunk. On the plaintext side that's 64 KiB. Binary
ciphertext chunks are 16 bytes larger (Poly1305 tag), so `encrypt_io` sizes its
output buffer as `buffer_blocks × (64 KiB + 16 B)`. That way each `write()`
carries exactly `buffer_blocks` encrypted chunks; a plain N × 64 KiB buffer
only fits N−1 and flushes early. The other three buffers are N × 64 KiB.
`decrypt_io`'s input isn't chunk-aligned, but `BufReader` streams it, so every
refill is still one full buffer.

## Method

`prepare` writes random plaintext files to `bench/data/`, encrypts each once
with `encrypt_io` and checks a `decrypt_io` round-trip. `run` then runs every
(op, size, buffer_blocks) case in `--procs` fresh child processes:

- The call runs on a worker thread via `loop.run_in_executor` inside a uvloop
  (default) or asyncio loop; `--mode inline` calls it on the loop instead.
- **Loop stall**: a heartbeat coroutine sleeps 1 ms in a loop and records how
  late each wakeup is. `stall` is the lag beyond the idle loop's p99 lag
  (uvloop timers have 1 ms granularity, so smaller stalls are below the noise).
- **Time / throughput / CPU**: median over the warm calls (`--repeat` per
  process); MiB/s is plaintext size / wall time, in/out MiB/s are bytes read
  and written per second.
- **Memory**: measured on the first call of each fresh process, since glibc
  keeps freed buffers afterwards. Peak is VmHWM (reset via
  `/proc/self/clear_refs`); average comes from the parent sampling the child's
  RSS every 1 ms, so sampling never competes for the child's GIL.
- **`8K*`**: the API can't go below one 64 KiB block, so std's default 8 KiB
  `BufReader`/`BufWriter` (the behaviour before `buffer_blocks`) is simulated
  with `buffer_blocks=1` and a wrapper that caps each Python `read()` (and, for
  decrypt, `write()`) at 8 KiB. pyo3-file honours short
  reads/writes, so this gives the same GIL round-trips. Encrypt writes aren't
  capped because age's encrypted chunks skip an 8 KiB `BufWriter` anyway. The
  wrapper's extra Python call per read/write counts toward the measurement.

## Output

`run` prints a best/worst table per file size and op, with throughput and loop
stall for the best, the worst and the `8K*` baseline. `--full` also prints
every case. Results go to `bench/results/latest.{json,csv}`
(`--out` to change); `report` reprints them, and `--markdown` writes the tables
as Markdown. Linux only (`/proc`).
