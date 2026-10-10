# `_io` buffer benchmark

`encrypt_io` / `decrypt_io` with `buffer_blocks` = 8K\*, 1, 2, 4, 8, 16, 32
and 64 on 1–100 MiB files. One block is one age chunk: 64 KiB, or 64 KiB + 16 B
for `encrypt_io`'s output, so each write carries whole encrypted chunks. `8K*`
simulates std's default 8 KiB buffers, the behaviour before `buffer_blocks`
existed. Each case: 3 fresh processes × 3 warm calls on a worker thread inside
uvloop; files are in the page cache. See [README.md](README.md) for the method.

## Best vs worst per file size

"in / out" is data read and written per second. "slower" columns compare
against the best setting. "spread" is the median run-to-run variation;
differences smaller than it are noise, which covers most of the 1–10 MiB rows.

| MiB | op | best | wall ms | in / out MiB/s | stall max | worst | wall ms | in / out MiB/s | stall max | 8K* wall ms | 8K* in / out MiB/s | 8K* stall max | spread ± | worst slower | 8K* slower |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 1 | encrypt | 16 | 1.0 | 976 / 977 | 0.09 | 8K* | 2.3 | 444 / 444 | 0.69 | 2.3 | 444 / 444 | 0.69 | 40% | +120% | +120% |
| 1 | decrypt | 1 | 1.2 | 821 / 820 | 0.19 | 8 | 2.4 | 424 / 424 | 0.27 | 1.9 | 533 / 533 | 0.37 | 57% | +93% | +54% |
| 5 | encrypt | 8 | 4.5 | 1119 / 1119 | 1.00 | 64 | 6.6 | 763 / 763 | 0.68 | 5.1 | 985 / 986 | 0.67 | 28% | +47% | +14% |
| 5 | decrypt | 4 | 5.4 | 926 / 926 | 0.45 | 8K* | 6.7 | 746 / 746 | 0.62 | 6.7 | 746 / 746 | 0.62 | 14% | +24% | +24% |
| 10 | encrypt | 4 | 8.6 | 1158 / 1158 | 0.92 | 64 | 11.1 | 898 / 899 | 0.77 | 9.1 | 1093 / 1093 | 0.57 | 9% | +29% | +6% |
| 10 | decrypt | 2 | 10.9 | 922 / 921 | 0.62 | 4 | 13.8 | 726 / 726 | 0.82 | 12.6 | 797 / 796 | 0.67 | 6% | +27% | +16% |
| 20 | encrypt | 4 | 17.1 | 1172 / 1173 | 0.15 | 1 | 19.0 | 1054 / 1055 | 0.88 | 18.6 | 1073 / 1074 | 0.50 | 7% | +11% | +9% |
| 20 | decrypt | 4 | 21.6 | 926 / 926 | 0.44 | 8K* | 24.5 | 816 / 816 | 1.37 | 24.5 | 816 / 816 | 1.37 | 4% | +13% | +13% |
| 100 | encrypt | 4 | 86.4 | 1157 / 1158 | 0.91 | 1 | 99.1 | 1010 / 1010 | 0.91 | 95.4 | 1049 / 1049 | 1.03 | 4% | +15% | +10% |
| 100 | decrypt | 2 | 110.2 | 908 / 908 | 0.50 | 8K* | 125.6 | 796 / 796 | 0.98 | 125.6 | 796 / 796 | 0.98 | 3% | +14% | +14% |

## Settings at 100 MiB

| blocks | encrypt MiB/s | decrypt MiB/s | max loop stall (enc / dec) | peak / avg extra memory |
|---|---|---|---|---|
| 8K\* | 1049 | 796 | 1.03 / 0.98 ms | 0.8 / 0.7 MiB |
| 1 (current default) | 1010 | 863 | 0.91 / 0.77 ms | 0.8 / 0.8 MiB |
| 2 | 1124 | 908 | 0.67 / 0.50 ms | 0.9 / 0.9 MiB |
| 4 | 1157 | 907 | 0.91 / 0.69 ms | 1.2 / 1.2 MiB |
| 8 | 1146 | 889 | 1.03 / 0.89 ms | 2.0 / 1.9 MiB |
| 16 | 1128 | 890 | 0.56 / 0.85 ms | 3.5 / 3.3 MiB |
| 32 | 1138 | 859 | 0.39 / 0.60 ms | 6.5 / 6.2 MiB |
| 64 | 1118 | 856 | 0.49 / 1.05 ms | 12.5 / 11.7 MiB |

- **Encrypt needs at least 2 blocks.** At 1 block (or 8K\*, which behaves the
  same on encrypt's output) every `write()` is a single encrypted chunk. From 2
  blocks up, whole chunks are batched per call, worth 11–15%.
- **Decrypt is slowest with 8K\*,** because its output is written in 8 KiB
  pieces. 2–4 blocks are 14% faster.
- **Loop stall:** the longest single stall is 0.1–1.4 ms in every case. For 20
  and 100 MiB files it adds up to at most 3.3% of a call's time, with no
  pattern by buffer size.
- **Memory** grows with the buffers. From 8 blocks up, peak extra memory is
  about 3–4 × one buffer: the read and write buffers plus the Python `bytes`
  object from each read.

## Recommendation

- **Use 4 blocks.** It's the best or within 1% of the best at 20 and 100 MiB for
  both ops, at about 1.2 MiB of extra memory. Compared with the current
  default of 1, that's about 15% faster on encrypt and 5% on decrypt; compared
  with the old 8 KiB buffers, 10% and 14%.
- **Avoid 32–64 blocks:** no faster, and up to 12.5 MiB of extra memory.
