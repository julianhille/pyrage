"""
Benchmark `pyrage.encrypt_io` / `pyrage.decrypt_io` across `buffer_blocks`.

Every case runs in its own child process inside an asyncio (or uvloop)
event loop. The crypto call is dispatched to a worker thread with
`run_in_executor`; meanwhile a heartbeat coroutine measures how long the
loop is stalled (e.g. by GIL contention from pyo3-file's per-read/write GIL
re-acquisition). The parent process samples the child's RSS from outside so
the sampling itself never competes for the child's GIL.

    python bench/bench_io.py prepare            # create + encrypt test files once
    python bench/bench_io.py run                # run the full matrix
    python bench/bench_io.py run --sizes 100 --blocks 8k,1,4 --ops encrypt
    python bench/bench_io.py report             # best/worst table from last run
    python bench/bench_io.py report --markdown bench/RESULTS_TABLES.md

`8k` in --blocks simulates std's default 8 KiB buffers (see `Capped8K`); it is
stored as buffer_blocks=0 in the results and shown as `8K*`.
"""

from __future__ import annotations

import argparse
import asyncio
import csv
import hashlib
import json
import os
import resource
import statistics
import subprocess
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import pyrage

MIB = 1024 * 1024
HERE = Path(__file__).resolve().parent
DEFAULT_DATA_DIR = HERE / "data"
DEFAULT_SIZES = [1, 5, 10, 20, 100]
SIM_8K = 0  # pseudo buffer_blocks value for the simulated 8 KiB case
SIM_8K_BYTES = 8 * 1024
DEFAULT_BLOCKS = [SIM_8K, 1, 2, 4, 8, 16, 32, 64]
PAGE_SIZE = os.sysconf("SC_PAGE_SIZE")


def plain_path(data_dir: Path, size: int) -> Path:
    return data_dir / f"plain_{size}MiB.bin"


def enc_path(data_dir: Path, size: int) -> Path:
    return data_dir / f"enc_{size}MiB.age"


def identity_path(data_dir: Path) -> Path:
    return data_dir / "identity.txt"


def load_identity(data_dir: Path) -> pyrage.x25519.Identity:
    return pyrage.x25519.Identity.from_str(identity_path(data_dir).read_text().strip())


def sha256(path: Path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        while chunk := f.read(MIB):
            h.update(chunk)
    return h.hexdigest()


def parse_ints(value: str) -> list[int]:
    return [int(v) for v in value.split(",") if v]


def parse_blocks(value: str) -> list[int]:
    return [SIM_8K if v.lower() in ("8k", "0") else int(v) for v in value.split(",") if v]


def blocks_label(blocks: int) -> str:
    return "8K*" if blocks == SIM_8K else str(blocks)


def buffer_kib(blocks: int) -> int:
    if blocks == SIM_8K:
        return SIM_8K_BYTES // 1024
    return blocks * pyrage.DEFAULT_PYRAGE_BLOCKSIZE // 1024


def io_bytes(data_dir: Path, op: str, size: int) -> tuple[int, int]:
    """(bytes read, bytes written) for one call."""
    plain = plain_path(data_dir, size).stat().st_size
    enc = enc_path(data_dir, size).stat().st_size
    return (plain, enc) if op == "encrypt" else (enc, plain)


# --------------------------------------------------------------------------
# prepare: create plaintext files, encrypt each once, verify one round-trip
# --------------------------------------------------------------------------


def cmd_prepare(args: argparse.Namespace) -> None:
    data_dir: Path = args.data_dir
    data_dir.mkdir(parents=True, exist_ok=True)

    if args.force or not identity_path(data_dir).exists():
        identity_path(data_dir).write_text(str(pyrage.x25519.Identity.generate()) + "\n")
    identity = load_identity(data_dir)
    recipient = identity.to_public()

    for size in args.sizes:
        plain, enc = plain_path(data_dir, size), enc_path(data_dir, size)
        if args.force or not plain.exists() or plain.stat().st_size != size * MIB:
            with open(plain, "wb") as f:
                for _ in range(size):
                    f.write(os.urandom(MIB))
            enc.unlink(missing_ok=True)

        if not enc.exists():
            with open(plain, "rb") as r, open(enc, "wb") as w:
                pyrage.encrypt_io(r, w, [recipient])

        roundtrip = data_dir / f"roundtrip_{size}MiB.bin"
        with open(enc, "rb") as r, open(roundtrip, "wb") as w:
            pyrage.decrypt_io(r, w, [identity])
        ok = sha256(roundtrip) == sha256(plain)
        roundtrip.unlink()
        if not ok:
            sys.exit(f"round-trip mismatch for {size} MiB")
        print(f"{size:>4} MiB  plain={plain.name}  enc={enc.name} "
              f"({enc.stat().st_size} B)  round-trip OK")


# --------------------------------------------------------------------------
# _case: child process, runs one (op, size, blocks) case in an event loop
# --------------------------------------------------------------------------


def rss_bytes() -> int:
    with open("/proc/self/statm") as f:
        return int(f.read().split()[1]) * PAGE_SIZE


def reset_peak_rss() -> bool:
    # Writing "5" to clear_refs resets VmHWM (Linux >= 4.0).
    try:
        with open("/proc/self/clear_refs", "w") as f:
            f.write("5")
        return True
    except OSError:
        return False


def peak_rss_bytes() -> int:
    with open("/proc/self/status") as f:
        for line in f:
            if line.startswith("VmHWM:"):
                return int(line.split()[1]) * 1024
    raise RuntimeError("VmHWM not found")


async def heartbeat(interval: float, lags: list[float], stop: asyncio.Event) -> None:
    """Sleeps `interval` repeatedly and records how late each wakeup was."""
    while not stop.is_set():
        t0 = time.perf_counter()
        await asyncio.sleep(interval)
        lags.append(time.perf_counter() - t0 - interval)


def percentile(sorted_values: list[float], q: float) -> float:
    if not sorted_values:
        return 0.0
    idx = min(len(sorted_values) - 1, max(0, round(q * (len(sorted_values) - 1))))
    return sorted_values[idx]


def lag_stats(lags: list[float], slack: float) -> dict:
    """
    `slack` is the idle loop's p99 wakeup lag (timer granularity: uvloop
    timers are 1 ms). Lag beyond it is time the loop was actually stalled.
    """
    s = sorted(lags)
    excess = [max(0.0, lag - slack) for lag in s]
    return {
        "ticks": len(s),
        "lag_max_ms": (s[-1] if s else 0.0) * 1e3,
        "lag_p99_ms": percentile(s, 0.99) * 1e3,
        "lag_mean_ms": (statistics.fmean(s) if s else 0.0) * 1e3,
        "stall_max_ms": (excess[-1] if excess else 0.0) * 1e3,
        "stall_p99_ms": percentile(excess, 0.99) * 1e3,
        "stalled_ms": sum(excess) * 1e3,
    }


class Capped8K:
    """
    Simulates `encrypt_io`/`decrypt_io` with std's default 8 KiB
    BufReader/BufWriter (the behaviour before `buffer_blocks`), which the API
    no longer allows: pyo3-file honours short reads/writes, so capping each
    Python-level call at 8 KiB under buffer_blocks=1 yields the same number of
    GIL round-trips. Only decrypt caps writes: on encrypt, age writes whole
    64 KiB+16 B chunks, which bypass an 8 KiB BufWriter just like a 64 KiB one.
    The extra Python call per read/write is included in the measurement.
    """

    def __init__(self, f, cap_writes: bool):
        read = f.read
        self.read = lambda n=-1: read(SIM_8K_BYTES if n < 0 or n > SIM_8K_BYTES else n)
        if cap_writes:
            write = f.write
            self.write = lambda b: write(b if len(b) <= SIM_8K_BYTES
                                         else memoryview(b)[:SIM_8K_BYTES])
        else:
            self.write = f.write
        self.flush = f.flush


async def run_case(args: argparse.Namespace) -> dict:
    data_dir: Path = args.data_dir
    identity = load_identity(data_dir)
    recipient = identity.to_public()
    interval = args.heartbeat_ms / 1e3
    loop = asyncio.get_running_loop()
    executor = ThreadPoolExecutor(max_workers=1)

    sim = args.blocks == SIM_8K
    blocks = 1 if sim else args.blocks
    if args.op == "encrypt":
        src = plain_path(data_dir, args.size)
        def call(r, w):
            pyrage.encrypt_io(r, w, [recipient], buffer_blocks=blocks)
    else:
        src = enc_path(data_dir, args.size)
        def call(r, w):
            pyrage.decrypt_io(r, w, [identity], buffer_blocks=blocks)

    def work(r, w):
        if sim:
            r, w = Capped8K(r, False), Capped8K(w, args.op == "decrypt")
        call(r, w)

    if args.sink == "null":
        dst = Path(os.devnull)
    else:
        dst = data_dir / f"out_{os.getpid()}.tmp"

    # Idle baseline: timer slack of this loop with nothing else going on.
    idle_lags: list[float] = []
    stop = asyncio.Event()
    hb = asyncio.create_task(heartbeat(interval, idle_lags, stop))
    await asyncio.sleep(0.25)
    stop.set()
    await hb
    idle = lag_stats(idle_lags, 0.0)
    slack = idle["lag_p99_ms"] / 1e3

    # Run 0 is the first call in this fresh process: glibc keeps freed buffers
    # around afterwards, so only this run shows the operation's real memory
    # footprint. The remaining (warm) runs are used for timing and stalls.
    runs = []
    try:
        for i in range(1 + args.repeat):
            lags: list[float] = []
            stop = asyncio.Event()
            with open(src, "rb") as r, open(dst, "wb") as w:
                hb = asyncio.create_task(heartbeat(interval, lags, stop))
                await asyncio.sleep(0)  # let the heartbeat arm its first timer
                hwm_reset = reset_peak_rss()
                rss0 = rss_bytes()
                ru0 = resource.getrusage(resource.RUSAGE_SELF)
                t0 = time.monotonic()
                p0 = time.perf_counter()
                if args.mode == "thread":
                    await loop.run_in_executor(executor, work, r, w)
                else:
                    work(r, w)
                wall = time.perf_counter() - p0
                t1 = time.monotonic()
                ru1 = resource.getrusage(resource.RUSAGE_SELF)
                peak = peak_rss_bytes() if hwm_reset else None
                stop.set()
                await hb
            runs.append({
                "cold": i == 0,
                "t_start": t0,
                "t_end": t1,
                "wall_s": wall,
                "cpu_user_s": ru1.ru_utime - ru0.ru_utime,
                "cpu_sys_s": ru1.ru_stime - ru0.ru_stime,
                "rss_before": rss0,
                "rss_peak": peak,
                **lag_stats(lags, slack),
            })
    finally:
        executor.shutdown()
        if args.sink != "null":
            dst.unlink(missing_ok=True)

    return {
        "idle": idle,
        "runs": runs,
        "ru_maxrss": resource.getrusage(resource.RUSAGE_SELF).ru_maxrss * 1024,
    }


def cmd_case(args: argparse.Namespace) -> None:
    if args.loop == "uvloop":
        import uvloop
        result = uvloop.run(run_case(args))
    else:
        result = asyncio.run(run_case(args))
    sys.stdout.write(json.dumps(result) + "\n")


# --------------------------------------------------------------------------
# run: parent orchestrator
# --------------------------------------------------------------------------


class RssSampler(threading.Thread):
    """Samples another process's RSS from /proc/<pid>/statm."""

    def __init__(self, pid: int, interval: float):
        super().__init__(daemon=True)
        self.path = f"/proc/{pid}/statm"
        self.interval = interval
        self.samples: list[tuple[float, int]] = []
        self.done = threading.Event()

    def run(self) -> None:
        try:
            with open(self.path) as f:
                while not self.done.is_set():
                    f.seek(0)
                    rss = int(f.read().split()[1]) * PAGE_SIZE
                    self.samples.append((time.monotonic(), rss))
                    time.sleep(self.interval)
        except (OSError, ValueError, IndexError):
            pass  # child exited


def run_child(args: argparse.Namespace, op: str, size: int, blocks: int) -> dict:
    cmd = [
        sys.executable, str(Path(__file__).resolve()),
        "--data-dir", str(args.data_dir), "_case",
        "--op", op, "--size", str(size), "--blocks", str(blocks),
        "--loop", args.loop, "--mode", args.mode, "--sink", args.sink,
        "--repeat", str(args.repeat),
        "--heartbeat-ms", str(args.heartbeat_ms),
    ]
    proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, text=True)
    sampler = RssSampler(proc.pid, args.sample_ms / 1e3)
    sampler.start()
    out, _ = proc.communicate()
    sampler.done.set()
    sampler.join()
    if proc.returncode != 0:
        sys.exit(f"case {op}/{size}/{blocks_label(blocks)} failed "
                 f"with exit code {proc.returncode}")
    result = json.loads(out.strip().splitlines()[-1])

    for run in result["runs"]:
        window = [rss for t, rss in sampler.samples if run["t_start"] <= t <= run["t_end"]]
        run["rss_samples"] = len(window)
        base = run["rss_before"]
        run["rss_avg_delta"] = (statistics.fmean(window) - base) if window else None
        sampled_peak = max(window) if window else base
        peak = max(sampled_peak, run["rss_peak"] or 0)
        run["rss_peak_delta"] = peak - base
    return result


def summarize(op: str, size: int, blocks: int, results: list[dict],
              in_bytes: int, out_bytes: int) -> dict:
    all_runs = [r for res in results for r in res["runs"]]
    runs = [r for r in all_runs if not r["cold"]]
    cold = [r for r in all_runs if r["cold"]]
    walls = [r["wall_s"] for r in runs]
    wall = statistics.median(walls)
    avg_deltas = [r["rss_avg_delta"] for r in cold if r["rss_avg_delta"] is not None]
    cpu = statistics.median((r["cpu_user_s"] + r["cpu_sys_s"]) / r["wall_s"] for r in runs)
    return {
        "op": op,
        "size_mib": size,
        "buffer_blocks": blocks,
        "buffer_kib": buffer_kib(blocks),
        "in_bytes": in_bytes,
        "out_bytes": out_bytes,
        "runs": len(runs),
        "wall_ms_median": wall * 1e3,
        "wall_ms_min": min(walls) * 1e3,
        "wall_ms_stdev": (statistics.stdev(walls) if len(walls) > 1 else 0.0) * 1e3,
        "throughput_mib_s": size / wall,
        "in_mib_s": in_bytes / MIB / wall,
        "out_mib_s": out_bytes / MIB / wall,
        "cpu_util": cpu,
        "lag_max_ms": max(r["lag_max_ms"] for r in runs),
        "lag_p99_ms": statistics.median(r["lag_p99_ms"] for r in runs),
        "lag_mean_ms": statistics.median(r["lag_mean_ms"] for r in runs),
        "stall_max_ms": max(r["stall_max_ms"] for r in runs),
        "stall_p99_ms": statistics.median(r["stall_p99_ms"] for r in runs),
        "stalled_ms": statistics.median(r["stalled_ms"] for r in runs),
        "stalled_pct": statistics.median(r["stalled_ms"] / 1e3 / r["wall_s"] for r in runs),
        "idle_lag_p99_ms": statistics.median(res["idle"]["lag_p99_ms"] for res in results),
        "rss_peak_delta_mib": max(r["rss_peak_delta"] for r in cold) / MIB,
        "rss_avg_delta_mib": (statistics.median(avg_deltas) / MIB) if avg_deltas else None,
        "process_peak_rss_mib": max(res["ru_maxrss"] for res in results) / MIB,
    }


COLUMNS = [
    ("op", "op", "{}"),
    ("size_mib", "MiB", "{}"),
    ("buffer_blocks", "blk", blocks_label),
    ("buffer_kib", "buf KiB", "{}"),
    ("wall_ms_median", "wall ms", "{:.1f}"),
    ("wall_ms_stdev", "±", "{:.1f}"),
    ("throughput_mib_s", "MiB/s", "{:.0f}"),
    ("cpu_util", "CPU", "{:.0%}"),
    ("lag_max_ms", "lag max", "{:.2f}"),
    ("lag_p99_ms", "lag p99", "{:.2f}"),
    ("stall_max_ms", "stall max", "{:.2f}"),
    ("stall_p99_ms", "stall p99", "{:.2f}"),
    ("stalled_ms", "stalled ms", "{:.1f}"),
    ("stalled_pct", "stalled", "{:.1%}"),
    ("rss_peak_delta_mib", "peak ΔRSS", "{:.1f}"),
    ("rss_avg_delta_mib", "avg ΔRSS", "{:.1f}"),
]


def fmt_row(row: dict) -> list[str]:
    return ["-" if row[k] is None else f(row[k]) if callable(f) else f.format(row[k])
            for k, _, f in COLUMNS]


def render(header: list[str], cells: list[list[str]], group: list | None = None,
           markdown: bool = False) -> str:
    """Renders a right-aligned text table, or a Markdown table."""
    if markdown:
        lines = ["| " + " | ".join(header) + " |", "|" + "---|" * len(header)]
        lines += ["| " + " | ".join(c) + " |" for c in cells]
        return "\n".join(lines)
    widths = [max(len(h), *(len(c[i]) for c in cells)) for i, h in enumerate(header)]
    lines = ["  ".join(h.rjust(w) for h, w in zip(header, widths)),
             "  ".join("-" * w for w in widths)]
    for i, c in enumerate(cells):
        if group and i and group[i] != group[i - 1]:
            lines.append("")
        lines.append("  ".join(v.rjust(w) for v, w in zip(c, widths)))
    return "\n".join(lines)


def full_table(rows: list[dict], markdown: bool = False) -> str:
    return render([h for _, h, _ in COLUMNS], [fmt_row(r) for r in rows],
                  [(r["op"], r["size_mib"]) for r in rows], markdown)


def groups(rows: list[dict]):
    """Yields (size, op, rows) per file size and operation."""
    for size in sorted({r["size_mib"] for r in rows}):
        for op in ("encrypt", "decrypt"):
            case = [r for r in rows if r["size_mib"] == size and r["op"] == op]
            if case:
                yield size, op, case


def winners_table(rows: list[dict], markdown: bool = False) -> str:
    """
    Per (size, op): best and worst buffer_blocks by median wall time, plus the
    simulated 8K* baseline, which is always shown.
    """
    header = ["MiB", "op",
              "best", "wall ms", "in / out MiB/s", "stall max",
              "worst", "wall ms", "in / out MiB/s", "stall max",
              "8K* wall ms", "8K* in / out MiB/s", "8K* stall max",
              "spread ±", "worst slower", "8K* slower"]
    cells, group = [], []
    for size, op, case in groups(rows):
        best = min(case, key=lambda r: r["wall_ms_median"])
        worst = max(case, key=lambda r: r["wall_ms_median"])
        sim = next((r for r in case if r["buffer_blocks"] == SIM_8K), None)
        spread = statistics.median(r["wall_ms_stdev"] / r["wall_ms_median"] for r in case)

        def perf(r):
            if r is None:
                return ["-", "-", "-"]
            return [f"{r['wall_ms_median']:.1f}",
                    f"{r['in_mib_s']:.0f} / {r['out_mib_s']:.0f}",
                    f"{r['stall_max_ms']:.2f}"]

        def slower(r):
            return f"{r['wall_ms_median'] / best['wall_ms_median'] - 1:+.0%}" if r else "-"

        cells.append([
            str(size), op,
            blocks_label(best["buffer_blocks"]), *perf(best),
            blocks_label(worst["buffer_blocks"]), *perf(worst),
            *perf(sim),
            f"{spread:.0%}", slower(worst), slower(sim),
        ])
        group.append(size)
    return render(header, cells, group, markdown)


LEGEND = """\
best/worst = buffer_blocks with the lowest/highest median wall time. 8K* =
simulated std 8 KiB buffers (the behaviour before buffer_blocks).
spread = median run-to-run stdev of the group; differences below it are noise.
"X slower" = how much slower X is than best."""


def report(rows: list[dict], full: bool = False, markdown: bool = False) -> str:
    parts = []
    if full:
        parts.append(full_table(rows, markdown))
    parts += [winners_table(rows, markdown), LEGEND]
    return "\n\n".join(parts)


def cmd_report(args: argparse.Namespace) -> None:
    data = json.loads(args.results.with_suffix(".json").read_text())
    rows = data["summary"]
    print(report(rows, full=args.full))
    if args.markdown:
        args.markdown.write_text(report(rows, full=args.full, markdown=True) + "\n")
        print(f"\nwrote {args.markdown}")


def cmd_run(args: argparse.Namespace) -> None:
    for size in args.sizes:
        src = plain_path(args.data_dir, size)
        if not src.exists() or not enc_path(args.data_dir, size).exists():
            sys.exit(f"missing test files for {size} MiB; run `prepare` first")
    if args.loop == "uvloop":
        import uvloop  # noqa: F401  fail early if missing

    total = len(args.ops) * len(args.sizes) * len(args.blocks)
    print(f"pyrage {pyrage.__file__}\nloop={args.loop} mode={args.mode} sink={args.sink} "
          f"procs={args.procs} repeat={args.repeat} heartbeat={args.heartbeat_ms}ms "
          f"rss-sample={args.sample_ms}ms  ({total} cases)\n", file=sys.stderr)

    rows, raw = [], []
    n = 0
    for op in args.ops:
        for size in args.sizes:
            for blocks in args.blocks:
                n += 1
                print(f"[{n}/{total}] {op} {size} MiB buffer_blocks={blocks_label(blocks)}",
                      file=sys.stderr, flush=True)
                results = [run_child(args, op, size, blocks) for _ in range(args.procs)]
                rows.append(summarize(op, size, blocks, results,
                                      *io_bytes(args.data_dir, op, size)))
                raw.append({"op": op, "size_mib": size, "buffer_blocks": blocks,
                            "processes": results})

    print()
    if args.full:
        print(full_table(rows))
    idle = statistics.median(r["idle_lag_p99_ms"] for r in rows)
    print(f"""
wall/MiB/s/CPU/lag/stall: median over {args.procs}x{args.repeat} warm calls.
lag   = how late a {args.heartbeat_ms:g} ms asyncio.sleep() heartbeat woke up (raw).
stall = lag beyond the idle loop's p99 lag ({idle:.3f} ms, timer granularity);
        stalled ms = total stall per call, stalled = that as a share of wall time.
ΔRSS  = RSS above the pre-call baseline (MiB) on the first (cold) call of each
        fresh process; peak = max via VmHWM, avg = mean of external 1 ms samples.
""")
    print(report(rows))

    if args.out:
        args.out.parent.mkdir(parents=True, exist_ok=True)
        meta = {k: (str(v) if isinstance(v, Path) else v) for k, v in vars(args).items()
                if k != "func"}
        args.out.with_suffix(".json").write_text(
            json.dumps({"meta": meta, "summary": rows, "raw": raw}, indent=2))
        with open(args.out.with_suffix(".csv"), "w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=list(rows[0]))
            writer.writeheader()
            writer.writerows(rows)
        print(f"\nwrote {args.out.with_suffix('.json')} and {args.out.with_suffix('.csv')}")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--data-dir", type=Path, default=DEFAULT_DATA_DIR)
    sub = parser.add_subparsers(required=True)

    p = sub.add_parser("prepare", help="create plaintext files and encrypt them once")
    p.add_argument("--sizes", type=parse_ints, default=DEFAULT_SIZES, help="MiB, comma-separated")
    p.add_argument("--force", action="store_true", help="regenerate everything")
    p.set_defaults(func=cmd_prepare)

    def common(p: argparse.ArgumentParser) -> None:
        p.add_argument("--loop", choices=["uvloop", "asyncio"], default="uvloop")
        p.add_argument("--mode", choices=["thread", "inline"], default="thread",
                       help="thread: run_in_executor (default); inline: call on the loop")
        p.add_argument("--sink", choices=["file", "null"], default="file",
                       help="write output to a temp file or /dev/null")
        p.add_argument("--repeat", type=int, default=3,
                       help="warm calls per process (after one cold call)")
        p.add_argument("--heartbeat-ms", type=float, default=1.0)

    p = sub.add_parser("run", help="run the benchmark matrix")
    common(p)
    p.add_argument("--ops", type=lambda v: v.split(","), default=["encrypt", "decrypt"])
    p.add_argument("--sizes", type=parse_ints, default=DEFAULT_SIZES)
    p.add_argument("--blocks", type=parse_blocks, default=DEFAULT_BLOCKS,
                   help="buffer_blocks values; 8k = simulated 8 KiB buffers")
    p.add_argument("--full", action="store_true", help="also print every case")
    p.add_argument("--procs", type=int, default=3, help="fresh processes per case")
    p.add_argument("--sample-ms", type=float, default=1.0, help="RSS sampling interval")
    p.add_argument("--out", type=Path, default=HERE / "results" / "latest",
                   help="writes <out>.json and <out>.csv")
    p.set_defaults(func=cmd_run)

    p = sub.add_parser("report", help="print the best/worst table from saved results")
    p.add_argument("--results", type=Path, default=HERE / "results" / "latest")
    p.add_argument("--full", action="store_true", help="also print every case")
    p.add_argument("--markdown", type=Path, help="also write the tables as Markdown")
    p.set_defaults(func=cmd_report)

    p = sub.add_parser("_case", help=argparse.SUPPRESS)
    common(p)
    p.add_argument("--op", choices=["encrypt", "decrypt"], required=True)
    p.add_argument("--size", type=int, required=True)
    p.add_argument("--blocks", type=int, required=True)
    p.set_defaults(func=cmd_case)

    args = parser.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()
