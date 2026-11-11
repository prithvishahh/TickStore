#!/usr/bin/env python3
"""Compare a TickStore run with Parquet (zstd) and DuckDB.

Reads ``engine.json`` plus the ``TBIN`` files written by ``tickstore bench``.
Checks every aggregate against the reference checksums in that JSON, then
updates the benchmark section of the README.
"""

from __future__ import annotations

import argparse
import json
import struct
import time
from pathlib import Path

import duckdb
import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq

ROOT = Path(__file__).resolve().parents[1]
README = ROOT / "README.md"
DOCS = ROOT / "docs"

TRADE_DTYPE = np.dtype(
    [
        ("symbol_id", "<u2"),
        ("side", "u1"),
        ("pad", "u1"),
        ("size", "<u4"),
        ("ts", "<i8"),
        ("price", "<i8"),
    ],
    align=False,
)
QUOTE_DTYPE = np.dtype(
    [
        ("symbol_id", "<u2"),
        ("pad0", "<u2"),
        ("bid_size", "<u4"),
        ("ask_size", "<u4"),
        ("ts", "<i8"),
        ("bid", "<i8"),
        ("ask", "<i8"),
        ("pad1", "<u4"),
    ],
    align=False,
)


def read_tbin(path: Path, expect_kind: int, dtype: np.dtype):
    blob = path.read_bytes()
    if blob[:4] != b"TBIN":
        raise SystemExit(f"{path} is not a TBIN file")
    version, kind, _flags, scale, nsym, rec, nrows, data_off = struct.unpack_from(
        "<III q I I Q Q", blob, 4
    )
    if version != 1 or kind != expect_kind or rec != dtype.itemsize:
        raise SystemExit(f"{path} header does not match {dtype.itemsize}-byte records")
    off = 48
    symbols = []
    for _ in range(nsym):
        (nlen,) = struct.unpack_from("<H", blob, off)
        off += 2
        symbols.append(blob[off : off + nlen].decode())
        off += nlen
    if data_off % dtype.alignment != 0 and dtype.alignment:
        raise SystemExit(f"record offset {data_off} is not aligned for {dtype}")
    records = np.frombuffer(blob, dtype=dtype, count=nrows, offset=data_off)
    return symbols, int(scale), records.copy()


def arrow_trades(symbols, rec):
    names = np.asarray(symbols, dtype=object)[rec["symbol_id"]]
    return pa.table(
        {
            "ts_ns": pa.array(rec["ts"], type=pa.int64()),
            "symbol": pa.array(names),
            "price_ticks": pa.array(rec["price"], type=pa.int64()),
            "size": pa.array(rec["size"].astype(np.int64), type=pa.int64()),
            "side": pa.array(rec["side"].astype(np.int8), type=pa.int8()),
        }
    )


def arrow_quotes(symbols, rec):
    names = np.asarray(symbols, dtype=object)[rec["symbol_id"]]
    return pa.table(
        {
            "ts_ns": pa.array(rec["ts"], type=pa.int64()),
            "symbol": pa.array(names),
            "bid_ticks": pa.array(rec["bid"], type=pa.int64()),
            "ask_ticks": pa.array(rec["ask"], type=pa.int64()),
            "bid_size": pa.array(rec["bid_size"].astype(np.int64), type=pa.int64()),
            "ask_size": pa.array(rec["ask_size"].astype(np.int64), type=pa.int64()),
        }
    )


def write_parquet(table: pa.Table, path: Path) -> float:
    t0 = time.perf_counter()
    pq.write_table(table, path, compression="zstd", compression_level=3)
    return time.perf_counter() - t0


def parquet_columns(path: Path):
    pf = pq.ParquetFile(path)
    md = pf.metadata
    comp: dict[str, int] = {}
    raw: dict[str, int] = {}
    for rg in range(md.num_row_groups):
        group = md.row_group(rg)
        for i in range(group.num_columns):
            col = group.column(i)
            name = col.path_in_schema
            comp[name] = comp.get(name, 0) + col.total_compressed_size
            raw[name] = raw.get(name, 0) + col.total_uncompressed_size
    return comp, raw


def median(samples: list[float]) -> float:
    ordered = sorted(samples)
    return ordered[len(ordered) // 2]


def time_call(fn, repeats: int):
    fn()
    samples = []
    value = None
    for _ in range(repeats):
        t0 = time.perf_counter()
        value = fn()
        samples.append((time.perf_counter() - t0) * 1e3)
    return median(samples), value


def as_int(value) -> int:
    if value is None:
        return 0
    return int(value)


def check(label: str, got, expect) -> None:
    if as_int(got) != as_int(expect):
        raise SystemExit(f"{label}: got {got} expected {expect}")


def sql_ohlcv(table: str) -> str:
    return f"""
        SELECT count(*)::BIGINT,
               coalesce(sum(volume), 0)::BIGINT,
               coalesce(sum(open), 0)::HUGEINT,
               coalesce(sum(close), 0)::HUGEINT
        FROM (
            SELECT (ts_ns // ?) * ? AS bucket,
                   arg_min(price_ticks, ts_ns) AS open,
                   arg_max(price_ticks, ts_ns) AS close,
                   sum(size)::BIGINT AS volume
            FROM {table}
            WHERE symbol = ? AND ts_ns >= ? AND ts_ns < ?
            GROUP BY 1
        )
    """


def sql_asof(trades: str, quotes: str) -> str:
    return f"""
        SELECT count(*)::BIGINT,
               (count(*) - count(q.bid_ticks))::BIGINT,
               coalesce(sum(q.bid_ticks::HUGEINT), 0)::HUGEINT
        FROM {trades} t
        ASOF LEFT JOIN {quotes} q
          ON t.symbol = q.symbol AND t.ts_ns >= q.ts_ns
        WHERE t.symbol = ? AND t.ts_ns >= ? AND t.ts_ns < ?
    """


def run_queries(con, trades: str, quotes: str, ds: dict, ref: dict, repeats: int):
    symbol = ds["focus_symbol"]
    full = (ds["session_start_ns"], ds["data_end_ns"])
    window = (ds["range_start_ns"], ds["range_end_ns"])
    out = {}

    def count(lo, hi):
        return con.execute(
            f"SELECT count(*)::BIGINT, coalesce(sum(price_ticks::HUGEINT), 0)::HUGEINT, "
            f"coalesce(sum(size), 0)::BIGINT FROM {trades} "
            f"WHERE symbol = ? AND ts_ns >= ? AND ts_ns < ?",
            [symbol, lo, hi],
        ).fetchone()

    def one(key, fn, expect):
        med, value = time_call(fn, repeats)
        for got, exp in zip(value, expect):
            check(key, got, exp)
        out[key] = {"median_ms": med}

    one("count_range", lambda: count(*window), (ref["range_count"], ref["range_price_sum"], ref["range_size_sum"]))
    one("scan_full", lambda: count(*full), (ref["full_count"], ref["full_price_sum"], ref["full_size_sum"]))

    def scan_window():
        rows = con.execute(
            f"SELECT ts_ns, price_ticks, size, side FROM {trades} "
            f"WHERE symbol = ? AND ts_ns >= ? AND ts_ns < ?",
            [symbol, *window],
        ).fetchall()
        return (len(rows), sum(r[1] for r in rows), sum(r[2] for r in rows))

    one(
        "scan_range",
        scan_window,
        (ref["range_count"], ref["range_price_sum"], ref["range_size_sum"]),
    )
    one(
        "vwap",
        lambda: con.execute(
            f"SELECT count(*)::BIGINT, coalesce(sum(price_ticks::HUGEINT * size::HUGEINT), 0)::HUGEINT, "
            f"coalesce(sum(size), 0)::BIGINT FROM {trades} "
            f"WHERE symbol = ? AND ts_ns >= ? AND ts_ns < ?",
            [symbol, *full],
        ).fetchone(),
        (ref["vwap_trades"], ref["vwap_notional"], ref["vwap_volume"]),
    )
    for key, interval, checksum in (
        ("ohlcv_1m", ds["interval_1m_ns"], ref["ohlcv_1m"]),
        ("ohlcv_1s", ds["interval_1s_ns"], ref["ohlcv_1s"]),
    ):
        one(
            key,
            lambda interval=interval: con.execute(
                sql_ohlcv(trades),
                [interval, interval, symbol, *full],
            ).fetchone(),
            (checksum["bars"], checksum["volume"], checksum["open_sum"], checksum["close_sum"]),
        )
    for key, bounds, checksum in (
        ("asof_full", full, ref["asof_full"]),
        ("asof_range", window, ref["asof_range"]),
    ):
        one(
            key,
            lambda bounds=bounds: con.execute(sql_asof(trades, quotes), [symbol, *bounds]).fetchone(),
            (checksum["rows"], checksum["nulls"], checksum["bid_sum"]),
        )
    return out


def svg_bars(path: Path, title: str, labels: list[str], series: list[tuple[str, str, list[float]]], ylabel: str, log: bool):
    width, height = 880, 460
    left, right, top, bottom = 64, 24, 48, 86
    plot_w = width - left - right
    plot_h = height - top - bottom
    values = [v for _, _, row in series for v in row if v > 0]
    if not values:
        values = [1.0]
    if log:
        log_min = float(np.log10(min(values)))
        log_max = float(np.log10(max(values)))
        pad = max((log_max - log_min) * 0.08, 0.15)
        floor = 10 ** (log_min - pad)
        ceil = 10 ** (log_max + pad * 0.25)
        def y_of(v: float) -> float:
            v = min(max(v, floor), ceil)
            span = np.log10(ceil) - np.log10(floor)
            return top + plot_h - (np.log10(v) - np.log10(floor)) / span * plot_h
    else:
        ceil = max(values) * 1.08
        def y_of(v: float) -> float:
            return top + plot_h - (v / ceil) * plot_h

    n = len(labels)
    group_w = plot_w / max(n, 1)
    bar_w = group_w / (len(series) + 1)
    parts = [
        f'<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="{height}" viewBox="0 0 {width} {height}">',
        '<rect width="100%" height="100%" fill="#f7f4ee"/>',
        f'<text x="{left}" y="30" font-family="Georgia, serif" font-size="20" fill="#1d1a16">{escape(title)}</text>',
        f'<text x="18" y="{top + plot_h / 2}" transform="rotate(-90 18,{top + plot_h / 2})" '
        f'font-family="ui-sans-serif, sans-serif" font-size="12" fill="#5c564e">{escape(ylabel)}</text>',
        f'<line x1="{left}" y1="{top + plot_h}" x2="{left + plot_w}" y2="{top + plot_h}" stroke="#1d1a16" stroke-width="1"/>',
    ]
    for i, label in enumerate(labels):
        x = left + i * group_w + group_w / 2
        parts.append(
            f'<text x="{x}" y="{top + plot_h + 18}" text-anchor="middle" '
            f'font-family="ui-sans-serif, sans-serif" font-size="11" fill="#1d1a16">{escape(label)}</text>'
        )
    for s_i, (name, color, row) in enumerate(series):
        for i, value in enumerate(row):
            if value <= 0:
                continue
            x = left + i * group_w + bar_w * (s_i + 0.5)
            y = y_of(value)
            h = top + plot_h - y
            parts.append(
                f'<rect x="{x:.1f}" y="{y:.1f}" width="{bar_w * 0.82:.1f}" height="{max(h, 0):.1f}" '
                f'fill="{color}" rx="2"/>'
            )
        lx = left + s_i * 150
        parts.append(f'<rect x="{lx}" y="{height - 28}" width="12" height="12" fill="{color}"/>')
        parts.append(
            f'<text x="{lx + 18}" y="{height - 18}" font-family="ui-sans-serif, sans-serif" '
            f'font-size="12" fill="#1d1a16">{escape(name)}</text>'
        )
    parts.append("</svg>")
    path.write_text("\n".join(parts))


def escape(text: str) -> str:
    return (
        text.replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
    )


def fmt_bytes(n: float) -> str:
    if n < 1024:
        return f"{n:.0f} B"
    if n < 1024**2:
        return f"{n / 1024:.1f} KiB"
    if n < 1024**3:
        return f"{n / 1024**2:.2f} MiB"
    return f"{n / 1024**3:.2f} GiB"


def fmt_ms(v: float) -> str:
    if v < 0.1:
        return f"{v:.3f}"
    if v < 10:
        return f"{v:.2f}"
    return f"{v:.1f}"


def per_row(num: float, rows: int) -> float:
    return num / rows if rows else 0.0


def headline(engine: dict, extra: dict) -> str:
    ds = engine["dataset"]
    stats = engine["tickstore"]["stats"]
    trades = ds["trade_rows"]
    quotes = ds["quote_rows"]
    ts_trade = stats["trade_file_bytes"] / trades
    ts_quote = stats["quote_file_bytes"] / quotes
    pq_trade = extra["parquet_trade_nosym_bytes"] / trades
    pq_quote = extra["parquet_quote_nosym_bytes"] / quotes
    csv_trade = engine["csv"]["trade_bytes"] / trades
    csv_quote = engine["csv"]["quote_bytes"] / quotes
    total_pq = extra["parquet_trade_nosym_bytes"] + extra["parquet_quote_nosym_bytes"]
    total_csv = engine["csv"]["trade_bytes"] + engine["csv"]["quote_bytes"]
    cols = stats["columns"]
    ts_b = cols["trade.ts"]["payload_bytes"] / cols["trade.ts"]["rows"]
    pq_ts = extra["parquet_trade_nosym_columns"].get("ts_ns", 0) / trades
    q = engine["tickstore"]["queries"]
    dk = extra["duckdb_queries"]
    return (
        f"**Headline.** The directory is {fmt_bytes(engine['tickstore']['disk_bytes'])} versus "
        f"{fmt_bytes(total_pq)} of Parquet zstd (no symbol column) and {fmt_bytes(total_csv)} of CSV. "
        f"Quotes are {ts_quote:.2f} B/row versus Parquet's {pq_quote:.2f}, because the selector wraps those "
        f"columns in a zstd frame. Trade timestamps, prices, and sizes stay bare so VWAP and OHLCV do not "
        f"inflate them on every query ({ts_trade:.2f} B/trade, next to Parquet's {pq_trade:.2f}; "
        f"timestamps alone are {ts_b:.2f} B versus Parquet's {pq_ts:.2f}). "
        f"Session VWAP is {q['vwap']['median_ms']:.2f} ms versus DuckDB's {dk['vwap']['median_ms']:.2f} ms. "
        f"1-second bars are {q['ohlcv_1s']['median_ms']:.2f} ms versus {dk['ohlcv_1s']['median_ms']:.2f} ms, "
        f"and 1-minute bars are {q['ohlcv_1m']['median_ms']:.2f} ms versus {dk['ohlcv_1m']['median_ms']:.2f} ms. "
        f"Ingest is {engine['tickstore']['rows_per_s']/1e6:.2f}M rows/s. That is slower than milestone 1, "
        f"because each append is also appended to the write-ahead log."
    )


def build_markdown(engine: dict, extra: dict) -> str:
    ds = engine["dataset"]
    ref_rows_t = ds["trade_rows"]
    ref_rows_q = ds["quote_rows"]
    packed_t = engine["packed"]["trade_bytes"]
    packed_q = engine["packed"]["quote_bytes"]
    csv_t = engine["csv"]["trade_bytes"]
    csv_q = engine["csv"]["quote_bytes"]
    ts_stats = engine["tickstore"]["stats"]
    ts_trade = ts_stats["trade_file_bytes"]
    ts_quote = ts_stats["quote_file_bytes"]
    ts_total = engine["tickstore"]["disk_bytes"]
    pq_t = extra["parquet_trade_bytes"]
    pq_q = extra["parquet_quote_bytes"]
    pq_t_nosym = extra["parquet_trade_nosym_bytes"]
    pq_q_nosym = extra["parquet_quote_nosym_bytes"]
    duck = extra["duckdb_bytes"]

    def ratio(raw, packed):
        return raw / packed if packed else 0.0

    lines = []
    lines.append(headline(engine, extra))
    lines.append("")
    lines.append(
        f"Measured on {engine['machine']['cpu']} ({engine['machine']['arch']}, "
        f"{engine['machine']['rustc']}, PyArrow {extra['pyarrow']}, DuckDB {extra['duckdb']}). "
        f"Dataset: {ref_rows_t:,} trades and {ref_rows_q:,} quotes, {len(ds['symbols'])} symbols, "
        f"seed {ds['seed']}, one US cash session, chunk size {ds['chunk_rows']:,}. "
        f"Focus symbol for queries: **{ds['focus_symbol']}** "
        f"({ds['focus_trade_rows']:,} trades, {ds['focus_quote_rows']:,} quotes). "
        "Ingest and query times are in-process medians and do not include process startup. "
        "The 10-minute window starts five minutes after the open."
    )
    lines.append("")
    lines.append("### Size")
    lines.append("")
    lines.append("| Representation | Trades | B/trade | vs packed | Quotes | B/quote | vs packed |")
    lines.append("| --- | ---: | ---: | ---: | ---: | ---: | ---: |")
    rows = [
        ("Packed binary (no symbol)", packed_t, ref_rows_t, packed_q, ref_rows_q),
        ("CSV", csv_t, ref_rows_t, csv_q, ref_rows_q),
        ("Parquet zstd, no symbol", pq_t_nosym, ref_rows_t, pq_q_nosym, ref_rows_q),
        ("Parquet zstd, with symbol", pq_t, ref_rows_t, pq_q, ref_rows_q),
        ("TickStore files", ts_trade, ref_rows_t, ts_quote, ref_rows_q),
    ]
    for name, tb, tr, qb, qr in rows:
        lines.append(
            f"| {name} | {fmt_bytes(tb)} | {per_row(tb, tr):.2f} | {ratio(packed_t, tb):.2f}× | "
            f"{fmt_bytes(qb)} | {per_row(qb, qr):.2f} | {ratio(packed_q, qb):.2f}× |"
        )
    lines.append("")
    lines.append(
        f"TickStore total directory (both streams plus catalog) is {fmt_bytes(ts_total)}. "
        f"The DuckDB file holding both tables is {fmt_bytes(duck)}. "
        "“vs packed” is the packed record size divided by the file size, so a number above 1 is smaller than the packed array "
        "(24 bytes per trade, 40 bytes per quote)."
    )
    lines.append("")
    lines.append("### Per-column payload, trades")
    lines.append("")
    lines.append("| Column | TickStore payload | B/value | Codecs chosen | Parquet zstd | Parquet raw |")
    lines.append("| --- | ---: | ---: | --- | ---: | ---: |")
    mapping = [
        ("trade.ts", "ts", "ts_ns"),
        ("trade.price", "price", "price_ticks"),
        ("trade.size", "size", "size"),
        ("trade.side", "side", "side"),
    ]
    cols = ts_stats["columns"]
    pq_comp = extra["parquet_trade_nosym_columns"]
    pq_raw = extra["parquet_trade_nosym_raw"]
    for key, _short, pq_name in mapping:
        col = cols[key]
        codecs = ", ".join(
            f"{name} ×{use['chunks']}" for name, use in col["codecs"].items()
        )
        bpr = per_row(col["payload_bytes"], col["rows"])
        lines.append(
            f"| `{key.split('.', 1)[1]}` | {fmt_bytes(col['payload_bytes'])} | {bpr:.2f} | {codecs} | "
            f"{fmt_bytes(pq_comp.get(pq_name, 0))} | {fmt_bytes(pq_raw.get(pq_name, 0))} |"
        )
    lines.append("")
    lines.append(
        "TickStore figures are encoded column payloads. Parquet figures are row-group column "
        "chunks from the no-symbol file, so the symbol dictionary is not in either side. "
        "File headers sit outside this table; they are included in the size table above."
    )
    lines.append("")
    lines.append("### Ingest")
    lines.append("")
    lines.append("| System | What is timed | Rows/s |")
    lines.append("| --- | --- | ---: |")
    lines.append(
        f"| TickStore | encode + write from memory, median of {len(engine['tickstore']['ingest_samples_s'])} | "
        f"{engine['tickstore']['rows_per_s']:,.0f} |"
    )
    lines.append(
        f"| CSV | text write from memory | {engine['csv']['rows_per_s']:,.0f} |"
    )
    lines.append(
        f"| Parquet zstd | PyArrow `write_table` from Arrow, both streams | {extra['parquet_rows_per_s']:,.0f} |"
    )
    lines.append(
        f"| DuckDB | `CREATE TABLE AS SELECT` from Arrow, both streams | {extra['duckdb_rows_per_s']:,.0f} |"
    )
    lines.append("")
    lines.append("### Query latency (median ms, warm)")
    lines.append("")
    q_labels = [
        ("count_range", "10-min count"),
        ("scan_range", "10-min scan"),
        ("ohlcv_1m", "1-min bars"),
        ("ohlcv_1s", "1-sec bars"),
        ("vwap", "session VWAP"),
        ("asof_range", "10-min as-of"),
        ("asof_full", "session as-of"),
    ]
    lines.append("| Query | TickStore | CSV rescan | DuckDB table | DuckDB on Parquet |")
    lines.append("| --- | ---: | ---: | ---: | ---: |")
    ts_q = engine["tickstore"]["queries"]
    csv_q = engine["csv"]["queries"]
    dk_q = extra["duckdb_queries"]
    pq_q = extra["parquet_queries"]
    for key, label in q_labels:
        def cell(block, key=key):
            if key not in block:
                return "—"
            return fmt_ms(block[key]["median_ms"])
        lines.append(
            f"| {label} | {cell(ts_q)} | {cell(csv_q)} | {cell(dk_q)} | {cell(pq_q)} |"
        )
    lines.append("")
    lines.append(
        "Count uses TickStore's min/max index: a chunk that sits entirely inside the window "
        "contributes its row count without decoding. The scan, bars, VWAP, and as-of queries "
        "decode the columns they need. CSV rescans the text file on every call. "
        "DuckDB-on-Parquet runs the same SQL against `read_parquet`. "
        "Every cell was checked against the same integer checksums (bar count, open/close sums, "
        "VWAP notional, as-of bid sum)."
    )
    lines.append("")
    lines.append("### Codec microbench on the focus symbol")
    lines.append("")
    lines.append("Bytes per value if that codec is used for the whole column, before the 5% selector band.")
    lines.append("")
    lines.append("| Codec | Timestamps | Prices | Sizes |")
    lines.append("| --- | ---: | ---: | ---: |")
    by_name = {}
    for kind, rows in (
        ("timestamps", engine["codecs"]["timestamps"]),
        ("prices", engine["codecs"]["prices"]),
        ("sizes", engine["codecs"]["sizes"]),
    ):
        for row in rows:
            by_name.setdefault(row["name"], {})[kind] = row
    names = []
    for row in engine["codecs"]["timestamps"]:
        if row["name"] != "selected":
            names.append(row["name"])
    for row in engine["codecs"]["sizes"]:
        if row["name"] not in names and row["name"] != "selected":
            names.append(row["name"])
    for name in names:
        def bpv(kind, name=name):
            row = by_name.get(name, {}).get(kind)
            if not row:
                return "—"
            return f"{row['bytes_per_value']:.2f}"
        lines.append(f"| `{name}` | {bpv('timestamps')} | {bpv('prices')} | {bpv('sizes')} |")
    xor = engine["codecs"]["price_f64_xor"]
    lines.append("")
    lines.append(
        f"Gorilla XOR on the `f64` view of the same prices is {xor['bytes_per_value']:.2f} bytes/value "
        "and is not a storage candidate: it is exact on float bits, not on the tick grid. "
        f"The on-disk selector stored timestamps as "
        f"**{selected_name(engine['codecs']['timestamps'])}**, prices as "
        f"**{selected_name(engine['codecs']['prices'])}**, sizes as "
        f"**{selected_name(engine['codecs']['sizes'])}** "
        "(whole-column choice; individual chunks can differ, see the payload table)."
    )
    lines.append("")
    lines.append("![Bytes per trade](docs/bench_size.svg)")
    lines.append("")
    lines.append("![Query latency](docs/bench_latency.svg)")
    lines.append("")
    lines.append("![Codec sizes](docs/bench_codecs.svg)")
    lines.append("")
    return "\n".join(lines)


def selected_name(rows: list[dict]) -> str:
    for row in rows:
        if row["name"] == "selected":
            return row.get("codec", "selected")
    return "n/a"


def patch_readme(body: str) -> None:
    text = README.read_text()
    start = "<!-- BENCHMARK_RESULTS_START -->"
    end = "<!-- BENCHMARK_RESULTS_END -->"
    if start not in text or end not in text:
        raise SystemExit("README is missing benchmark markers")
    pre, rest = text.split(start, 1)
    _, post = rest.split(end, 1)
    README.write_text(pre + start + "\n\n" + body + "\n" + end + post)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--workdir", type=Path, default=ROOT / "bench" / "run")
    parser.add_argument("--repeats", type=int, default=5)
    args = parser.parse_args()
    engine = json.loads((args.workdir / "engine.json").read_text())
    ds = engine["dataset"]
    ref = engine["reference"]

    symbols_t, _scale, trades = read_tbin(args.workdir / "trades.bin", 1, TRADE_DTYPE)
    symbols_q, _scale_q, quotes = read_tbin(args.workdir / "quotes.bin", 2, QUOTE_DTYPE)
    if symbols_t != symbols_q:
        raise SystemExit("trade and quote symbol tables differ")
    if symbols_t != ds["symbols"]:
        raise SystemExit("binary symbols do not match engine.json")

    trade_table = arrow_trades(symbols_t, trades)
    quote_table = arrow_quotes(symbols_q, quotes)
    trade_nosym = trade_table.drop(["symbol"])
    quote_nosym = quote_table.drop(["symbol"])

    paths = {
        "trades": args.workdir / "trades.parquet",
        "quotes": args.workdir / "quotes.parquet",
        "trades_nosym": args.workdir / "trades_nosym.parquet",
        "quotes_nosym": args.workdir / "quotes_nosym.parquet",
    }
    write_samples = []
    for _ in range(args.repeats):
        write_samples.append(write_parquet(trade_table, paths["trades"]) + write_parquet(quote_table, paths["quotes"]))
    write_parquet(trade_nosym, paths["trades_nosym"])
    write_parquet(quote_nosym, paths["quotes_nosym"])
    parquet_rows_per_s = (ds["trade_rows"] + ds["quote_rows"]) / median(write_samples)

    comp, raw = parquet_columns(paths["trades_nosym"])

    db_path = args.workdir / "market.duckdb"
    if db_path.exists():
        db_path.unlink()
    load_samples = []
    for i in range(args.repeats):
        path = args.workdir / ("market.duckdb" if i + 1 == args.repeats else "market-tmp.duckdb")
        if path.exists():
            path.unlink()
        con = duckdb.connect(str(path))
        t0 = time.perf_counter()
        con.register("trades_arrow", trade_table)
        con.execute("CREATE TABLE trades AS SELECT * FROM trades_arrow")
        con.unregister("trades_arrow")
        con.register("quotes_arrow", quote_table)
        con.execute("CREATE TABLE quotes AS SELECT * FROM quotes_arrow")
        con.unregister("quotes_arrow")
        load_samples.append(time.perf_counter() - t0)
        if i + 1 != args.repeats:
            con.close()
            path.unlink()
        else:
            kept = con
    duck_rows_per_s = (ds["trade_rows"] + ds["quote_rows"]) / median(load_samples)

    print("checking DuckDB tables")
    duck_queries = run_queries(kept, "trades", "quotes", ds, ref, args.repeats)
    kept.close()

    print("checking DuckDB on Parquet")
    con = duckdb.connect()
    tpath = str(paths["trades"]).replace("'", "''")
    qpath = str(paths["quotes"]).replace("'", "''")
    parquet_queries = run_queries(
        con,
        f"read_parquet('{tpath}')",
        f"read_parquet('{qpath}')",
        ds,
        ref,
        args.repeats,
    )
    con.close()

    extra = {
        "pyarrow": pa.__version__,
        "duckdb": duckdb.__version__,
        "parquet_trade_bytes": paths["trades"].stat().st_size,
        "parquet_quote_bytes": paths["quotes"].stat().st_size,
        "parquet_trade_nosym_bytes": paths["trades_nosym"].stat().st_size,
        "parquet_quote_nosym_bytes": paths["quotes_nosym"].stat().st_size,
        "parquet_trade_nosym_columns": comp,
        "parquet_trade_nosym_raw": raw,
        "duckdb_bytes": db_path.stat().st_size,
        "parquet_rows_per_s": parquet_rows_per_s,
        "duckdb_rows_per_s": duck_rows_per_s,
        "duckdb_queries": duck_queries,
        "parquet_queries": parquet_queries,
        "parquet_write_samples_s": write_samples,
        "duckdb_load_samples_s": load_samples,
    }
    merged = {"engine": engine, "comparison": extra}
    DOCS.mkdir(exist_ok=True)
    (DOCS / "bench_results.json").write_text(json.dumps(merged, indent=2) + "\n")

    trade_rows = ds["trade_rows"]
    svg_bars(
        DOCS / "bench_size.svg",
        "Bytes per trade",
        ["Packed", "CSV", "Parquet", "Parquet+symbol", "TickStore"],
        [
            (
                "bytes / trade",
                "#0f6e56",
                [
                    24,
                    engine["csv"]["trade_bytes"] / trade_rows,
                    extra["parquet_trade_nosym_bytes"] / trade_rows,
                    extra["parquet_trade_bytes"] / trade_rows,
                    engine["tickstore"]["stats"]["trade_file_bytes"] / trade_rows,
                ],
            )
        ],
        "bytes",
        log=False,
    )
    labels = ["10m count", "10m scan", "1s bars", "VWAP", "10m as-of"]
    keys = ["count_range", "scan_range", "ohlcv_1s", "vwap", "asof_range"]

    def series(name, color, block):
        return (name, color, [block[k]["median_ms"] if k in block else 0.0 for k in keys])

    svg_bars(
        DOCS / "bench_latency.svg",
        "Warm query latency",
        labels,
        [
            series("TickStore", "#0f6e56", engine["tickstore"]["queries"]),
            series("CSV", "#8a8175", engine["csv"]["queries"]),
            series("DuckDB", "#2c5aa0", duck_queries),
            series("Parquet", "#c47b2b", parquet_queries),
        ],
        "milliseconds (log)",
        log=True,
    )
    codec_names = [row["name"] for row in engine["codecs"]["timestamps"] if row["name"] != "selected"]
    def codec_series(kind, color):
        rows = {row["name"]: row["bytes_per_value"] for row in engine["codecs"][kind] if "bytes_per_value" in row}
        return (kind, color, [rows.get(name, 0.0) for name in codec_names])
    svg_bars(
        DOCS / "bench_codecs.svg",
        "Codec size on the focus symbol",
        codec_names,
        [
            codec_series("timestamps", "#0f6e56"),
            codec_series("prices", "#c47b2b"),
        ],
        "bytes per value",
        log=False,
    )
    patch_readme(build_markdown(engine, extra))
    print("updated README benchmark section")
    print(
        f"parquet {parquet_rows_per_s:,.0f} rows/s, duckdb load {duck_rows_per_s:,.0f} rows/s, "
        f"trade parquet {extra['parquet_trade_nosym_bytes'] / trade_rows:.2f} B/row"
    )


if __name__ == "__main__":
    main()
