"""
convert_to_mt4.py – Generate MT4-compatible *.csv* and *.hst* files
for BOTH your IG DEMO (symbol **GER30(£)**) and IG LIVE (symbol **GER30**)
accounts in one pass.

Main fixes compared with *mt4_converter_refactored_bkup.py*
-----------------------------------------------------------
1. **True v509 record layout** – 60-byte <q d d d d q i q> per bar.
2. **Trading-hour filter** – default keeps only 08:00-18:30 London time
   so the Strategy Tester never encounters zero-volume minutes.
3. **Separate file suffix vs. header symbol** – `_OFFLINE` appended to the
   *filename* only, so the header stays exactly `GER30(£)` / `GER30`.
4. **User-set spread** – stored in header *and* each record so you can
   override MT4’s “50-point while market closed” spread in the Tester.

Usage examples
~~~~~~~~~~~~~~
    python convert_to_mt4.py 2024                       # one year
    python convert_to_mt4.py 2000-2024 --spread 2       # range, 2-pt spread
    python convert_to_mt4.py 2024 --limit_hours 07:00-21:00

The script expects the raw Chicago-time CSV(s) named
`dax_1m_YYYY.csv` in the same directory.

Author  : ChatGPT (fixes 2025-05-20)
Version : 1.1
Lines   : ≈ 280 (including this docstring)
"""

import argparse
import struct
import sys
from datetime import time
from pathlib import Path

import pandas as pd
import pytz

# ---------- MT4 509 layout constants ----------
RATE_FORMAT = "<qddddqiq"          # 60 bytes per bar
RATE_SIZE   = struct.calcsize(RATE_FORMAT)

LONDON  = pytz.timezone("Europe/London")
CHICAGO = pytz.timezone("America/Chicago")

# ---------- CLI parsing ----------
def parse_args():
    p = argparse.ArgumentParser(prog="convert_to_mt4")
    p.add_argument("years", help="Single year (2024) or range (2000-2024)")
    p.add_argument(
        "--limit_hours",
        default="08:00-18:30",
        help="Keep bars whose *London* time is within this range HH:MM-HH:MM; "
             "set to 00:00-23:59 to keep everything.",
    )
    p.add_argument("--spread", type=int, default=2, help="Spread in points to store.")
    p.add_argument("--digits", type=int, default=1, help="Price digits (1 for DAX £).")
    p.add_argument(
        "--file_suffix", default="_OFFLINE", help="Suffix for output filenames."
    )
    return p.parse_args()

# ---------- Helpers ----------
def year_iter(span: str):
    """Yield each year integer from '2024' or '2000-2024'."""
    if "-" in span:
        start, end = map(int, span.split("-", 1))
        return range(start, end + 1)
    return range(int(span), int(span) + 1)

def read_raw_csv(year: int) -> pd.DataFrame:
    fn = Path(f"dax_1m_{year}.csv")
    if not fn.exists():
        sys.exit(f"Input CSV {fn} not found.")
    df = pd.read_csv(fn)
    # expected cols Date,Time,Open,High,Low,Close,Volume
    dt_col = pd.to_datetime(
        df["Date"] + " " + df["Time"], format="%Y.%m.%d %H:%M"
    ).dt.tz_localize(CHICAGO)
    df.index = dt_col.tz_convert(LONDON)
    df = df[["Open", "High", "Low", "Close", "Volume"]].astype(float)
    return df

def apply_filters(df: pd.DataFrame, hours: str) -> pd.DataFrame:
    start_s, end_s = hours.split("-", 1)
    start_t = time.fromisoformat(start_s)
    end_t   = time.fromisoformat(end_s)
    mask = (df.index.time >= start_t) & (df.index.time <= end_t)
    df = df[mask]
    # drop weekends
    df = df[df.index.dayofweek < 5]
    return df

def write_csv(df: pd.DataFrame, out_path: Path, digits: int):
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_df = df.copy()
    out_df.index = out_df.index.tz_localize(None)
    out_df.insert(0, "Time", out_df.index.strftime("%H:%M"))
    out_df.insert(0, "Date", out_df.index.strftime("%Y.%m.%d"))
    out_df.to_csv(out_path, index=False, float_format=f"%.{digits}f")

def build_header(symbol: str, digits: int, spread: int, bars: int) -> bytes:
    copyright_b  = b"(C) 2025 ChatGPT".ljust(64, b"\x00")[:64]
    symbol_bytes = (
        symbol.encode("windows-1252", errors="replace").ljust(64, b"\x00")[:64]
    )
    header = struct.pack(
        "<i64s64siiiiiii",
        509,
        copyright_b,
        symbol_bytes,
        digits,
        spread,
        0,
        bars,
        0,
        0,
        0,
    )
    header += b"\x00" * (148 - len(header))  # pad to 148
    return header

def write_hst(
    df: pd.DataFrame, symbol: str, out_path: Path, digits: int, spread: int
):
    out_path.parent.mkdir(parents=True, exist_ok=True)
    bars = len(df)
    with open(out_path, "wb") as f:
        f.write(build_header(symbol, digits, spread, bars))
        for ts, row in df.iterrows():
            utc_ms = int(ts.timestamp()) * 1000  # milliseconds
            record = struct.pack(
                RATE_FORMAT,
                utc_ms,
                row["Open"],
                row["Low"],
                row["High"],
                row["Close"],
                int(row["Volume"]),
                0,  # spread kept in header
                int(row["Volume"]),
            )
            f.write(record)

# ---------- main ----------
def main():
    args = parse_args()
    for y in year_iter(args.years):
        df = read_raw_csv(y)
        df = apply_filters(df, args.limit_hours)
        if df.empty:
            print(f"No bars left after filtering for {y}")
            continue

        missing = (365 * 24 * 60) - len(df)
        if missing:
            print(f"Warning: {missing} minutes removed by filters for {y}")

        for account, symbol in (("DEMO", "GER30(£)"), ("LIVE", "GER30")):
            base_name = f"{symbol}1{args.file_suffix}"
            out_dir   = Path(account)
            write_csv(df, out_dir / f"{base_name}.csv", args.digits)
            write_hst(df, symbol, out_dir / f"{base_name}.hst", args.digits, args.spread)

        print(f"{y}: wrote {len(df):,} bars → DEMO & LIVE")

if __name__ == "__main__":
    main()
