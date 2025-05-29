#!/usr/bin/env python3
"""
MT4 Converter: Read DAX CSV (semicolon-delimited, day-first), clean and fill missing bars,
and output MT4 HST (v509) and CSV files for IG-DEMO and IG-LIVE environments.

Usage:
    python mt4_converter_refactored.py INPUT_CSV YEARS [--output_base_dir DIR]
        [--digits D] [--spread S] [--copyright C]
        [--input_timezone TZ] [--no_remove_weekends] [--verbose]
        [--symbol_suffix SUFFIX] [--combine_years]
        python mt4_converter_refactored.py INPUT_CSV YEARS [--output_base_dir DIR] [--digits D] [--spread S] [--copyright C] [--input_timezone TZ] [--no_remove_weekends] [--verbose] [--symbol_suffix SUFFIX] [--combine_years]
        python mt4_converter_refactored.py dax-1m.csv 2024 --combine_years
        python mt4_converter_refactored.py dax-1m.csv 2024 --combine_years --symbol_suffix _OFFLINE

"""

import os
import struct
import argparse
import logging
from datetime import datetime, UTC
import pytz
import pandas as pd
import numpy as np

# Constants for HST file format (version 509)
HST_VERSION = 509
HST_HEADER_FORMAT = (
    "<i"      # version
    "64s"     # copyright
    "12s"     # symbol
    "i"       # period
    "i"       # digits
    "i"       # creation timestamp
    "i"       # last sync
    "52s"     # reserved
)
RATE_FORMAT = (
    "<q"  # timestamp (int64)
    "d"   # open
    "d"   # high
    "d"   # low
    "d"   # close
    "q"   # volume
    "i"   # spread
    "q"   # real volume
)

def setup_logging(verbose: bool):
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(format="%(asctime)s %(levelname)s: %(message)s", level=level)

def parse_args():
    parser = argparse.ArgumentParser(description="Convert DAX CSV to MT4 HST/CSV")
    parser.add_argument("input_csv", help="Path to input DAX CSV file")
    parser.add_argument("years", help="Year or range (e.g., '2024' or '2000-2025')")
    parser.add_argument("--output_base_dir", default="mt4_output_data",
                        help="Directory for output files")
    parser.add_argument("--digits", type=int, default=1,
                        help="Decimal places for prices (default: 1)")
    parser.add_argument("--spread", type=int, default=0,
                        help="Spread in points for HST records")
    parser.add_argument("--copyright", default="(C) Processed by script",
                        help="Copyright string for HST header")
    parser.add_argument("--input_timezone", default="Europe/Berlin",
                        help="Timezone of input data (Olson name)")
    parser.add_argument("--no_remove_weekends", action="store_true",
                        dest="keep_weekends",
                        help="Keep weekend data (default: remove Saturdays/Sundays)")
    parser.add_argument("--verbose", action="store_true",
                        help="Enable debug logging")
    parser.add_argument("--symbol_suffix", default="",
                        help="Suffix for symbol names (e.g., '_OFFLINE')")
    parser.add_argument("--combine_years", action="store_true",
                        help="Generate a single HST/CSV file for all specified years")
    return parser.parse_args()

def parse_years(year_arg: str):
    if "-" in year_arg:
        start, end = map(int, year_arg.split("-"))
        return list(range(start, end + 1))
    return [int(year_arg)]

# ── target timezone (London) ───────────────────────────────────────────
TARGET_TZ = pytz.timezone("Europe/London")

def read_and_clean_csv(path: str, tz_local: pytz.timezone):
    logging.info("Reading CSV: %s", path)
    cols = ['Date', 'Time', 'Open', 'High', 'Low', 'Close', 'Volume']
    df = pd.read_csv(path, sep=';', header=None, names=cols, dayfirst=True, dtype=str)
    # Convert OHLCV to numeric, coerce errors to NaN
    for col in ['Open', 'High', 'Low', 'Close', 'Volume']:
        df[col] = pd.to_numeric(df[col].replace(r'^\s*$', np.nan, regex=True),
                                errors='coerce')
    # Combine date/time and parse
    df['datetime'] = pd.to_datetime(df['Date'] + ' ' + df['Time'],
                                    format='%d/%m/%Y %H:%M:%S',
                                    errors='coerce')
    df.dropna(subset=['datetime', 'Open', 'High', 'Low', 'Close', 'Volume'],
              inplace=True)
    # Localize and convert to target timezone
    df['dt_local'] = df['datetime'].apply(lambda x: tz_local.localize(x))
    df['dt_london'] = df['dt_local'].dt.tz_convert(TARGET_TZ)
    df.set_index('dt_london', inplace=True)
    df = df[['Open', 'High', 'Low', 'Close', 'Volume']]
    df = df.sort_index()
    logging.info("Read %d records from %s to %s", len(df), df.index.min(), df.index.max())
    return df

def fill_missing_minutes(df: pd.DataFrame, keep_weekends: bool):
    start, end = df.index.min(), df.index.max()
    full_idx = pd.date_range(start, end, freq='min', tz=TARGET_TZ)
    df = df.reindex(full_idx)
    missing = df['Open'].isna().sum()
    if missing:
        logging.info("Filling %d missing minutes", missing)
        df[['Open','High','Low','Close']] = df[['Open','High','Low','Close']].ffill()
        df['Volume'] = df['Volume'].fillna(0).astype(int)
    if not keep_weekends:
        df = df[~df.index.weekday.isin([5,6])]
        logging.debug("Removed weekends, remaining entries: %d", len(df))
    return df

def create_hst(df: pd.DataFrame, filepath: str, symbol: str,
               period: int, digits: int, copyright_str: str, spread: int):
    os.makedirs(os.path.dirname(filepath), exist_ok=True)
    with open(filepath, 'wb') as f:
        # Header
        sym_bytes = symbol.encode('cp1252', errors='replace')[:11].ljust(12, b'\0')
        copy_bytes = copyright_str.encode('ascii', errors='replace')[:63].ljust(64, b'\0')
        timestamp = int(datetime.now(UTC).timestamp())
        header = struct.pack(HST_HEADER_FORMAT, HST_VERSION, copy_bytes,
                             sym_bytes, period, digits,
                             timestamp, 0, b'\0'*52)
        f.write(header)
        # Data
        for ts, row in df.iterrows():
            data = struct.pack(RATE_FORMAT, int(ts.timestamp()),
                               float(row['Open']), float(row['High']),
                               float(row['Low']), float(row['Close']),
                               int(row['Volume']), spread, 0)
            f.write(data)
    logging.info("Wrote HST: %s", filepath)

def create_csv(df: pd.DataFrame, filepath: str, digits: int):
    os.makedirs(os.path.dirname(filepath), exist_ok=True)
    out = pd.DataFrame({
        '<DATE>': df.index.strftime('%Y.%m.%d'),
        '<TIME>': df.index.strftime('%H:%M:%S'),
        '<OPEN>': df['Open'].map(lambda x: f"{x:.{digits}f}"),
        '<HIGH>': df['High'].map(lambda x: f"{x:.{digits}f}"),
        '<LOW>': df['Low'].map(lambda x: f"{x:.{digits}f}"),
        '<CLOSE>': df['Close'].map(lambda x: f"{x:.{digits}f}"),
        '<VOLUME>': df['Volume'].astype(int)
    })
    out.to_csv(filepath, index=False)
    logging.info("Wrote CSV: %s", filepath)

def main():
    args = parse_args()
    setup_logging(args.verbose)
    try:
        tz_local = pytz.timezone(args.input_timezone)
    except Exception as e:
        logging.error("Invalid timezone '%s': %s", args.input_timezone, e)
        return
    df = read_and_clean_csv(args.input_csv, tz_local)
    if df.empty:
        logging.error("No data found in CSV file. Please check the input file.")
        return
    df = fill_missing_minutes(df, args.keep_weekends)
    years = parse_years(args.years)
    suffix = args.symbol_suffix if args.symbol_suffix else ""
    configs = [
        {"env": "IG-DEMO", "file_sym": f"GER30(£){suffix}", "hdr_sym": f"GER30(£){suffix}"},
        {"env": "IG-LIVE", "file_sym": f"GER30{suffix}",    "hdr_sym": f"GER30{suffix}"}
    ]
    if args.combine_years:
        df_combined = df[df.index.year.isin(years)]
        if df_combined.empty:
            logging.warning("No data for the specified years: %s", years)
            return
        for cfg in configs:
            base = os.path.join(args.output_base_dir, cfg["env"])
            hst_fn = f"{cfg['file_sym']}1.hst"
            csv_fn = f"{cfg['file_sym']}1.csv"
            create_hst(df_combined, os.path.join(base, hst_fn),
                       cfg['hdr_sym'], 1, args.digits,
                       args.copyright, args.spread)
            create_csv(df_combined, os.path.join(base, csv_fn), args.digits)
    else:
        for year in years:
            sub = df[df.index.year == year]
            if sub.empty:
                logging.warning("No data for year %d", year)
                continue
            for cfg in configs:
                base = os.path.join(args.output_base_dir, cfg["env"])
                hst_fn = f"{cfg['file_sym']}1_{year}.hst"
                csv_fn = f"{cfg['file_sym']}1_{year}.csv"
                create_hst(sub, os.path.join(base, hst_fn),
                           cfg['hdr_sym'], 1, args.digits,
                           args.copyright, args.spread)
                create_csv(sub, os.path.join(base, csv_fn), args.digits)

if __name__ == "__main__":
    main()