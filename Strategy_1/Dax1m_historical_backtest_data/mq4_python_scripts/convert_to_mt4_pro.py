#!/usr/bin/env python3
"""
MT4 Converter: Read DAX CSV (semicolon-delimited, day-first), clean/fill missing bars,
and output MT4 HST (v509) and CSV files for IG-DEMO and IG-LIVE environments.
Also produces a 5th CSV output (raw_segment) to compare original vs. processed.

Usage Examples:
    python mt4_converter_pro.py dax-1m.csv 2024 --combine_years
    python mt4_converter_pro.py dax-1m.csv 2024 --combine_years --symbol_suffix _OFFLINE
    python mt4_converter_pro.py dax-1m.csv 2000-2025
    python mt4_converter_pro.py dax-1m.csv 2024 --single_day "2024-02-13"
"""

import os
import struct
import argparse
import logging
from datetime import datetime, timezone
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
    "q"   # real volume (unused)
)

# We always convert final times to London time
TARGET_TZ = pytz.timezone("Europe/London")

def setup_logging(verbose: bool):
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(format="%(asctime)s %(levelname)s: %(message)s", level=level)

def parse_args():
    parser = argparse.ArgumentParser(description="Convert DAX CSV to MT4 HST/CSV + raw CSV segments")
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
    # Defaulting to Chicago time here, since your purchased data is from Chicago time
    parser.add_argument("--input_timezone", default="America/Chicago",
                        help="Timezone of input data (e.g. 'America/Chicago')")
    parser.add_argument("--no_remove_weekends", action="store_true",
                        dest="keep_weekends",
                        help="Keep weekend data (default: remove Saturdays/Sundays)")
    parser.add_argument("--verbose", action="store_true",
                        help="Enable debug logging")
    parser.add_argument("--symbol_suffix", default="",
                        help="Suffix for symbol names (e.g., '_OFFLINE')")
    parser.add_argument("--combine_years", action="store_true",
                        help="Generate a single HST/CSV file for all specified years")

    # NEW ARGUMENT for single-day export
    parser.add_argument("--single_day",
                        help="Export only one YYYY-MM-DD day to a single text file (raw + converted).")

    return parser.parse_args()

def parse_years(year_arg: str):
    """Parse 'years' arg into a list of int years."""
    if "-" in year_arg:
        start, end = map(int, year_arg.split("-"))
        return list(range(start, end + 1))
    return [int(year_arg)]

def read_raw_csv(path: str):
    """
    Reads the original CSV: semicolon-delimited, dayfirst format.
    Columns: [Date, Time, Open, High, Low, Close, Volume].
    Creates a 'datetime' column as naive (no timezone yet).
    """
    logging.info(f"Reading raw CSV: {path}")
    cols = ['Date', 'Time', 'Open', 'High', 'Low', 'Close', 'Volume']
    df_raw = pd.read_csv(path, sep=';', header=None, names=cols, dayfirst=True, dtype=str)

    # Convert numeric columns
    for col in ['Open', 'High', 'Low', 'Close', 'Volume']:
        df_raw[col] = pd.to_numeric(
            df_raw[col].replace(r'^\s*$', np.nan, regex=True),
            errors='coerce'
        )
    # Parse Date+Time => naive datetime
    df_raw['datetime'] = pd.to_datetime(
        df_raw['Date'] + ' ' + df_raw['Time'],
        format='%d/%m/%Y %H:%M:%S',
        errors='coerce'
    )
    df_raw.dropna(subset=['datetime','Open','High','Low','Close','Volume'], inplace=True)
    return df_raw

def localize_and_clean(df_raw: pd.DataFrame, tz_local: pytz.timezone):
    """
    1) Localize naive df_raw['datetime'] to tz_local (e.g. America/Chicago)
    2) Convert to London time
    3) Create final df with index=dt_london
    """
    # Localize naive => tz_local
    df_raw['dt_local'] = df_raw['datetime'].dt.tz_localize(
        tz_local,
        nonexistent='shift_forward',
        ambiguous='NaT'
    )
    # Convert local => London
    df_raw['dt_london'] = df_raw['dt_local'].dt.tz_convert(TARGET_TZ)

    df = df_raw.set_index('dt_london')[['Open','High','Low','Close','Volume']].copy()
    df.sort_index(inplace=True)
    return df

def fill_missing_minutes(df: pd.DataFrame, keep_weekends: bool):
    """
    Reindex to fill every minute between min and max times.
    Forward-fill O/H/L/C.
    Optionally remove weekends.
    """
    start, end = df.index.min(), df.index.max()
    full_idx = pd.date_range(start, end, freq='min', tz=TARGET_TZ)
    df = df.reindex(full_idx)
    missing = df['Open'].isna().sum()
    if missing:
        logging.info(f"Filling {missing} missing minutes (OHLC ffill).")
        df[['Open','High','Low','Close']] = df[['Open','High','Low','Close']].ffill()
        df['Volume'] = df['Volume'].fillna(0).astype(int)

    if not keep_weekends:
        before = len(df)
        df = df[~df.index.weekday.isin([5,6])]
        after = len(df)
        logging.debug(f"Removed weekends: from {before} to {after} rows.")

    return df

def create_hst(df: pd.DataFrame, filepath: str, symbol: str,
               period: int, digits: int, copy_str: str, spread: int):
    """
    Writes an HST v509 file with the processed data.
    """
    os.makedirs(os.path.dirname(filepath), exist_ok=True)
    with open(filepath, 'wb') as f:
        sym_bytes = symbol.encode('cp1252', errors='replace')[:11].ljust(12,b'\0')
        copy_bytes = copy_str.encode('ascii', errors='replace')[:63].ljust(64,b'\0')
        timestamp = int(datetime.now(timezone.utc).timestamp())
        header = struct.pack(
            HST_HEADER_FORMAT,
            HST_VERSION,       # version
            copy_bytes,        # copyright
            sym_bytes,         # symbol
            period,            # period
            digits,            # digits
            timestamp,         # creation timestamp
            0,                 # last sync
            b'\0'*52           # reserved
        )
        f.write(header)

        # Write bar data
        for ts, row in df.iterrows():
            data = struct.pack(
                RATE_FORMAT,
                int(ts.timestamp()),      # time as int64
                float(row['Open']),       # open
                float(row['High']),       # high
                float(row['Low']),        # low
                float(row['Close']),      # close
                int(row['Volume']),       # volume
                spread,                   # spread
                0                         # real volume
            )
            f.write(data)
    logging.info(f"Wrote HST: {filepath}")

def create_csv(df: pd.DataFrame, filepath: str, digits: int):
    """
    Creates a CSV in standard <DATE>,<TIME>,<OPEN>... format for MT4 or other analysis.
    By default: YYYY.MM.DD in <DATE>.
    """
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
    logging.info(f"Wrote CSV: {filepath}")

def create_raw_segment_csv(df_raw: pd.DataFrame, outfile: str, years: list, combine: bool):
    """
    Writes a "raw segment" CSV of unmodified data for the chosen year(s).
    Same semicolon-delimited format as input, day-first dates, no headers.
    Useful for comparing original vs. processed.
    """
    os.makedirs(os.path.dirname(outfile), exist_ok=True)
    if combine:
        df_sub = df_raw[df_raw['datetime'].dt.year.isin(years)].copy()
        if df_sub.empty:
            logging.warning(f"No raw data for combined years {years}")
            return
        _write_raw_csv(df_sub, outfile)
        logging.info(f"Wrote RAW combined CSV: {outfile}")
    else:
        # If multiple years, one file per year
        for y in years:
            df_y = df_raw[df_raw['datetime'].dt.year == y].copy()
            if df_y.empty:
                logging.warning(f"No raw data for year {y}")
                continue
            base,ext = os.path.splitext(outfile)
            outy = f"{base}_{y}{ext}"
            _write_raw_csv(df_y, outy)
            logging.info(f"Wrote RAW CSV for year {y} => {outy}")

def _write_raw_csv(df_segment: pd.DataFrame, filepath: str):
    """
    Writes data in semicolon-delimited format:
      Date;Time;Open;High;Low;Close;Volume
    with dayfirst date (dd/mm/YYYY).
    """
    df_segment.sort_values(by='datetime', inplace=True)
    df_segment['DateStr'] = df_segment['datetime'].dt.strftime('%d/%m/%Y')
    df_segment['TimeStr'] = df_segment['datetime'].dt.strftime('%H:%M:%S')
    df_segment[['DateStr','TimeStr','Open','High','Low','Close','Volume']] \
        .to_csv(filepath, sep=';', index=False, header=False)

def main():
    args = parse_args()
    setup_logging(args.verbose)

    # 1) Attempt to load user-defined tz (default: America/Chicago)
    try:
        tz_local = pytz.timezone(args.input_timezone)
    except Exception as e:
        logging.error(f"Invalid timezone '{args.input_timezone}': {e}")
        return

    # 2) Read raw CSV
    df_raw = read_raw_csv(args.input_csv)
    if df_raw.empty:
        logging.error("No valid data in raw CSV. Check input file.")
        return

    # ============ SINGLE-DAY MODE ====================================
    if args.single_day:
        single_str = args.single_day.strip()  # e.g. "2024-02-13"
        try:
            single_day_dt = pd.to_datetime(single_str, format="%Y-%m-%d")
        except ValueError:
            logging.error(f"Unable to parse --single_day='{single_str}' as YYYY-MM-DD.")
            return
        
        # Filter raw data to that single date
        df_day_raw = df_raw[df_raw['datetime'].dt.date == single_day_dt.date()]
        if df_day_raw.empty:
            logging.warning(f"No raw data found for single day: {single_str}")
            return
        
        # Localize + fill
        df_day_local = localize_and_clean(df_day_raw, tz_local)
        df_day_filled = fill_missing_minutes(df_day_local, args.keep_weekends)
        if df_day_filled.empty:
            logging.warning(f"No processed data left after fill_missing for {single_str}")
            return

        # Build an in-memory text of raw + converted
        lines = []
        lines.append("=== RAW SEGMENT (Single Day) ===\n")
        for _, row in df_day_raw.iterrows():
            # "dd/mm/YYYY;HH:MM:SS;Open;High;Low;Close;Volume"
            dt_str = row['datetime'].strftime("%d/%m/%Y;%H:%M:%S")
            seg_line = f"{dt_str};{row['Open']};{row['High']};{row['Low']};{row['Close']};{row['Volume']}"
            lines.append(seg_line)

        lines.append("\n=== CONVERTED 1M BARS (Single Day) ===\n")
        for ts, row in df_day_filled.iterrows():
            # "YYYY.MM.DD,HH:MM:SS,Open,High,Low,Close,Volume"
            dt_date = ts.strftime("%Y.%m.%d")
            dt_time = ts.strftime("%H:%M:%S")
            open_  = f"{row['Open']:.{args.digits}f}"
            high_  = f"{row['High']:.{args.digits}f}"
            low_   = f"{row['Low']:.{args.digits}f}"
            close_ = f"{row['Close']:.{args.digits}f}"
            vol_   = str(int(row['Volume']))
            conv_line = f"{dt_date},{dt_time},{open_},{high_},{low_},{close_},{vol_}"
            lines.append(conv_line)

        # Write to single_day_export.txt
        out_file = os.path.join(args.output_base_dir, "single_day_export.txt")
        os.makedirs(os.path.dirname(out_file), exist_ok=True)
        with open(out_file, "w", encoding="utf-8") as f:
            f.write("\n".join(lines))

        logging.info(f"Wrote single-day combined text to: {out_file}")
        return  # Done with single_day logic; skip normal year-based logic
    # ============ END SINGLE-DAY MODE =================================

    # 3) Subset by requested year(s)
    years = parse_years(args.years)
    suffix = args.symbol_suffix if args.symbol_suffix else ""
    configs = [
        {"env": "IG-DEMO", "file_sym": f"GER30(£){suffix}", "hdr_sym": f"GER30(£){suffix}"},
        {"env": "IG-LIVE", "file_sym": f"GER30{suffix}",    "hdr_sym": f"GER30{suffix}"}
    ]

    # 4) If combining years into 1
    if args.combine_years:
        df_combined = localize_and_clean(df_raw[df_raw['datetime'].dt.year.isin(years)], tz_local)
        df_combined = fill_missing_minutes(df_combined, args.keep_weekends)
        if df_combined.empty:
            logging.warning(f"No processed data for combined years: {years}")
        else:
            # Write HST/CSV
            for cfg in configs:
                base = os.path.join(args.output_base_dir, cfg["env"])
                hst_fn = f"{cfg['file_sym']}1.hst"
                csv_fn = f"{cfg['file_sym']}1.csv"
                create_hst(df_combined, os.path.join(base, hst_fn),
                           cfg['hdr_sym'], 1, args.digits,
                           args.copyright, args.spread)
                create_csv(df_combined, os.path.join(base, csv_fn), args.digits)

        # Also write the raw segment for combined
        raw_out = os.path.join(args.output_base_dir, "raw_segments", "raw_segment_combined.csv")
        create_raw_segment_csv(df_raw, raw_out, years, combine=True)

    else:
        # Year-by-year approach (filter BEFORE time zone conversion)
        for y in years:
            df_filtered = df_raw[df_raw['datetime'].dt.year == y].copy()
            if df_filtered.empty:
                logging.warning(f"No raw data for year {y}")
                continue

            df_processed_y = localize_and_clean(df_filtered, tz_local)
            df_processed_y = fill_missing_minutes(df_processed_y, args.keep_weekends)

            if df_processed_y.empty:
                logging.warning(f"No processed data for year {y} after localization.")
                continue

            for cfg in configs:
                base = os.path.join(args.output_base_dir, cfg["env"])
                hst_fn = f"{cfg['file_sym']}1_{y}.hst"
                csv_fn = f"{cfg['file_sym']}1_{y}.csv"
                create_hst(df_processed_y, os.path.join(base, hst_fn),
                           cfg['hdr_sym'], 1, args.digits,
                           args.copyright, args.spread)
                create_csv(df_processed_y, os.path.join(base, csv_fn), args.digits)

        # The raw data for each year (unmodified, from df_raw)
        raw_out = os.path.join(args.output_base_dir, "raw_segments", "raw_segment.csv")
        create_raw_segment_csv(df_raw, raw_out, years, combine=False)

if __name__ == "__main__":
    main()
