--- START OF FILE mt4_converter.py ---

import pandas as pd
import struct
import os
import argparse
from datetime import datetime # Keep this
import pytz # For timezone handling
import numpy as np # For np.nan

# HST File Format Constants (Version 401)
HST_VERSION = 401
HST_HEADER_FORMAT = (
    "<i"      # version (4 bytes)
    "64s"     # copyright (64 bytes)
    "12s"     # symbol (12 bytes)
    "i"       # period (4 bytes)
    "i"       # digits (4 bytes)
    "i"       # timesign (4 bytes, timestamp of file creation, 0 is fine)
    "i"       # last_sync (4 bytes, 0 is fine)
    "52s"     # reserved (52 bytes)
) # Total 148 bytes

RATE_FORMAT = (
    "<q"  # ctm (long long / int64) - Unix timestamp (UTC)
    "d"   # open (double)
    "d"   # high (double)
    "d"   # low (double)
    "d"   # close (double)
    "q"   # vol (long long / int64)
    "i"   # spread (int)
    "q"   # real_vol (long long / int64)
) # Total 60 bytes

def parse_year_argument(year_arg):
    """Parses year argument string like '2024' or '2000-2023'."""
    if '-' in year_arg:
        start_year, end_year = map(int, year_arg.split('-'))
        return list(range(start_year, end_year + 1))
    else:
        return [int(year_arg)]

def create_hst_file(df_utc, output_filepath, symbol_name_for_header, period, digits, copyright_str, spread_points):
    """Creates an HST file from a pandas DataFrame with UTC timestamps."""
    os.makedirs(os.path.dirname(output_filepath), exist_ok=True)

    with open(output_filepath, 'wb') as f:
        # --- Write Header ---
        try:
            symbol_bytes = symbol_name_for_header.encode('cp1252')
        except UnicodeEncodeError:
            print(f"Warning: Symbol '{symbol_name_for_header}' contains characters not in cp1252 for HST header. Using ASCII with replacement.")
            symbol_bytes = symbol_name_for_header.encode('ascii', 'replace')

        if len(symbol_bytes) > 11: # Max 11 chars + null terminator
            symbol_bytes = symbol_bytes[:11]
        packed_symbol = symbol_bytes.ljust(12, b'\0')
        
        copyright_bytes = copyright_str.encode('ascii', 'replace')[:63].ljust(64, b'\0')
        # --- MODIFICATION HERE ---
        file_creation_timestamp = int(datetime.now(pytz.UTC).timestamp()) # Changed from datetime.utcnow()
        # --- END MODIFICATION ---

        header = struct.pack(
            HST_HEADER_FORMAT, HST_VERSION, copyright_bytes, packed_symbol,
            period, digits, file_creation_timestamp, 0, b'\0' * 52
        )
        f.write(header)

        if not isinstance(df_utc.index, pd.DatetimeIndex):
            raise ValueError("DataFrame index must be a DatetimeIndex for HST creation.")
        if df_utc.index.tz is None or (not df_utc.index.empty and df_utc.index.tz.utcoffset(df_utc.index[0]) != pytz.UTC.utcoffset(df_utc.index[0])):
             raise ValueError("DataFrame index must be timezone-aware UTC for HST creation.")
        
        df_sorted = df_utc.sort_index()

        for timestamp_utc_dt, row in df_sorted.iterrows():
            ctm = int(timestamp_utc_dt.timestamp())
            try:
                # These float/int conversions will fail if any NaNs survived, which they shouldn't for OHLCV
                rate_data = struct.pack(
                    RATE_FORMAT, ctm,
                    float(row['Open']), float(row['High']), float(row['Low']), float(row['Close']),
                    int(row['Volume']),
                    int(spread_points), 0
                )
                f.write(rate_data)
            except Exception as e:
                print(f"CRITICAL ERROR packing/writing rate data for HST: Timestamp {timestamp_utc_dt}, Row: {row}. Error: {e}")
                # raise # Re-raise to stop, or comment out to try to continue (not recommended for HST)
                # For HST, it's usually better to ensure data is clean beforehand.
                # If we reach here, it means dropna didn't work and NaNs are still in df_year_utc
                print("This indicates NaNs are still present in df_year_utc. Please check data cleaning steps.")
                return # Stop creating this specific HST file if bad data is encountered
    print(f"Generated HST: {output_filepath} (Spread: {spread_points}, Digits: {digits})")


def create_mt4_csv_file(df_utc, output_filepath, digits):
    """Creates a CSV file (UTC) suitable for MT4 History Center import."""
    os.makedirs(os.path.dirname(output_filepath), exist_ok=True)
    
    csv_df = pd.DataFrame()
    csv_df['date'] = df_utc.index.strftime('%Y.%m.%d')
    csv_df['time'] = df_utc.index.strftime('%H:%M:%S')
    
    price_format_string = f"{{:.{digits}f}}"
    
    # Apply formatting. NaNs should have been dropped from df_utc by this point.
    # If they somehow survived, this will format them as "nan" or error on float(nan) if not handled.
    csv_df['open'] = df_utc['Open'].apply(lambda x: price_format_string.format(float(x)))
    csv_df['high'] = df_utc['High'].apply(lambda x: price_format_string.format(float(x)))
    csv_df['low'] = df_utc['Low'].apply(lambda x: price_format_string.format(float(x)))
    csv_df['close'] = df_utc['Close'].apply(lambda x: price_format_string.format(float(x)))
    
    # Volume must be int. df_utc['Volume'] should not contain NaNs here.
    csv_df['volume'] = df_utc['Volume'].astype(int)

    csv_df.to_csv(output_filepath, index=False, header=['<DATE>','<TIME>','<OPEN>','<HIGH>','<LOW>','<CLOSE>','<VOLUME>'])
    print(f"Generated CSV: {output_filepath} (Timestamps are UTC, Digits: {digits})")


def main():
    parser = argparse.ArgumentParser(description="Convert CSV OHLC data to MT4 HST and CSV formats.")
    parser.add_argument("input_csv", help="Path to the input CSV file (e.g., Dax-1m.csv)")
    parser.add_argument("years", help="Year or year range to process (e.g., '2024' or '2000-2023')")
    parser.add_argument("--output_base_dir", default="mt4_output_data",
                        help="Base directory to save the output files (default: './mt4_output_data')")
    parser.add_argument("--digits", type=int, default=1,
                        help="Number of decimal places for price (default: 1)")
    parser.add_argument("--spread", type=int, default=0,
                        help="Spread in points for HST file records (default: 0)")
    parser.add_argument("--copyright", default="(C) Data processed by script",
                        help="Copyright string for HST header")
    parser.add_argument("--input_timezone", default="Europe/Berlin", # Adjusted default, change if needed
                        help="Timezone of the input CSV data. Default: 'Europe/Berlin'.")
    parser.add_argument("--remove_weekends", action='store_true', default=True,
                        help="Remove data falling on Saturdays/Sundays (UTC). Default: True.")
    parser.add_argument("--no_remove_weekends", action='store_false', dest='remove_weekends',
                        help="Do not remove weekend data.")

    args = parser.parse_args()

    try:
        input_tz = pytz.timezone(args.input_timezone)
    except pytz.exceptions.UnknownTimeZoneError:
        print(f"Error: Unknown input timezone '{args.input_timezone}'. Please use a valid Olson timezone name.")
        return
        
    print(f"Reading input CSV: {args.input_csv}")
    try:
        column_names = ['Date_Str', 'Time_Str', 'Open', 'High', 'Low', 'Close', 'Volume']
        # Read all as object first to handle mixed types and potential non-numeric strings robustly
        df_all = pd.read_csv(
            args.input_csv,
            delimiter=';',
            header=None,
            names=column_names,
            decimal=',', # Still useful if numbers use comma decimal
            dtype=str # Read ALL columns as string initially
        )
        print("\n--- Initial df_all (after read_csv with dtype=str) ---")
        df_all.info(verbose=True, show_counts=True)
        print("\nHead of initial df_all:")
        print(df_all.head())
        
        # Explicitly replace empty strings or strings with only whitespace with np.nan for OHLCV
        # This is crucial for robust NaN conversion before pd.to_numeric
        ohlcv_cols = ['Open', 'High', 'Low', 'Close', 'Volume']
        for col in ohlcv_cols:
            # Replace empty/whitespace strings with np.nan
            df_all[col] = df_all[col].replace(r'^\s*$', np.nan, regex=True)
            # Then convert to numeric, coercing errors. Valid numbers (even if read as str) will convert.
            # np.nan will remain np.nan. Other non-convertible strings will become np.nan.
            df_all[col] = pd.to_numeric(df_all[col], errors='coerce')

        print("\n--- df_all (after replacing blanks with NaN and pd.to_numeric for OHLCV) ---")
        df_all.info(verbose=True, show_counts=True)
        print("\nNaN counts per column (after explicit NaN conversion and to_numeric):")
        print(df_all.isna().sum())
        print("\nHead of df_all (after to_numeric):")
        print(df_all.head(10)) # Show a bit more
        print("\nTail of df_all (after to_numeric):")
        print(df_all.tail(10))
        
        df_all['datetime_str'] = df_all['Date_Str'] + ' ' + df_all['Time_Str']
        df_all['datetime_naive'] = pd.to_datetime(df_all['datetime_str'], format='%d/%m/%Y %H:%M:%S', errors='coerce')
        
        initial_row_count = len(df_all)
        df_all.dropna(subset=['datetime_naive'], inplace=True)
        if len(df_all) < initial_row_count:
            print(f"\nDropped {initial_row_count - len(df_all)} rows due to invalid date/time formatting.")

        initial_row_count_before_ohlcv_dropna = len(df_all)
        df_all.dropna(subset=ohlcv_cols, inplace=True) # Drop rows if ANY of OHLCV is NaN
        print("\n--- df_all (after dropna for OHLCV) ---")
        if len(df_all) < initial_row_count_before_ohlcv_dropna:
            print(f"Dropped {initial_row_count_before_ohlcv_dropna - len(df_all)} rows due to NaN in OHLCV.")
        else:
            print("No rows dropped by OHLCV dropna (either all OHLCV data was valid or already filtered).")
        
        df_all.info(verbose=True, show_counts=True)
        print("\nNaN counts per column (after OHLCV dropna):")
        print(df_all.isna().sum()) # Should be 0 for OHLCV columns if df_all is not empty

        if df_all.empty:
            print("\nDataFrame is empty after all parsing and cleaning. No data to process. Exiting.")
            return
            
        df_all['datetime_localized'] = df_all['datetime_naive'].apply(lambda x: input_tz.localize(x, is_dst=None))
        df_all['datetime_utc'] = df_all['datetime_localized'].dt.tz_convert(pytz.utc)
        df_all.set_index('datetime_utc', inplace=True)
        
        df_all.drop(columns=['Date_Str', 'Time_Str', 'datetime_str', 'datetime_naive', 'datetime_localized'], inplace=True, errors='ignore')

    except FileNotFoundError:
        print(f"Error: Input CSV file not found at {args.input_csv}")
        return
    except Exception as e:
        print(f"Error reading or parsing CSV: {e}")
        import traceback
        traceback.print_exc()
        return

    if df_all.empty:
        print("No valid data loaded after all processing steps. Please check CSV format and timezone.")
        return

    if args.remove_weekends:
        original_count = len(df_all)
        if isinstance(df_all.index, pd.DatetimeIndex):
            df_all = df_all[~df_all.index.dayofweek.isin([5, 6])]
            print(f"\nRemoved {original_count - len(df_all)} weekend (Sat/Sun UTC) data points.")
        else:
            print("\nWarning: Cannot remove weekends as index is not a DatetimeIndex.")

    years_to_process = parse_year_argument(args.years)
    print(f"\nProcessing years: {years_to_process}")
    print(f"Parameters: Digits={args.digits}, Spread={args.spread}, Copyright='{args.copyright}', Input TZ='{args.input_timezone}', Remove Weekends={args.remove_weekends}")

    configs = [
        {"env_name": "IG-DEMO", "filename_symbol_part": "GER30(£)", "hst_symbol_header": "GER30(£)"},
        {"env_name": "IG-LIVE", "filename_symbol_part": "GER30", "hst_symbol_header": "GER30"}
    ]
    
    for year in years_to_process:
        print(f"\nProcessing data for year: {year}")
        # Use .copy() to avoid SettingWithCopyWarning if any further modifications were planned on df_year_utc (not currently the case here but good practice)
        df_year_utc = df_all[df_all.index.year == year].copy() 

        if df_year_utc.empty:
            print(f"No data found for year {year} (after all filters).")
            continue

        print(f"\n--- df_year_utc for year {year} (before file creation) ---")
        df_year_utc.info(verbose=True, show_counts=True)
        print(f"\nHead of df_year_utc ({year}):")
        print(df_year_utc.head())
        print(f"\nTail of df_year_utc ({year}):")
        print(df_year_utc.tail())
        print(f"\nNaN counts for df_year_utc ({year}) (should be all zeros for OHLCV):")
        print(df_year_utc.isna().sum()) # Should be all zero for OHLCV columns here

        # Final check before writing files
        if df_year_utc[ohlcv_cols].isna().any().any():
            print(f"CRITICAL WARNING: NaNs detected in OHLCV columns for year {year} just before file writing. This should not happen.")
            print("Problematic rows (first 5 with NaNs):")
            print(df_year_utc[df_year_utc[ohlcv_cols].isna().any(axis=1)].head())
            # continue # Skip this year or handle error
            
        for config in configs:
            env_output_path = os.path.join(args.output_base_dir, config["env_name"])
            os.makedirs(env_output_path, exist_ok=True)
            
            hst_filename = f"{config['filename_symbol_part']}1_{year}.hst"
            hst_filepath = os.path.join(env_output_path, hst_filename)
            try:
                create_hst_file(df_year_utc, hst_filepath, config["hst_symbol_header"], 1, args.digits, args.copyright, args.spread)
            except Exception as e:
                print(f"CRITICAL ERROR during HST file creation for {hst_filepath}: {e}")
                import traceback
                traceback.print_exc()

            csv_filename = f"{config['filename_symbol_part']}1_{year}.csv"
            csv_filepath = os.path.join(env_output_path, csv_filename)
            try:
                create_mt4_csv_file(df_year_utc, csv_filepath, args.digits)
            except Exception as e:
                print(f"CRITICAL ERROR during CSV file creation for {csv_filepath}: {e}")
                import traceback
                traceback.print_exc()
            
    print("\nProcessing complete.")
    print(f"Output files are in: {os.path.abspath(args.output_base_dir)}")
    print("Remember to restart MetaTrader 4 after copying HST files to its history folder.")
    print("For testing, rename the specific _YYYY.hst file to strip the year (e.g., 'GER30(£)1.hst' or 'GER301.hst').")

if __name__ == "__main__":
    main()

--- END OF FILE mt4_converter.py ---