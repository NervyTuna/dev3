#!/usr/bin/env python3
"""
Analyze DAX 1-minute data to see hour-by-hour volatility for 2024.
We assume 'dax-1m.csv' has semicolon-delimited lines:
  Date;Time;Open;High;Low;Close;Volume
with day-first date format (dd/mm/yyyy).
We'll parse, filter to 2024, localize to London if needed,
and compute each hour's average "price range" or standard deviation.
"""

import pandas as pd
import numpy as np
import datetime

def main():
    # 1) Read raw CSV
    fname = "dax-1m.csv"
    print(f"Reading CSV: {fname}")
    df = pd.read_csv(
        fname,
        sep=';',
        names=['Date','Time','Open','High','Low','Close','Volume'],
        header=None,
        dayfirst=True
    )
    # Convert numeric columns
    for col in ['Open','High','Low','Close','Volume']:
        df[col] = pd.to_numeric(df[col], errors='coerce')
    df.dropna(subset=['Open','High','Low','Close','Volume'], inplace=True)

    # 2) Combine Date+Time => a proper datetime index
    df['Datetime'] = pd.to_datetime(df['Date'] + ' ' + df['Time'], dayfirst=True, errors='coerce')
    df.set_index('Datetime', inplace=True)
    df.sort_index(inplace=True)

    # (Optional) If the data is truly in "Europe/London" or needs localizing,
    # you could do:
    # df.index = df.index.tz_localize("Europe/Berlin").tz_convert("Europe/London")

    # 3) Filter to 2024 only
    df_2024 = df[df.index.year == 2024].copy()
    if df_2024.empty:
        print("No data for 2024; check your CSV or date parsing.")
        return

    # 4) Compute 1-min bar "range" or you might do (High-Low) or std dev
    df_2024['Range'] = df_2024['High'] - df_2024['Low']

    # 5) Group by hour-of-day => average range
    df_2024['Hour'] = df_2024.index.hour
    hourly_vol = df_2024.groupby('Hour')['Range'].mean()  # or .std() for stdev

    # 6) Print top hours
    print("\n=== Average 1-min Price Range by Hour (00-23) ===")
    print(hourly_vol.sort_values(ascending=False))

    # Done

if __name__ == "__main__":
    main()
