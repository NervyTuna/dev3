import pandas as pd

# Load the CSV (adjust path if needed)
csv_path = 'dax-1m.csv'
df = pd.read_csv(csv_path, sep=';', names=["Date", "Time", "Open", "High", "Low", "Close", "Volume"], skiprows=1)

# Combine Date and Time into full timestamp
df['Timestamp'] = pd.to_datetime(df['Date'] + ' ' + df['Time'], format='%d/%m/%Y %H:%M:%S')
df.set_index('Timestamp', inplace=True)

# Convert price columns to float
for col in ['Open', 'High', 'Low', 'Close']:
    df[col] = pd.to_numeric(df[col], errors='coerce')
df.dropna(inplace=True)

# ✅ Use full dataset (no year filtering)
df_all = df.copy()

# Create daily OHLC from 1-minute data
daily = df_all.resample('D').agg({
    'Open': 'first',
    'High': 'max',
    'Low': 'min',
    'Close': 'last'
}).dropna()

# Simulate breakout strategy: SL = 40, TP = 100
results = []
for i in range(1, len(daily)):
    prev_high = daily.iloc[i-1]['High']
    prev_low = daily.iloc[i-1]['Low']
    day_open = daily.iloc[i]['Open']
    date = daily.index[i]

    direction = None
    entry_price = None
    result = 0

    # Determine direction
    if day_open > prev_high:
        direction = 'buy'
        entry_price = day_open
        sl = entry_price - 40
        tp = entry_price + 100
    elif day_open < prev_low:
        direction = 'sell'
        entry_price = day_open
        sl = entry_price + 40
        tp = entry_price - 100

    if direction:
        intraday = df_all[df_all.index.date == date.date()]  # ✅ Updated line
        for _, row in intraday.iterrows():
            if direction == 'buy':
                if row['Low'] <= sl:
                    result = -40
                    break
                elif row['High'] >= tp:
                    result = 100
                    break
            elif direction == 'sell':
                if row['High'] >= sl:
                    result = -40
                    break
                elif row['Low'] <= tp:
                    result = 100
                    break

        results.append({
            'Date': date,
            'Direction': direction,
            'Entry': entry_price,
            'ResultPts': result
        })

# Summarize results by year
df_results = pd.DataFrame(results)
df_results['Year'] = df_results['Date'].dt.year
yearly_summary = df_results.groupby('Year')['ResultPts'].sum()

# Output results
print("Yearly Results (All Years) in Index Points:")
print(yearly_summary)
