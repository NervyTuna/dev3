#!/usr/bin/env python3
"""
DAX 1m Backtest that SKIPS missing data and logs 'MinsSkipped' in each open trade,
storing state in a single dictionary (no more `global allowSession1` issues).
"""

import csv
import argparse
import logging
from datetime import datetime, time
import pandas as pd
import pytz

##############################################################################
# Strategy Constants
##############################################################################
SESSION1_START = (8, 0)
SESSION1_END   = (12, 30)
SESSION2_START = (14, 30)
SESSION2_END   = (17, 16)

ZONE_LEVELS = [45, 70, 100, 130]
RETRACTION_TOLERANCE = 9.0
RETRACTION_STEPS = [
    {"min":15.0, "max":29.9, "shift":18},
    {"min":30.0, "max":35.9, "skip":1},
    {"min":36.0, "max":45.9, "skip":2},
    {"min":46.0, "max":9999, "cancel":True}
]
ZONE_CANCEL_LEVEL = 179
OVERNIGHT_VOL_LIMIT = 200
MIDDAY_VOL_LIMIT    = 150
GSL_DISTANCE        = 40.0

##############################################################################
# Session Data
##############################################################################
class SessionData:
    def __init__(self, name, startH, startM, endH, endM):
        self.name = name
        self.start = (startH, startM)
        self.end   = (endH, endM)
        self.openPrice = None
        self.high = None
        self.low  = None
        self.active = False
        self.tradeOpened = False

session1 = SessionData("Session1", *SESSION1_START, *SESSION1_END)
session2 = SessionData("Session2", *SESSION2_START, *SESSION2_END)

##############################################################################
# A single dictionary holds dynamic flags & references:
##############################################################################
strategyState = {
    "allowSession1": True,
    "allowSession2": True,
    "overnightRefPrice": None,
    "middayRefPrice": None,
    "activeTrade": None,
    "closedTrades": []
}

##############################################################################
# Command-line & Logging
##############################################################################
def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--csv", required=True, help="Path to CSV with semicolons")
    p.add_argument("--input_tz", default="Europe/Berlin", help="Timezone of input data")
    p.add_argument("--year_range", default="2024", help="Year or range (e.g. 2022 or 2020-2023)")
    p.add_argument("--verbose", action="store_true", help="Enable debug logs")
    return p.parse_args()

def setup_logging(verbose):
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(level=level, format="%(asctime)s %(levelname)s: %(message)s")

def parse_years(year_arg):
    if "-" in year_arg:
        start, end = map(int, year_arg.split("-"))
        return range(start, end+1)
    else:
        return [int(year_arg)]

##############################################################################
# Reading CSV
##############################################################################
def read_csv_dax(filename, input_tz="Europe/Berlin"):
    logging.info("Reading CSV: %s", filename)
    dfraw = pd.read_csv(filename, sep=";", header=None,
                        names=["Date","Time","Open","High","Low","Close","Volume"],
                        dtype=str)
    # Convert numeric columns
    for c in ["Open","High","Low","Close","Volume"]:
        dfraw[c] = pd.to_numeric(dfraw[c], errors="coerce")

    def parse_dt(row):
        dts = f"{row['Date']} {row['Time']}"
        try:
            return datetime.strptime(dts, "%d/%m/%Y %H:%M:%S")
        except:
            return None

    dfraw["naive_dt"] = dfraw.apply(parse_dt, axis=1)

    # drop invalid rows
    dfraw.dropna(subset=["naive_dt","Open","High","Low","Close","Volume"], inplace=True)

    tz_local = pytz.timezone(input_tz)
    def to_uk(dt_naive):
        loc = tz_local.localize(dt_naive)
        return loc.astimezone(pytz.timezone("Europe/London"))

    dfraw["uk_dt"] = dfraw["naive_dt"].apply(to_uk)
    dfraw.sort_values("uk_dt", inplace=True)
    logging.info("Data from %s to %s",
                 dfraw["uk_dt"].iloc[0], dfraw["uk_dt"].iloc[-1])

    # Convert to a list of rows
    rows = []
    for i, r in dfraw.iterrows():
        rows.append((r["uk_dt"], r["Open"], r["High"], r["Low"], r["Close"], r["Volume"]))
    return rows

##############################################################################
# Strategy Logic
##############################################################################
def is_within_session(dt, startH, startM, endH, endM):
    st = time(startH, startM)
    ed = time(endH, endM)
    t  = dt.timetz()
    if ed < st:
        return (t >= st) or (t < ed)
    return (t >= st) and (t < ed)

def start_session(sess, price, dt):
    sess.openPrice = price
    sess.high = price
    sess.low  = price
    sess.active = True
    sess.tradeOpened = False
    logging.debug("Session %s start at %s price=%.1f", sess.name, dt, price)

def end_session(sess, dt):
    sess.active = False
    sess.openPrice = None
    sess.high = None
    sess.low  = None
    sess.tradeOpened = False
    logging.debug("Session %s end at %s", sess.name, dt)

def distance_from_open(sess, price):
    if sess.openPrice is None:
        return 0.0
    return abs(price - sess.openPrice)

def apply_retraction(price, sess):
    if sess.openPrice is None:
        return None
    if price >= sess.high:
        retractVal = sess.high - price
    else:
        retractVal = price - sess.low

    for r in RETRACTION_STEPS:
        if retractVal >= r["min"] and retractVal <= r["max"]:
            if "shift" in r:
                return {"shift": r["shift"]}
            if "skip" in r:
                return {"skip": r["skip"]}
            if "cancel" in r:
                return {"cancel": True}
    return None

def get_target_level(dist):
    candidate = None
    for lvl in ZONE_LEVELS:
        if dist >= lvl:
            candidate = lvl
    return candidate

def open_trade(sess, direction, entryPrice, dt):
    strategyState["activeTrade"] = {
        "direction": direction,
        "entryPrice": entryPrice,
        "openTime": dt,
        "session": sess.name,
        "minsSkipped": 0.0
    }
    sess.tradeOpened = True
    logging.debug("Open %s @ %.1f time=%s session=%s", direction, entryPrice, dt, sess.name)

def close_trade(currentPrice, dt, reason):
    if strategyState["activeTrade"] is not None:
        activeT = strategyState["activeTrade"]
        direction = activeT["direction"]
        entry = activeT["entryPrice"]
        openT = activeT["openTime"]
        if direction == "BUY":
            plPoints = currentPrice - entry
        else:
            plPoints = entry - currentPrice

        strategyState["closedTrades"].append({
            "openTime": openT,
            "closeTime": dt,
            "direction": direction,
            "entryPrice": entry,
            "closePrice": currentPrice,
            "points": plPoints,
            "reason": reason,
            "session": activeT["session"],
            "minsSkipped": activeT["minsSkipped"]
        })
        logging.debug("Close %s @ %.1f => P/L=%.1f reason=%s skipMins=%.1f",
                      direction, currentPrice, plPoints, reason, activeT["minsSkipped"])
        strategyState["activeTrade"] = None

def check_gsl_stop(currentPrice, dt):
    at = strategyState["activeTrade"]
    if at is None:
        return
    if at["direction"] == "BUY":
        stop = at["entryPrice"] - GSL_DISTANCE
        if currentPrice <= stop:
            close_trade(currentPrice, dt, "GSL")
    else:
        stop = at["entryPrice"] + GSL_DISTANCE
        if currentPrice >= stop:
            close_trade(currentPrice, dt, "GSL")

def check_sweeps(sess, currentPrice, dt):
    if sess.active:
        dist = distance_from_open(sess, currentPrice)
        if dist >= ZONE_CANCEL_LEVEL:
            if strategyState["activeTrade"] is not None:
                close_trade(currentPrice, dt, "Sweep>179")

def check_volatility_filters(dt, currentPrice):
    st = strategyState
    # store overnight
    if dt.hour==17 and dt.minute==16 and st["overnightRefPrice"] is None:
        st["overnightRefPrice"] = currentPrice
    # check overnight
    if dt.hour==8 and dt.minute==0:
        if st["overnightRefPrice"] is not None:
            if abs(currentPrice - st["overnightRefPrice"]) >= OVERNIGHT_VOL_LIMIT:
                st["allowSession1"] = False
                logging.debug("Overnight vol >=200 => skip session1")
        st["overnightRefPrice"] = None

    # midday
    if dt.hour==12 and dt.minute==0 and st["middayRefPrice"] is None:
        st["middayRefPrice"] = currentPrice
    if dt.hour==14 and dt.minute==30:
        if st["middayRefPrice"] is not None:
            if abs(currentPrice - st["middayRefPrice"]) >= MIDDAY_VOL_LIMIT:
                st["allowSession2"] = False
                logging.debug("Midday vol >=150 => skip session2")
        st["middayRefPrice"] = None

def tick_logic(dt, price):
    st = strategyState
    check_volatility_filters(dt, price)

    # session1 logic
    if st["allowSession1"]:
        if is_within_session(dt, *SESSION1_START, *SESSION1_END):
            if not session1.active:
                start_session(session1, price, dt)
        else:
            if session1.active:
                end_session(session1, dt)
    else:
        if session1.active:
            end_session(session1, dt)

    # session2 logic
    if st["allowSession2"]:
        if is_within_session(dt, *SESSION2_START, *SESSION2_END):
            if not session2.active:
                start_session(session2, price, dt)
        else:
            if session2.active:
                end_session(session2, dt)
    else:
        if session2.active:
            end_session(session2, dt)

    # update highs/lows, sweeps
    if session1.active:
        if price > session1.high: session1.high=price
        if price < session1.low:  session1.low=price
        check_sweeps(session1, price, dt)
    if session2.active:
        if price > session2.high: session2.high=price
        if price < session2.low:  session2.low=price
        check_sweeps(session2, price, dt)

    # GSL
    check_gsl_stop(price, dt)

    # attempt new trades
    if strategyState["activeTrade"] is None:
        if session1.active and not session1.tradeOpened:
            try_open_trade(session1, dt, price)
        if session2.active and not session2.tradeOpened:
            try_open_trade(session2, dt, price)

def try_open_trade(sess, dt, price):
    st = strategyState
    dist = distance_from_open(sess, price)
    if dist > ZONE_LEVELS[-1] + RETRACTION_TOLERANCE:
        if sess==session1:
            st["allowSession1"] = False
        else:
            st["allowSession2"] = False
        logging.debug("distance>130+9 => skip %s", sess.name)
        return

    ret = apply_retraction(price, sess)
    if ret and "cancel" in ret:
        if sess==session1:
            st["allowSession1"] = False
        else:
            st["allowSession2"] = False
        logging.debug("retraction => cancel session %s", sess.name)
        return

    finalLevels = ZONE_LEVELS[:]
    if ret and "skip" in ret:
        finalLevels = finalLevels[ret["skip"]:]

    target = get_target_level(dist)
    if not target or target not in finalLevels:
        return

    shift = ret["shift"] if (ret and "shift" in ret) else 0

    isBuy = (price < sess.openPrice)
    if isBuy:
        finalPrice = sess.openPrice - (target + shift)
    else:
        finalPrice = sess.openPrice + (target + shift)

    minOk = target - RETRACTION_TOLERANCE
    maxOk = target + RETRACTION_TOLERANCE
    if dist<minOk or dist>maxOk:
        return

    direction = "BUY" if isBuy else "SELL"
    open_trade(sess, direction, price, dt)

def run_backtest(rows, years):
    filtered = [r for r in rows if r[0].year in years]
    if not filtered:
        logging.warning("No data for the specified years: %s", list(years))
        return

    for i in range(len(filtered)):
        dt, op, hi, lo, cl, vol = filtered[i]
        price = op  # or your choice
        tick_logic(dt, price)

        # check gap to next
        if i < len(filtered)-1:
            dtNext = filtered[i+1][0]
            delta = dtNext - dt
            mins = delta.total_seconds()/60.0
            if mins>1.0:
                missed = int(mins - 1)
                if missed>0 and strategyState["activeTrade"] is not None:
                    strategyState["activeTrade"]["minsSkipped"] += missed

    # end => if trade open, close
    if strategyState["activeTrade"]:
        lastrow = filtered[-1]
        lastPrice = lastrow[1]
        close_trade(lastPrice, lastrow[0], "EndOfBacktest")

def summarize_results():
    closed = strategyState["closedTrades"]
    if not closed:
        print("No trades closed.")
        return

    dfc = pd.DataFrame(closed)
    dfc["year"]  = dfc["closeTime"].dt.year
    dfc["month"] = dfc["closeTime"].dt.strftime("%Y-%m")

    print("\nClosed Trades:\n")
    for i,row in dfc.iterrows():
        print(f"{row['openTime']} -> {row['closeTime']}, {row['direction']}, "
              f"Entry={row['entryPrice']:.1f}, Close={row['closePrice']:.1f}, "
              f"Pts={row['points']:.1f}, SkipMins={row['minsSkipped']}, "
              f"Reason={row['reason']}")

    monthly = dfc.groupby("month")["points"].sum().reset_index()
    print("\nMonthly Totals:")
    for i,row in monthly.iterrows():
        print(f"{row['month']} => {row['points']:.1f} pts")

    yearly = dfc.groupby("year")["points"].sum().reset_index()
    print("\nYearly Totals:")
    for i,row in yearly.iterrows():
        print(f"{row['year']} => {row['points']:.1f} pts")

def main():
    args = parse_args()
    setup_logging(args.verbose)
    years = parse_years(args.year_range)
    rows = read_csv_dax(args.csv, args.input_tz)
    if not rows:
        logging.error("No data rows found.")
        return
    run_backtest(rows, years)
    summarize_results()

if __name__=="__main__":
    main()
