#!/usr/bin/env python3
"""
DAX 1m Backtest with semicolon CSV (Date,Time,Open,High,Low,Close,Volume).
No "datetime" column needed. We parse columns 0..6 ourselves.

Usage:
  python backtest.py --csv "dax-1m.csv" --year_range "2024"
"""

import csv
import argparse
import logging
from datetime import datetime, time
import math

##############################################################################
# Strategy Parameters
##############################################################################
SESSION1_START = (8, 0)    # 08:00
SESSION1_END   = (12, 30)  # 12:30
SESSION2_START = (14, 30)  # 14:30
SESSION2_END   = (17, 16)  # 17:16

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
# Session Data Structures
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
# Single dictionary holds dynamic flags & references
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
    p.add_argument("--csv", required=True, help="Path to DAX CSV (semicolon separated)")
    p.add_argument("--year_range", default="2024", help="Single year or range (e.g. '2024' or '2000-2003')")
    p.add_argument("--verbose", action="store_true", help="Enable debug-level logs")
    return p.parse_args()

def setup_logging(verbose: bool):
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(format="%(asctime)s %(levelname)s: %(message)s", level=level)

def parse_year_range(arg) -> list:
    if "-" in arg:
        start, end = arg.split("-")
        return list(range(int(start), int(end)+1))
    else:
        return [int(arg)]

##############################################################################
# Time & Session Helpers
##############################################################################
def is_within_session(dt: datetime, startH, startM, endH, endM) -> bool:
    """Check if dt's time is within [startH:startM, endH:endM)."""
    st = time(startH, startM)
    ed = time(endH, endM)
    t  = dt.time()
    if ed < st:
        # crosses midnight, not typical here
        return (t >= st) or (t < ed)
    return (t >= st) and (t < ed)

def start_session(sess: SessionData, price: float):
    sess.openPrice = price
    sess.high = price
    sess.low  = price
    sess.active = True
    sess.tradeOpened = False

def end_session(sess: SessionData):
    sess.active = False
    sess.openPrice = None
    sess.high = None
    sess.low  = None
    sess.tradeOpened = False

def distance_from_open(sess: SessionData, price: float) -> float:
    if sess.openPrice is None:
        return 0.0
    return abs(price - sess.openPrice)

def apply_retraction(currentPrice: float, sess: SessionData):
    """
    Return dict {shift:x} or {skip:y} or {cancel:True} or None
    based on how far currentPrice is from sess.high/sess.low vs. RETRACTION_STEPS.
    """
    if sess.openPrice is None:
        return None
    # measure retraction from the 'peak' side
    if currentPrice >= sess.high:
        retr = sess.high - currentPrice
    else:
        retr = currentPrice - sess.low

    for step in RETRACTION_STEPS:
        if retr >= step["min"] and retr <= step["max"]:
            if "shift" in step:
                return {"shift": step["shift"]}
            if "skip" in step:
                return {"skip": step["skip"]}
            if "cancel" in step:
                return {"cancel": True}
    return None

def get_target_level(dist: float) -> int:
    """Find largest ZONE_LEVEL <= dist."""
    candidate = None
    for lvl in ZONE_LEVELS:
        if dist >= lvl:
            candidate = lvl
    return candidate

##############################################################################
# Trade & Volatility Checking
##############################################################################
def check_gsl_stop(currentPrice: float):
    """
    Guaranteed stop at +/-40 from entry. If triggered, close trade.
    """
    st = strategyState
    if st["activeTrade"] is None:
        return
    trade = st["activeTrade"]
    direction = trade["direction"]
    entry = trade["entryPrice"]
    if direction == "BUY":
        stopPrice = entry - GSL_DISTANCE
        if currentPrice <= stopPrice:
            # close
            pl = currentPrice - entry
            st["closedTrades"].append({
                "direction": direction,
                "entry": entry,
                "exit": currentPrice,
                "resultPips": pl,
                "reason": "GSL"
            })
            st["activeTrade"] = None
    else:  # SELL
        stopPrice = entry + GSL_DISTANCE
        if currentPrice >= stopPrice:
            pl = entry - currentPrice
            st["closedTrades"].append({
                "direction": direction,
                "entry": entry,
                "exit": currentPrice,
                "resultPips": pl,
                "reason": "GSL"
            })
            st["activeTrade"] = None

def close_trade(currentPrice: float, reason: str):
    st = strategyState
    if st["activeTrade"] is not None:
        t = st["activeTrade"]
        direction = t["direction"]
        entry = t["entryPrice"]
        if direction == "BUY":
            pl = currentPrice - entry
        else:
            pl = entry - currentPrice
        st["closedTrades"].append({
            "direction": direction,
            "entry": entry,
            "exit": currentPrice,
            "resultPips": pl,
            "reason": reason
        })
        st["activeTrade"] = None

def check_volatility_filters(dt: datetime, currentPrice: float):
    st = strategyState
    # 17:16 => store overnightRef
    if dt.hour==17 and dt.minute==16 and st["overnightRefPrice"] is None:
        st["overnightRefPrice"] = currentPrice
    # 08:00 => compare => skip session1 if >=200
    if dt.hour==8 and dt.minute==0:
        if st["overnightRefPrice"] is not None:
            if abs(currentPrice - st["overnightRefPrice"]) >= OVERNIGHT_VOL_LIMIT:
                st["allowSession1"] = False
        st["overnightRefPrice"] = None

    # 12:00 => store middayRef
    if dt.hour==12 and dt.minute==0 and st["middayRefPrice"] is None:
        st["middayRefPrice"] = currentPrice
    # 14:30 => compare => skip session2 if >=150
    if dt.hour==14 and dt.minute==30:
        if st["middayRefPrice"] is not None:
            if abs(currentPrice - st["middayRefPrice"]) >= MIDDAY_VOL_LIMIT:
                st["allowSession2"] = False
        st["middayRefPrice"] = None

def check_sweeps(currentPrice: float):
    # if session1 active & dist>=179 => close trade
    if session1.active and distance_from_open(session1, currentPrice)>=ZONE_CANCEL_LEVEL:
        close_trade(currentPrice, f"Sweep>={ZONE_CANCEL_LEVEL} (Session1)")
    if session2.active and distance_from_open(session2, currentPrice)>=ZONE_CANCEL_LEVEL:
        close_trade(currentPrice, f"Sweep>={ZONE_CANCEL_LEVEL} (Session2)")

##############################################################################
# Opening Trades
##############################################################################
def try_open_trade(currentPrice: float, sess: SessionData):
    st = strategyState
    # must be active session, not have opened a trade, not have global activeTrade
    if not sess.active:
        return
    if sess.tradeOpened:
        return
    if st["activeTrade"] is not None:
        return

    dist = distance_from_open(sess, currentPrice)
    if dist > ZONE_LEVELS[-1] + RETRACTION_TOLERANCE:
        # skip session
        if sess==session1:
            st["allowSession1"] = False
        else:
            st["allowSession2"] = False
        return

    ret = apply_retraction(currentPrice, sess)
    if ret and "cancel" in ret and st["activeTrade"] is None:
        if sess==session1:
            st["allowSession1"] = False
        else:
            st["allowSession2"] = False
        return

    finalLevels = ZONE_LEVELS[:]
    if ret and "skip" in ret:
        finalLevels = finalLevels[ret["skip"]:]

    target = get_target_level(dist)
    if target not in finalLevels:
        return

    shift = ret["shift"] if (ret and "shift" in ret) else 0

    isBuy = (currentPrice < sess.openPrice)
    if isBuy:
        finalPrice = sess.openPrice - (target + shift)
    else:
        finalPrice = sess.openPrice + (target + shift)

    # Tolerance
    minOk = target - RETRACTION_TOLERANCE
    maxOk = target + RETRACTION_TOLERANCE
    if dist<minOk or dist>maxOk:
        return

    direction = "BUY" if isBuy else "SELL"
    st["activeTrade"] = {
        "direction": direction,
        "entryPrice": currentPrice
    }
    sess.tradeOpened = True

##############################################################################
# Session & Time Management
##############################################################################
def handle_session_logic(dt: datetime, currentPrice: float, sess: SessionData, allow: bool):
    """Start or end session based on dt.time(), if allow is True."""
    if not allow:
        # forcibly end if was active
        if sess.active:
            end_session(sess)
        return

    if is_within_session(dt, sess.start[0], sess.start[1], sess.end[0], sess.end[1]):
        if not sess.active:
            start_session(sess, currentPrice)
    else:
        if sess.active:
            end_session(sess)

##############################################################################
# MAIN BACKTEST
##############################################################################
def run_backtest(csvFile: str, years: list):
    st = strategyState

    with open(csvFile, "r", newline="") as f:
        reader = csv.reader(f, delimiter=';')
        for row in reader:
            # row[0]=Date(DD/MM/YYYY), row[1]=Time(HH:MM:SS)
            # row[2]=Open, row[3]=High, row[4]=Low, row[5]=Close, row[6]=Volume
            if len(row)<7:
                continue  # skip invalid lines

            ddmmyyyy = row[0]
            hhmmss   = row[1]
            try:
                # parse dt
                dtStr = f"{ddmmyyyy} {hhmmss}"
                dt    = datetime.strptime(dtStr, "%d/%m/%Y %H:%M:%S")
            except:
                continue  # skip parse errors

            # filter by year
            if dt.year not in years:
                continue

            try:
                op = float(row[2])
                hi = float(row[3])
                lo = float(row[4])
                cl = float(row[5])
                vol= float(row[6])  # might not use it
            except:
                continue

            # handle session open/close
            handle_session_logic(dt, op, session1, st["allowSession1"])
            handle_session_logic(dt, op, session2, st["allowSession2"])

            # update session1 high/low if active
            if session1.active:
                if op>session1.high: session1.high=op
                if op<session1.low : session1.low =op
            if session2.active:
                if op>session2.high: session2.high=op
                if op<session2.low : session2.low =op

            # volatility filters
            check_volatility_filters(dt, op)
            # sweeps
            check_sweeps(op)
            # try open trades
            try_open_trade(op, session1)
            try_open_trade(op, session2)
            # check GSL
            check_gsl_stop(op)

    # after file ends, if a trade is still open, close it at the last known price
    if st["activeTrade"] is not None:
        lastPrice = st["activeTrade"]["entryPrice"]  # or better store final known
        close_trade(lastPrice, "EndOfBacktest")

##############################################################################
# Print Results
##############################################################################
def print_results():
    st = strategyState
    trades = st["closedTrades"]
    if not trades:
        print("No trades closed.")
        return

    netPips = 0.0
    wins=0
    losses=0

    print("\nClosed Trades:")
    for i, t in enumerate(trades, start=1):
        netPips += t["resultPips"]
        if t["resultPips"]>=0: wins+=1
        else: losses+=1
        print(f"{i}) {t['direction']} entry={t['entry']:.1f}, exit={t['exit']:.1f}, "
              f"pips={t['resultPips']:.1f}, reason={t['reason']}")

    total = len(trades)
    winRate = 0.0
    if total>0:
        winRate = (wins/total)*100.0

    print("\nSummary:")
    print(f"Total Trades: {total}")
    print(f"Wins: {wins}, Losses: {losses}, WinRate={winRate:.2f}%")
    print(f"Net Pips: {netPips:.1f}")

def main():
    args = parse_args()
    setup_logging(args.verbose)
    years = parse_year_range(args.year_range)

    run_backtest(args.csv, years)
    print_results()

if __name__=="__main__":
    main()
