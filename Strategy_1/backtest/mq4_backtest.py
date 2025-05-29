#!/usr/bin/env python3
"""
Python simulator matching the *final* 'GER30EA.mq4' brief exactly,
with time zone conversion from --src_tz (America/Chicago) to --dst_tz (Europe/London),
limited to the year 2024 by default, listing only the trades that opened/closed in 2024.

Usage Examples:
  # 1) Just 2024 (default)
  python ger30_backtest.py --csv dax-1m.csv --verbose

  # 2) Another single year
  python ger30_backtest.py --csv dax-1m.csv --year_range 2023 --verbose

  # 3) A multi-year range
  python ger30_backtest.py --csv dax-1m.csv --year_range 2020-2025 --verbose
"""

import csv
import argparse
import logging
from datetime import datetime, time
import math
from zoneinfo import ZoneInfo
import os
import re
import pathlib

# -------------------------------------------------------------------------
# CONFIG (identical to your final logic)
# -------------------------------------------------------------------------
TOLERANCE   = 9.0
GSL         = 40.0
SWEEP_CLOSE = 179.0
OVERNIGHT_LIMIT = 200.0
MIDDAY_LIMIT    = 150.0

TIME_CLOSE_45_69   = 16
TIME_CLOSE_70_PLUS = 31

# Dynamic spread schedule (index points)
SPREAD_SCHEDULE = [
    ((1, 15),  (8, 0),  4.0),
    ((8, 0),   (9, 0),  2.0),
    ((9, 0),   (17, 30), 1.2),
    ((17, 30), (22, 0), 2.0),
    ((22, 0),  (23, 59), 5.0),
    ((0, 0),   (1, 15), 5.0),
]
DEFAULT_SPREAD = 5.0

RETRACTION_TABLE = [
    (15.0, 29.9, 0, 18.0),
    (30.0, 35.9, 1, 0.0),
    (36.0, 45.9, 2, 0.0),
    (46.0, 9999.0, -1, 0.0),
]

DIST_LEVELS = [45, 70, 100, 130]

SESSION1_START = (8, 0)
SESSION1_END   = (12, 30)

SESSION2_START = (14, 30)
SESSION2_END   = (17, 16)

SESSION1_ZONES = [
    ((8, 16),  (9, 5),  (9, 31), 1, 45, True),   # noCloseRules => skip 16/31
    ((9, 30),  (9, 45), (10, 6), 2, 45, False),
    ((10, 15), (10, 45), (12, 31), 3, 70, False),
    ((10, 45), (11, 45), (12, 31), 4, 45, False),
]
SESSION2_ZONES = [
    ((14, 46), (15, 6), (17, 16), 1, 45, False),
    ((15, 15), (15, 45), (17, 16), 2, 70, False),
    ((15, 45), (16, 48), (17, 16), 3, 45, False),
]

# -------------------------------------------------------------------------
class SessionData:
    def __init__(self, name, stH, stM, endH, endM, zones):
        self.name = name
        self.startH = stH
        self.startM = stM
        self.endH   = endH
        self.endM   = endM
        self.active = False
        self.dayDate  = None
        self.openPrice = None
        self.highPrice = None
        self.lowPrice  = None
        self.allowed   = True
        self.zones     = zones
        self.used      = set()

class Trade:
    def __init__(self, direction, entryPrice, sessionID, zoneID,
                 forcedClose, openTime, finalDist, noClose):
        self.direction   = direction  # "BUY" or "SELL"
        self.entry       = entryPrice
        self.sessionID   = sessionID
        self.zoneID      = zoneID
        self.forcedClose = forcedClose
        self.openTime    = openTime
        self.finalDist   = finalDist
        self.noCloseRules = noClose
        self.peakHigh    = entryPrice
        self.peakLow     = entryPrice
        self.peakTime    = openTime
        self.active      = True

# -------------------------------------------------------------------------
# Global "engine" state
# -------------------------------------------------------------------------
state = {
    "session1": SessionData("Session1", *SESSION1_START, *SESSION1_END, SESSION1_ZONES),
    "session2": SessionData("Session2", *SESSION2_START, *SESSION2_END, SESSION2_ZONES),
    "activeTrades": {1: None, 2: None},
    "closedTrades": [],
    "overnightRef": None,
    "middayRef": None
}

# -------------------------------------------------------------------------
def get_spread(dt):
    """Return spread value for the given datetime."""
    t = dt.time()
    for (st, en, val) in SPREAD_SCHEDULE:
        st_t = time(*st)
        en_t = time(*en)
        if st_t <= en_t:
            if st_t <= t <= en_t:
                return val
        else:  # wrap midnight
            if t >= st_t or t <= en_t:
                return val
    return DEFAULT_SPREAD

def parse_args():
    ap = argparse.ArgumentParser()
    ap.add_argument("--csv", required=True,
                    help="CSV file with D/M/Y;H:M:S;O;H;L;C")
    ap.add_argument("--year_range", default="2024",
                    help="e.g. 2024 or 2020-2025 (default=2024)")
    ap.add_argument("--verbose", action="store_true")
    ap.add_argument("--src_tz", default="America/Chicago",
                    help="Time zone of CSV data (default=America/Chicago).")
    ap.add_argument("--dst_tz", default="Europe/London",
                    help="Time zone to convert to (default=Europe/London).")
    ap.add_argument("--log_dir", default=".",
                    help="Directory for log files (default=current directory).")
    ap.add_argument("--quiet_debug", action="store_true",
                    help="Suppress per-row DEBUG output even when --verbose.")
    return ap.parse_args()

def setup_logging(verbose: bool, quiet_debug: bool, log_path):
    class SimTimeFormatter(logging.Formatter):
        def format(self, record):
            record.sim_time = getattr(record, "sim_time", "")
            return super().format(record)

    fmt = SimTimeFormatter("[%(sim_time)s] %(levelname)s: %(message)s")

    # Console handler
    con = logging.StreamHandler()
    con.setFormatter(fmt)
    con.setLevel(logging.DEBUG if verbose and not quiet_debug else logging.INFO)

    # File handler
    to_file = logging.FileHandler(log_path, mode="w", encoding="utf-8")
    to_file.setFormatter(fmt)
    to_file.setLevel(logging.INFO)

    root = logging.getLogger()
    root.handlers.clear()
    root.addHandler(con)
    root.addHandler(to_file)
    root.setLevel(logging.DEBUG)  # Let handlers filter levels

def handle_daily_reset(dt):
    """Reset session allowed flags at the start of a new day."""
    current_date = dt.date()
    if 'currentDate' not in state or state['currentDate'] != current_date:
        state['currentDate'] = current_date
        state["session1"].allowed = True
        state["session2"].allowed = True
        state["session1"].used = set()
        state["session2"].used = set()
        state["activeTrades"] = {1: None, 2: None}
        logging.debug(f"Daily reset: {current_date} - sessions allowed.")

def parse_year_range(rng):
    """Parse year range or default to ALL years."""
    if rng.lower() == "all":
        return None  # Process all years
    if "-" in rng:
        start, end = rng.split("-")
        return range(int(start), int(end) + 1)
    else:
        y = int(rng)
        return range(y, y + 1)

# -------------------------------------------------------------------------
def in_time_window(dt, h1,m1, h2,m2):
    t = dt.time()
    st= time(h1,m1)
    ed= time(h2,m2)
    if ed<st:
        return (t>=st) or (t<ed)
    else:
        return (t>=st) and (t<ed)

def minutes_diff(d1,d2):
    return int((d2 - d1).total_seconds() // 60)

def handle_session(dt, price, sess: SessionData, sID: int):
    dDate = dt.date()
    if sess.dayDate != dDate and sess.active:
        end_session(sess)

    if not sess.allowed:
        if sess.active:
            end_session(sess)
        return

    if in_time_window(dt, sess.startH, sess.startM, sess.endH, sess.endM):
        if not sess.active:
            start_session(sess, price, dDate, dt)  # Pass dt here
        else:
            if price > sess.highPrice:
                sess.highPrice = price
            if price < sess.lowPrice:
                sess.lowPrice = price
    else:
        if sess.active:
            end_session(sess)

def start_session(sess: SessionData, price, dDate, dt):
    sess.active = True
    sess.dayDate = dDate
    sess.openPrice = price
    sess.highPrice = price
    sess.lowPrice = price
    logging.debug(
        f"Session1 start at {price:.2f}",
        extra={"sim_time": fmt_sim_time(dt)}
    )

def end_session(sess: SessionData):
    logging.debug(f"{sess.name} end.")
    sess.active    = False
    sess.dayDate   = None
    sess.openPrice = None
    sess.highPrice = None
    sess.lowPrice  = None

def get_active_zone(dt, sess: SessionData):
    if not sess.active: return None
    for z in sess.zones:
        (stH, stM)= z[0]
        (enH, enM)= z[1]
        (fcH,fcM)= z[2]
        zID      = z[3]
        baseDist = z[4]
        noClose  = z[5]

        if in_time_window(dt, stH, stM, enH, enM):
            forcedC = datetime(dt.year, dt.month, dt.day, fcH, fcM, tzinfo=dt.tzinfo)
            return (forcedC, zID, baseDist, noClose)
    return None

def calc_retraction(sess: SessionData, price):
    if not sess.active or sess.openPrice is None:
        return 0.0
    if price >= sess.openPrice:
        return sess.highPrice - price
    else:
        return price - sess.lowPrice

def retraction_lookup(r):
    for (mn,mx, skip, shift) in RETRACTION_TABLE:
        if mn <= r <= mx:
            return (skip, shift)
    return (0,0)

def pick_final_distance(baseDist, skip, shift):
    try:
        baseIdx= DIST_LEVELS.index(baseDist)
    except ValueError:
        return None
    finalIdx= baseIdx + skip
    if finalIdx>3: finalIdx=3

    baseVal = DIST_LEVELS[baseIdx]
    nextVal = DIST_LEVELS[finalIdx]
    shifted= baseVal + shift
    chosen = max(shifted, nextVal)

    if chosen > (130 + TOLERANCE):
        return None

    finalDist= None
    for i, lv in enumerate(DIST_LEVELS):
        if abs(chosen - lv) < 0.5:
            finalDist= lv
            break
        if i<3 and chosen>lv and chosen<DIST_LEVELS[i+1]:
            finalDist= DIST_LEVELS[i+1]
            break
        if i==3 and chosen>lv:
            finalDist= lv
            break
    return finalDist

def try_open_trade(dt, price, sess: SessionData, sID: int):
    if state["activeTrades"][sID] is not None:
        return
    z = get_active_zone(dt, sess)
    if z is None:
        return
    forcedC, zoneID, baseDist, noClose = z

    r = calc_retraction(sess, price)
    skip, shift = retraction_lookup(r)
    if skip == -1:
        sess.allowed = False
        logging.debug(f"{sess.name} blocked≥46, no trade => session canceled.")
        return

    direction = "BUY" if price < sess.openPrice else "SELL"
    dist = abs(price - sess.openPrice)
    if dist < baseDist:
        return
    if dist > (baseDist + TOLERANCE):
        return

    finalDist = pick_final_distance(baseDist, skip, shift)
    if finalDist is None:
        return
    if dist < finalDist:
        return
    if dist > (finalDist + TOLERANCE):
        return

    tr = Trade(direction, price, sID, zoneID, forcedC, dt, finalDist, noClose)
    state["activeTrades"][sID] = tr

    logging.debug(  # Changed from logging.info to logging.debug
        "OPEN  s%d z%d %-4s entry=%.1f final=%d  forced=%s  noClose=%s",
        sID, zoneID, direction, price, finalDist,
        forcedC.time(), noClose,
        extra={"sim_time": fmt_sim_time(dt)}
    )

def manage_trade(dt, price):
    for sID, tr in state["activeTrades"].items():
        if not tr or not tr.active:
            continue

        # forced close
        if dt >= tr.forcedClose:
            close_trade(tr, price, "ForcedClose", dt)
            continue

        dd= abs(price- tr.entry)
        if dd >= GSL:
            close_trade(tr, price, "GSL", dt)
            continue

        # update peaks
        if tr.direction=="BUY":
            if price> tr.peakHigh:
                tr.peakHigh= price
                tr.peakTime= dt
            if price< tr.peakLow:
                tr.peakLow= price
                tr.peakTime= dt
        else:
            if price< tr.peakLow:
                tr.peakLow= price
                tr.peakTime= dt
            if price> tr.peakHigh:
                tr.peakHigh= price
                tr.peakTime= dt

        if tr.noCloseRules:
            continue

        if tr.finalDist>=45 and tr.finalDist<70:
            adv= (tr.entry- price) if tr.direction=="BUY" else (price- tr.entry)
            if adv>=15:
                diff= abs(price- tr.entry)
                if diff<1.0:
                    # Break-even
                    close_trade(tr, price, "BreakEven15", dt)
                    continue
            holdMins= minutes_diff(tr.peakTime, dt)
            if holdMins>= TIME_CLOSE_45_69:
                # Time-based exits
                close_trade(tr, price, "TimeClose16", dt)
        else:
            holdMins= minutes_diff(tr.peakTime, dt)
            if holdMins>= TIME_CLOSE_70_PLUS:
                close_trade(tr, price, "TimeClose31", dt)

def close_trade(tr: Trade, price, reason, dt):
    pl = (price - tr.entry) if tr.direction == "BUY" else (tr.entry - price)
    state["closedTrades"].append({
        "session": tr.sessionID,
        "zone":    tr.zoneID,
        "dir":     tr.direction,
        "entry":   tr.entry,
        "exit":    price,
        "pips":    pl,
        "reason":  reason,
        "openTime":  tr.openTime,
        "closeTime": dt
    })

    logging.info(
        "TRADE s%(session)d z%(zone)d %(dir)-4s "
        "entry=%(entry).1f exit=%(exit).1f pips=%(pips).1f "
        "final=%(final)d forced=%(forced)s noClose=%(nc)s "
        "reason=%(reason)s  open=%(open)s close=%(close)s",
        {
            "session":  tr.sessionID,
            "zone":     tr.zoneID,
            "dir":      tr.direction,
            "entry":    tr.entry,
            "exit":     price,
            "pips":     pl,
            "final":    tr.finalDist,
            "forced":   tr.forcedClose.strftime("%H:%M"),
            "nc":       tr.noCloseRules,
            "reason":   reason,
            "open":     tr.openTime.strftime("%Y-%m-%d %H:%M"),
            "close":    dt.strftime("%Y-%m-%d %H:%M"),
        },
        extra={"sim_time": fmt_sim_time(dt)}
    )
    tr.active = False
    state["activeTrades"][tr.sessionID] = None

def check_sweeps(dt, price):
    for sID, tr in state["activeTrades"].items():
        if not tr or not tr.active:
            continue
        if tr.sessionID==1:
            sess= state["session1"]
        else:
            sess= state["session2"]

        if sess.active and sess.openPrice is not None:
            dist= abs(price- sess.openPrice)
            if dist>= SWEEP_CLOSE:
                reason = f"Sweep≥{SWEEP_CLOSE} s{tr.sessionID}"
                close_trade(tr, price, reason)

def check_outside_sessions(dt, price):
    s1_in= in_time_window(dt, *SESSION1_START, *SESSION1_END)
    s2_in= in_time_window(dt, *SESSION2_START, *SESSION2_END)
    if not s1_in and not s2_in:
        tr= state["activeTrade"]
        if tr and tr.active:
            close_trade(tr, price, "OutsideSessions", dt)

def check_volatility(dt, price):
    # overnight
    if dt.hour==17 and dt.minute==16:
        state["overnightRef"]= price
    if dt.hour==8 and dt.minute==0:
        if state["overnightRef"] is not None:
            if abs(price- state["overnightRef"])>= OVERNIGHT_LIMIT:
                state["session1"].allowed= False
                logging.debug("session1 blocked by overnight≥200")
        state["overnightRef"]= None

    # midday
    if dt.hour==12 and dt.minute==0:
        state["middayRef"]= price
    if dt.hour==14 and dt.minute==30:
        if state["middayRef"] is not None:
            if abs(price- state["middayRef"])>= MIDDAY_LIMIT:
                state["session2"].allowed= False
                logging.debug("session2 blocked by midday≥150")
        state["middayRef"]= None

def run_backtest(csvPath, yrs, srcTz, dstTz, quiet=False):
    # Build ZoneInfo objects for your time zones
    tzSrc = ZoneInfo(srcTz)
    tzDst = ZoneInfo(dstTz)

    DEBUG_LIMIT = 20  # Number of rows to debug
    line_count = 0    # Initialize line counter

    with open(csvPath, "r", newline="") as f:
        rdr = csv.reader(f, delimiter=';')
        for row in rdr:
            if len(row) < 6:
                continue
            dstr = row[0].strip()  # dd/mm/yyyy
            tstr = row[1].strip()  # hh:mm:ss
            dtRaw = f"{dstr} {tstr}"
            try:
                # naive parse
                naive = datetime.strptime(dtRaw, "%d/%m/%Y %H:%M:%S")
            except:
                continue

            # interpret as srcTz
            dtSrc = naive.replace(tzinfo=tzSrc)
            # convert to London
            dt = dtSrc.astimezone(tzDst)

            # Year filtering
            if yrs and dt.year not in yrs:
                continue

            try:
                op = float(row[2])
                hi = float(row[3])
                lo = float(row[4])
                cl = float(row[5])
            except:
                continue

            price = op

            # Debug the first few rows
            line_count += 1
            if not quiet and line_count <= DEBUG_LIMIT:
                logging.debug(
                    f"LINE {line_count}: "
                    f"naive={naive} "
                    f"dtSrc={dtSrc} "
                    f"dtDst={dt} "
                    f"price={price:.2f}"
                )

            # Daily reset of session flags
            handle_daily_reset(dt)

            # Continue with existing logic
            handle_session(dt, price, state["session1"], 1)
            handle_session(dt, price, state["session2"], 2)
            check_volatility(dt, price)
            check_sweeps(dt, price)
            check_outside_sessions(dt, price)
            if state["session1"].active:
                try_open_trade(dt, price, state["session1"], 1)
            if state["session2"].active:
                try_open_trade(dt, price, state["session2"], 2)
            manage_trade(dt, price)

    for tr in state["activeTrades"].values():
        if tr and tr.active:
            close_trade(tr, tr.entry, "EndOfBacktest", dt)

def print_summary():
    closed = state["closedTrades"]
    if not closed:
        print("No trades closed.")
        return

    net= 0.0
    wins=0
    loses=0
    for i,t in enumerate(closed,1):
        net+= t["pips"]
        if t["pips"]>=0: wins+=1
        else: loses+=1
        print(f"{i}) S{t['session']}Z{t['zone']} {t['dir']} "
              f"ent={t['entry']:.1f} exit={t['exit']:.1f} pips={t['pips']:.1f} {t['reason']}")
    total= len(closed)
    wr= (wins/total*100.0) if total>0 else 0.0
    print(f"\nTrades={total}, net={net:.1f}, wins={wins}, loses={loses}, WR={wr:.1f}%")

def fmt_sim_time(dt, src_tz="America/Chicago"):
    """
    Return a string like '2019-03-28 09:45 (+5)'.

    dt       – aware datetime in the *destination* zone (Europe/London)
    src_tz   – IANA name of the original CSV zone (default = America/Chicago)
    """
    src_dt = dt.astimezone(ZoneInfo(src_tz))
    # London offset minus Chicago offset → 5 or 6 depending on DST mismatch
    diff_hrs = int((dt.utcoffset() - src_dt.utcoffset()).total_seconds() // 3600)
    sign     = "+" if diff_hrs >= 0 else "-"
    return f"{dt.strftime('%Y-%m-%d %H:%M')} ({sign}{abs(diff_hrs)})"

def main():
    args = parse_args()

    # Convert year_range to a valid filename tag
    yr_tag = re.sub(r'[^0-9]+', '_', args.year_range).strip('_').lower()
    log_dir = pathlib.Path(args.log_dir)
    log_dir.mkdir(exist_ok=True)  # Create logs directory if it doesn't exist
    log_name = log_dir / f"mq4_backtest_{yr_tag}.log"

    setup_logging(args.verbose, args.quiet_debug, log_name)
    yrs = parse_year_range(args.year_range)

    # Warn if default year range is used
    if not args.year_range or args.year_range == "2024":
        logging.warning("Default year_range=2024 used. Pass --year_range ALL for all years.")

    quiet = not args.verbose  # Suppress debug output if not verbose
    run_backtest(args.csv, yrs, args.src_tz, args.dst_tz, quiet)
    print_summary()

if __name__ == "__main__":
    main()
