#!/usr/bin/env python3
r"""
Multi-Strategy Python simulator for GER30EA with 4 variations, logs all trades,
and produces a single-sheet Excel with multiple sections:

(1) ListAllTrades
(1B) Subtotals by (Strategy, Year)
(2) TotalsByStrategy
(3) TotalsByMonth
(4) Year x Strategy Pivot
(5) Major Metrics Table
(6) Optional “Optimal Levels Analysis”

Notable adjustments:
 - Removed “Total_hcXX” columns from the pivot table (section 4).
 - Distances: 70–99, 100–129, 130+ are recognized if trades actually open that far from session open.
 - We still compute “hc_50, hc_60, …” for the “Optimal Levels” (section 6).
 - Partial-close if 16+ min pass since peak and peakProfit in [32..35] => forcibly +32 pips
"""

import csv
import argparse
import logging
from datetime import datetime, time
from zoneinfo import ZoneInfo
import pathlib
import re
from collections import defaultdict, Counter

try:
    import openpyxl
    from openpyxl.styles import Font, PatternFill
    HAVE_OPENPYXL = True
except ImportError:
    HAVE_OPENPYXL = False

# -------------------------------------------------------------------------
# AGGREGATOR for final stats
# -------------------------------------------------------------------------
def aggregator_factory():
    return {
        "count": 0,
        "wins":  0,
        "loses": 0,
        "pips_list": [],
        "hc_50_list":  [],
        "hc_60_list":  [],
        "hc_70_list":  [],
        "hc_80_list":  [],
        "hc_90_list":  [],
        "hc_100_list": [],

        "sumDD":  0.0,
        "maxDD":  0.0,
        "sumProfitPeak": 0.0,
        "maxProfitPeak": 0.0,
    }

data = defaultdict(aggregator_factory)

# For the new major metrics table (#5), we track each strategy+year’s
# final-dist-based pips in separate lists:
def major_metrics_factory():
    return {
        "session1_list": [],
        "session2_list": [],
        "zone1_list":    [],
        "zone2_list":    [],
        "zone3_list":    [],
        "zone4_list":    [],

        "dist45_69_list":   [],
        "dist70_99_list":   [],
        "dist100_129_list": [],
        "dist130_plus_list":[]  # for finalDist >=130
    }

majorMetrics = defaultdict(major_metrics_factory)

# diag counters
open_dist_counter   = Counter()
close_reason_counter= Counter()

# -------------------------------------------------------------------------
# Common strategy config
# -------------------------------------------------------------------------
TOLERANCE        = 9.0
GSL              = 40.0
SWEEP_CLOSE      = 179.0
OVERNIGHT_LIMIT  = 200.0
MIDDAY_LIMIT     = 150.0
TIME_CLOSE_45_69 = 16
TIME_CLOSE_70_PLUS = 31

RETRACTION_TABLE = [
    (15.0, 29.9,  0, 18.0),
    (30.0, 35.9,  1,  0.0),
    (36.0, 45.9,  2,  0.0),
    (46.0, 9999.0, -1, 0.0),
]

DIST_LEVELS = [45, 70, 100, 130]

SESSION1_START = (8, 0)
SESSION1_END   = (12, 30)

SESSION2_START = (14, 30)
SESSION2_END   = (17, 16)

# ---------------------------------------------------------------------------
#  ACTIVE-TRADING ZONES  (no overlaps, IDs 1-4 preserved)
# ---------------------------------------------------------------------------

SESSION1_ZONES = [
    # id  window-start     window-end       forced-close  base  noClose
    ((8,16),  (9,5),   (9,31),  1, 45, False),   # Zone-1 45 pts
    ((9,30),  (9,45),  (10,6),  2, 45, False),   # Zone-2 45 pts
    ((10,15), (10,30), (12,31), 3, 70, False),   # Zone-3a 70 pts  (trimmed)
    ((10,30), (10,45), (12,31), 4, 70, False),   # Zone-3b 70 pts  (new)
]

SESSION2_ZONES = [
    ((14,46), (15,6),  (17,16), 1, 45, False),   # Zone-1 45 pts
    ((15,15), (15,30), (17,16), 2, 70, False),   # Zone-2a 70 pts  (trimmed)
    ((15,30), (15,45), (17,16), 3, 70, False),   # Zone-2b 70 pts  (new)
    ((15,45), (16,48), (17,16), 4, 45, False),   # Zone-4 45 pts
]

STRATEGY_NAMES = {
    1: "OriginalAll",
    2: "No15BE_EarliestZone",
    3: "No15BE_Any45Trade",
    4: "No15BE_AfterEarliestZone",
}

# -------------------------------------------------------------------------
# Strategy state
# -------------------------------------------------------------------------
class SessionData:
    def __init__(self, name, stH, stM, endH, endM, zones):
        self.name      = name
        self.startH    = stH
        self.startM    = stM
        self.endH      = endH
        self.endM      = endM
        self.active    = False
        self.dayDate   = None
        self.openPrice = None
        self.highPrice = None
        self.lowPrice  = None
        self.allowed   = True
        self.zones     = zones

class Trade:
    def __init__(self, strategyID, direction, entryPrice,
                 sessionID, zoneID, forcedClose, openTime,
                 finalDist, noCloseRules):
        self.strategyID   = strategyID
        self.direction    = direction
        self.entry        = entryPrice
        self.sessionID    = sessionID
        self.zoneID       = zoneID
        self.forcedClose  = forcedClose
        self.openTime     = openTime
        self.finalDist    = finalDist
        self.noCloseRules = noCloseRules

        self.peakHigh     = entryPrice
        self.peakLow      = entryPrice
        self.peakTime     = openTime
        self.active       = True

STRAT_STATE = {}

def init_strategy_states():
    for sid in (1,2,3,4):
        s1 = SessionData("Session1", *SESSION1_START, *SESSION1_END, SESSION1_ZONES)
        s2 = SessionData("Session2", *SESSION2_START, *SESSION2_END, SESSION2_ZONES)
        STRAT_STATE[sid] = {
            "session1": s1,
            "session2": s2,
            "activeTrades": {1: None, 2: None},
            "closedTrades": [],
            "overnightRef": None,
            "middayRef": None,
            "currentDate": None,
        }

# -------------------------------------------------------------------------
# CLI & logging
# -------------------------------------------------------------------------
def parse_args():
    ap= argparse.ArgumentParser()
    ap.add_argument("--csv", required=True)
    ap.add_argument("--year_range", default="2024")
    ap.add_argument("--verbose", action="store_true")
    ap.add_argument("--src_tz", default="America/Chicago")
    ap.add_argument("--dst_tz", default="Europe/London")
    ap.add_argument("--out_dir", default=".")
    ap.add_argument("--excel", action="store_true")
    return ap.parse_args()

def setup_logging(verbose, out_dir, yr_tag=""):
    outp= pathlib.Path(out_dir)
    outp.mkdir(exist_ok=True)
    log_name= outp/ f"mq4_multi_{yr_tag}.log"
    logging.basicConfig(
        level=(logging.DEBUG if verbose else logging.INFO),
        format="%(asctime)s %(levelname)s: %(message)s",
        handlers=[
            logging.StreamHandler(),
            logging.FileHandler(log_name, mode="w", encoding="utf-8")
        ]
    )
    logging.info(f"Logging to {log_name}")

def parse_year_range(rng):
    if "-" in rng:
        s,e= rng.split("-")
        return range(int(s), int(e)+1)
    else:
        y= int(rng)
        return range(y,y+1)

# -------------------------------------------------------------------------
def minutes_diff(d1, d2):
    return int((d2-d1).total_seconds()//60)

def daily_reset_if_new_date(dt, sid):
    st= STRAT_STATE[sid]
    old= st["currentDate"]
    new_ = dt.date()
    if old is None or old!= new_:
        st["currentDate"]= new_
        st["session1"].allowed= True
        st["session1"].active= False
        st["session2"].allowed= True
        st["session2"].active= False
        st["session1"].dayDate= new_
        st["session2"].dayDate= new_
        st["activeTrades"]= {1: None, 2: None}
        logging.debug(f"[Strategy {sid}] daily reset for {new_}")

def in_time_window(dt, h1,m1, h2,m2):
    t= dt.time()
    st= time(h1,m1)
    ed= time(h2,m2)
    if ed<st:
        return (t>=st) or (t<ed)
    else:
        return (t>=st) and (t<ed)

def handle_session(dt, price, sid, sessNum):
    st= STRAT_STATE[sid]
    sess= st["session1"] if sessNum==1 else st["session2"]
    if sess.dayDate!= dt.date() and sess.active:
        end_session(sid, sessNum)

    if not sess.allowed:
        if sess.active:
            end_session(sid, sessNum)
        return

    if in_time_window(dt, sess.startH, sess.startM, sess.endH, sess.endM):
        if not sess.active:
            start_session(sid, sessNum, price, dt)
        else:
            if price> sess.highPrice:
                sess.highPrice= price
            if price< sess.lowPrice:
                sess.lowPrice= price
    else:
        if sess.active:
            end_session(sid, sessNum)

def start_session(sid, sessNum, price, dt):
    st= STRAT_STATE[sid]
    sess= st["session1"] if sessNum==1 else st["session2"]
    sess.active= True
    sess.openPrice= price
    sess.highPrice= price
    sess.lowPrice= price
    logging.debug(f"[Strat{sid}] Session{sessNum} start @ {price:.1f}, date={dt.date()}")

def end_session(sid, sessNum):
    st= STRAT_STATE[sid]
    sess= st["session1"] if sessNum==1 else st["session2"]
    logging.debug(f"[Strat{sid}] Session{sessNum} end.")
    sess.active= False
    sess.openPrice= None
    sess.highPrice= None
    sess.lowPrice= None

def get_active_zone(dt, sid, sessNum):
    st= STRAT_STATE[sid]
    sess= st["session1"] if sessNum==1 else st["session2"]
    if not sess.active:
        return None
    for z in sess.zones:
        (stH,stM)= z[0]
        (enH,enM)= z[1]
        (fcH,fcM)= z[2]
        zID= z[3]
        bD=  z[4]
        nC=  z[5]
        if in_time_window(dt, stH,stM, enH,enM):
            forcedC= datetime(dt.year, dt.month, dt.day, fcH, fcM, tzinfo=dt.tzinfo)
            return (forcedC, zID, bD, nC)
    return None

def calc_retraction(dt, price, sid, sessNum):
    st= STRAT_STATE[sid]
    sess= st["session1"] if sessNum==1 else st["session2"]
    if not sess.active or sess.openPrice is None:
        return 0.0
    if price>= sess.openPrice:
        return sess.highPrice- price
    else:
        return price- sess.lowPrice

def retraction_lookup(r):
    for (mn,mx, skip, shift) in RETRACTION_TABLE:
        if mn<=r<=mx:
            return (skip, shift)
    return (0,0)

def pick_final_distance(baseDist, skip, shift):
    try:
        baseIdx= DIST_LEVELS.index(baseDist)
    except ValueError:
        return None
    finalIdx= baseIdx+skip
    if finalIdx>3:
        finalIdx=3
    baseVal= DIST_LEVELS[baseIdx]
    nextVal= DIST_LEVELS[finalIdx]
    shifted= baseVal+ shift
    chosen= max(shifted, nextVal)
    if chosen> (130+ TOLERANCE):
        return None
    final= None
    for i, lv in enumerate(DIST_LEVELS):
        if abs(chosen- lv)<0.5:
            final= lv
            break
        if i<3 and chosen> lv and chosen< DIST_LEVELS[i+1]:
            final= DIST_LEVELS[i+1]
            break
        if i==3 and chosen> lv:
            final= lv
            break
    return final

def try_open_trade(dt, price, sid, sessNum):
    st = STRAT_STATE[sid]
    if st["activeTrades"][sessNum] is not None:
        return
    z = get_active_zone(dt, sid, sessNum)
    if not z:
        return
    forcedC, zID, bD, nC = z

    r = calc_retraction(dt, price, sid, sessNum)
    skip, shift = retraction_lookup(r)
    if skip == -1:
        if sessNum == 1:
            st["session1"].allowed = False
        else:
            st["session2"].allowed = False
        logging.debug(f"[Strat{sid}] Session{sessNum} blocked≥46 => no trade")
        return

    sess = st["session1"] if sessNum == 1 else st["session2"]
    direction = "BUY" if price < sess.openPrice else "SELL"
    dist = abs(price - sess.openPrice)
    if dist < bD:
        return
    # --- overshoot handling ----------------------------------------------
    # If price has moved past the base distance tolerance, check if we're
    # still within the zone and inside the 100/130 bands. Overshoot can
    # therefore still lead to a trade at the next level.
    if dist > (bD + TOLERANCE):
        finalDist = None
        if dt < forcedC:
            if 100 - TOLERANCE <= dist <= 100 + TOLERANCE:
                finalDist = 100
            elif 130 - TOLERANCE <= dist <= 130 + TOLERANCE:
                finalDist = 130
        if finalDist is None:
            return
    else:
        finalDist = pick_final_distance(bD, skip, shift)
        if finalDist is None:
            return
        if dist < finalDist:
            return
        if dist > (finalDist + TOLERANCE):
            return

    # ------------------------------------------------------------------
    #  🔼  ESCALATOR – allow an immediate 100- or 130-pt trade IF:
    #       • current price is in that band (≤ +9 pts overshoot)
    #       • we are STILL inside the active zone returned earlier (forcedC not reached)
    # ------------------------------------------------------------------
    escalated = False
    for band in (100, 130):
        if band > finalDist and (band <= dist <= band + TOLERANCE):
            # Make sure we haven't left the zone’s open window
            if dt < forcedC:  # still within zone’s chrono-window
                finalDist = band
                escalated = True
                break  # pick first qualifying band
    # (optional debug)
    if escalated:
        logging.debug(f"[Strat{sid}] escalated target to {finalDist}p at dist={dist:.1f}")

    open_dist_counter[finalDist] += 1

    tr = Trade(sid, direction, price, sessNum, zID, forcedC, dt, finalDist, nC)
    st["activeTrades"][sessNum] = tr
    logging.debug(
        f"[Strat{sid}] OPEN s{sessNum} z{zID} {direction} ent={price:.1f} finalDist={finalDist}, forced={forcedC.time()}, noClose={nC}"
    )

def manage_trade(dt, price, sid, sessNum):
    st= STRAT_STATE[sid]
    tr= st["activeTrades"][sessNum]
    if not tr or not tr.active:
        return

    if dt>= tr.forcedClose:
        close_trade(tr, price, "ForcedClose", dt)
        return

    dd= abs(price- tr.entry)
    if dd>= GSL:
        close_trade(tr, price, "GSL", dt)
        return

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

    if tr.noCloseRules and tr.strategyID!=1:
        return

    # partial close: if peakProfit is in [32..35] for≥16min => forcibly +32
    if tr.finalDist>=45 and tr.finalDist<70:
        skipBreakEven= False
        if tr.strategyID==2:
            if tr.sessionID==1 and tr.zoneID==1:
                skipBreakEven= True
        elif tr.strategyID==3:
            skipBreakEven= True
        elif tr.strategyID==4:
            if not (tr.sessionID==1 and tr.zoneID==1):
                skipBreakEven= True

        if tr.direction=="BUY":
            peakProfit= tr.peakHigh- tr.entry
        else:
            peakProfit= tr.entry- tr.peakLow

        elap= minutes_diff(tr.peakTime, dt)
        if (32<= peakProfit<=35) and elap>=16:
            # forcibly close at +32
            if tr.direction=="BUY":
                forcedP= tr.entry+32
            else:
                forcedP= tr.entry-32
            close_trade(tr, forcedP, "PartialClose32", dt)
            return

        if not skipBreakEven:
            if tr.direction=="BUY":
                mae= tr.entry- tr.peakLow
            else:
                mae= tr.peakHigh- tr.entry
            if mae>=15 and abs(price- tr.entry)<1.0:
                close_trade(tr, price, "BreakEven-15", dt)
                return

        holdMins= minutes_diff(tr.peakTime, dt)
        if holdMins>= TIME_CLOSE_45_69:
            close_trade(tr, price, "TimeClose16", dt)
    else:
        holdMins= minutes_diff(tr.peakTime, dt)
        if holdMins>= TIME_CLOSE_70_PLUS:
            close_trade(tr, price, "TimeClose31", dt)

def hard_close_pips(direction, entry, final_pips, peakProfit, level):
    # if peakProfit≥level => forcibly close at +level
    if peakProfit>= level:
        return float(level)
    return float(final_pips)

def close_trade(tr, price, reason, dt):
    st= STRAT_STATE[tr.strategyID]
    pl= (price- tr.entry) if tr.direction=="BUY" else (tr.entry- price)

    if tr.direction=="BUY":
        peakProfitPips= tr.peakHigh- tr.entry
        peakDrawdownPips= tr.entry- tr.peakLow
    else:
        peakProfitPips= tr.entry- tr.peakLow
        peakDrawdownPips= tr.peakHigh- tr.entry

    hc_50=  hard_close_pips(tr.direction, tr.entry, pl, peakProfitPips, 50)
    hc_60=  hard_close_pips(tr.direction, tr.entry, pl, peakProfitPips, 60)
    hc_70=  hard_close_pips(tr.direction, tr.entry, pl, peakProfitPips, 70)
    hc_80=  hard_close_pips(tr.direction, tr.entry, pl, peakProfitPips, 80)
    hc_90=  hard_close_pips(tr.direction, tr.entry, pl, peakProfitPips, 90)
    hc_100= hard_close_pips(tr.direction, tr.entry, pl, peakProfitPips, 100)

    cdict= {
      "strategy":  tr.strategyID,
      "session":   tr.sessionID,
      "zone":      tr.zoneID,
      "dir":       tr.direction,
      "entry":     tr.entry,
      "exit":      price,
      "pips":      pl,
      "hc_50":     hc_50,
      "hc_60":     hc_60,
      "hc_70":     hc_70,
      "hc_80":     hc_80,
      "hc_90":     hc_90,
      "hc_100":    hc_100,
      "reason":    reason,
      "openTime":  tr.openTime,
      "closeTime": dt,
      "finalDist": tr.finalDist,

      "peakProfitPips":  peakProfitPips,
      "peakDrawdownPips":peakDrawdownPips,
    }
    st["closedTrades"].append(cdict)

    close_reason_counter[(tr.strategyID, reason)] +=1
    logging.info(f"[Close] Strat{tr.strategyID} s{tr.sessionID} z{tr.zoneID} {tr.direction} "
                 f"ent={tr.entry:.1f} exit={price:.1f} pips={pl:.1f} reason={reason} "
                 f"(peakProfit={peakProfitPips:.1f}, peakDD={peakDrawdownPips:.1f})")

    tr.active= False
    st["activeTrades"][tr.sessionID]= None

    # aggregator
    sid= tr.strategyID
    y= dt.year
    m= dt.month
    b= data[(sid,y,m)]
    b["count"]+=1
    if pl>0: b["wins"]+=1
    else:    b["loses"]+=1

    b["pips_list"].append(pl)
    b["hc_50_list"].append(hc_50)
    b["hc_60_list"].append(hc_60)
    b["hc_70_list"].append(hc_70)
    b["hc_80_list"].append(hc_80)
    b["hc_90_list"].append(hc_90)
    b["hc_100_list"].append(hc_100)

    b["sumDD"]+= peakDrawdownPips
    if peakDrawdownPips> b["maxDD"]: b["maxDD"]= peakDrawdownPips
    b["sumProfitPeak"]+= peakProfitPips
    if peakProfitPips> b["maxProfitPeak"]: b["maxProfitPeak"]= peakProfitPips

    # major metrics => distance buckets
    mm= majorMetrics[(sid,y)]
    # session
    if tr.sessionID==1:
        mm["session1_list"].append(pl)
    else:
        mm["session2_list"].append(pl)
    # zone
    if tr.zoneID>=1 and tr.zoneID<=4:
        mm[f"zone{tr.zoneID}_list"].append(pl)

    d_= tr.finalDist
    if 45<= d_<70:
        mm["dist45_69_list"].append(pl)
    elif 70<= d_<100:
        mm["dist70_99_list"].append(pl)
    elif 100<= d_<130:
        mm["dist100_129_list"].append(pl)
    else:
        if d_>=130:
            mm["dist130_plus_list"].append(pl)

# -------------------------------------------------------------------------
def check_sweeps(dt, price, sid, sessNum):
    st= STRAT_STATE[sid]
    tr= st["activeTrades"][sessNum]
    if not tr or not tr.active: return
    sess= st["session1"] if sessNum==1 else st["session2"]
    if sess.active and sess.openPrice is not None:
        dist= abs(price- sess.openPrice)
        if dist>= SWEEP_CLOSE:
            close_trade(tr, price, f"Sweep≥{SWEEP_CLOSE}", dt)

def check_outside_sessions(dt, price, sid, sessNum):
    s1_in= in_time_window(dt, *SESSION1_START, *SESSION1_END)
    s2_in= in_time_window(dt, *SESSION2_START, *SESSION2_END)
    if not s1_in and not s2_in:
        st= STRAT_STATE[sid]
        tr= st["activeTrades"][sessNum]
        if tr and tr.active:
            close_trade(tr, price, "OutsideSessions", dt)

def check_volatility(dt, price, sid):
    st= STRAT_STATE[sid]
    if dt.hour==17 and dt.minute==16:
        st["overnightRef"]= price
    if dt.hour==8 and dt.minute==0:
        if st["overnightRef"] is not None:
            if abs(price- st["overnightRef"])>= OVERNIGHT_LIMIT:
                st["session1"].allowed= False
                logging.debug(f"[Strat {sid}] session1 blocked≥200 overnight")
        st["overnightRef"]= None
    if dt.hour==12 and dt.minute==0:
        st["middayRef"]= price
    if dt.hour==14 and dt.minute==30:
        if st["middayRef"] is not None:
            if abs(price- st["middayRef"])>= MIDDAY_LIMIT:
                st["session2"].allowed= False
                logging.debug(f"[Strat {sid}] session2 blocked≥150 midday")
        st["middayRef"]= None

# -------------------------------------------------------------------------
def run_backtest(csv_file, yrs, src_tz, dst_tz, produce_excel=False, out_dir="."):
    init_strategy_states()

    tzSrc = ZoneInfo(src_tz)
    tzDst = ZoneInfo(dst_tz)

    f = pathlib.Path(csv_file)
    if not f.is_file():
        logging.error(f"CSV file not found: {csv_file}")
        return

    with f.open("r", newline="") as fh:
        rdr = csv.reader(fh, delimiter=';')
        naive = None
        for row in rdr:
            if len(row) < 6:
                continue
            dstr = row[0].strip()
            tstr = row[1].strip()
            dtRaw = f"{dstr} {tstr}"
            try:
                naive = datetime.strptime(dtRaw, "%d/%m/%Y %H:%M:%S")
            except:
                continue

            dtSrc = naive.replace(tzinfo=tzSrc)
            dt = dtSrc.astimezone(tzDst)
            if dt.year not in yrs:
                continue

            try:
                op = float(row[2])
                hi = float(row[3])
                lo = float(row[4])
                cl = float(row[5])
            except:
                continue

            for sid in (1, 2, 3, 4):
                daily_reset_if_new_date(dt, sid)
                # Handle specific times for session management
                if dt.hour == 8 and dt.minute == 0:
                    handle_session(dt, op, sid, 1)
                if dt.hour == 14 and dt.minute == 30:
                    handle_session(dt, op, sid, 2)

            for p in [lo, hi]:
                for sid in (1, 2, 3, 4):
                    handle_session(dt, p, sid, 1)
                    handle_session(dt, p, sid, 2)
                    check_volatility(dt, p, sid)
                    check_sweeps(dt, p, sid, 1)
                    check_sweeps(dt, p, sid, 2)
                    check_outside_sessions(dt, p, sid, 1)
                    check_outside_sessions(dt, p, sid, 2)
                    try_open_trade(dt, p, sid, 1)
                    try_open_trade(dt, p, sid, 2)
                    manage_trade(dt, p, sid, 1)
                    manage_trade(dt, p, sid, 2)

        if naive:
            last_dt = naive.replace(tzinfo=tzSrc).astimezone(tzDst)
            for sid in (1, 2, 3, 4):
                st = STRAT_STATE[sid]
                for sID, tr in st["activeTrades"].items():
                    if tr and tr.active:
                        close_trade(tr, tr.entry, "EndOfBacktest", last_dt)

    produce_summaries_and_excel_single_sheet(yrs, out_dir, produce_excel)
# -------------------------------------------------------------------------
def median(lst):
    if not lst:
        return 0.0
    s= sorted(lst)
    n= len(s)
    mid= n//2
    if n%2==1:
        return s[mid]
    return (s[mid-1]+ s[mid])/2.0

# -------------------------------------------------------------------------
def produce_summaries_and_excel_single_sheet(yrs, out_dir, produce_excel):
    if not produce_excel:
        logging.info("Excel not requested => skipping summary.")
        return
    if not HAVE_OPENPYXL:
        logging.warning("openpyxl not installed => skipping summary.")
        return

    import openpyxl
    from openpyxl.styles import Font, PatternFill

    all_keys = list(data.keys())
    if not all_keys:
        logging.info("No trades => no excel.")
        return

    sids_in = sorted({k[0] for k in all_keys})
    yrs_in = sorted({k[1] for k in all_keys})
    multi_year_str = f"{min(yrs_in)}-{max(yrs_in)}"
    date_tag = datetime.now().strftime("%Y%m%d")

    wb = openpyxl.Workbook()
    ws = wb.active
    ws.title = "Summary"

    row = 1
    # (6) “Optimal Levels Analysis”
    row += 1
    ws.cell(row=row, column=1, value="(6) Optimal Levels Analysis").font = Font(bold=True)
    row += 2

    headers_6 = [
        "Strategy", "Trades",
        "Hit_50%", "Hit_60%", "Hit_70%", "Hit_80%", "Hit_90%", "Hit_100%",
        "Net@50", "Net@60", "Net@70", "Net@80", "Net@90", "Net@100",
        "Best_TP", "Best_Net", "Median_DD", "P75_DD", "Suggested_SL_Range"
    ]
    for c_i, hh in enumerate(headers_6, 1):
        ws.cell(row=row, column=c_i, value=hh).font = Font(bold=True)
    row += 1

    def pct(a, b):
        return round(a / b * 100, 1) if b > 0 else 0.0

    for sid in sids_in:
        trades = STRAT_STATE[sid]["closedTrades"]
        n = len(trades)
        if n == 0:
            continue
        hit_ct = {L: 0 for L in (50, 60, 70, 80, 90, 100)}
        net_if = {L: 0.0 for L in (50, 60, 70, 80, 90, 100)}
        dd_list = []
        for cT in trades:
            pk = cT["peakProfitPips"]
            dd_list.append(cT["peakDrawdownPips"])
            for L in (50, 60, 70, 80, 90, 100):
                net_if[L] += cT[f"hc_{L}"]
                if pk >= L:
                    hit_ct[L] += 1

        from statistics import median as stat_median
        dd_list.sort()
        med_dd = stat_median(dd_list) if dd_list else 0
        i75 = int(0.75 * len(dd_list))
        p75 = dd_list[i75] if i75 < len(dd_list) else med_dd
        hint_ = f"{round(med_dd)}–{round(p75)}"

        best_L, best_val = max(net_if.items(), key=lambda kv: kv[1])

        c = 1
        ws.cell(row=row, column=c, value=STRATEGY_NAMES[sid])
        c += 1
        ws.cell(row=row, column=c, value=n)
        c += 1
        for L in (50, 60, 70, 80, 90, 100):
            ws.cell(row=row, column=c, value=pct(hit_ct[L], n))
            c += 1
        for L in (50, 60, 70, 80, 90, 100):
            cell = ws.cell(row=row, column=c, value=round(net_if[L], 1))
            if L == best_L:
                cell.fill = PatternFill(start_color="C6EFCE", end_color="C6EFCE", fill_type="solid")
            c += 1

        ws.cell(row=row, column=c, value=best_L).fill = PatternFill(start_color="C6EFCE", end_color="C6EFCE", fill_type="solid")
        c += 1
        ws.cell(row=row, column=c, value=round(best_val, 1)).fill = PatternFill(start_color="C6EFCE", end_color="C6EFCE", fill_type="solid")
        c += 1
        ws.cell(row=row, column=c, value=round(med_dd, 1))
        c += 1
        ws.cell(row=row, column=c, value=round(p75, 1))
        c += 1

        row += 1

    outp= pathlib.Path(out_dir)/ f"results_{date_tag}.xlsx"
    wb.save(outp)
    logging.info(f"Excel file saved => {outp}")


def main():
    args= parse_args()
    yrs= parse_year_range(args.year_range)
    yr_tag= re.sub(r'[^0-9\-]+','_', args.year_range)
    setup_logging(args.verbose, args.out_dir, yr_tag)

    run_backtest(
        csv_file=args.csv,
        yrs= yrs,
        src_tz= args.src_tz,
        dst_tz= args.dst_tz,
        produce_excel= args.excel,
        out_dir= args.out_dir
    )
    logging.info("All years complete.")

    print("\n=== Diagnostics ===")
    print("Opened trades by finalDist:", open_dist_counter)
    print("Close reasons by strategy:")
    for (theSid, theReason), c_ in sorted(close_reason_counter.items()):
        print(f"  Strat{theSid} – {theReason}: {c_}")

    # Clear to avoid double-counting if script is re-run
    data.clear()
    open_dist_counter.clear()
    close_reason_counter.clear()
    majorMetrics.clear()

if __name__=="__main__":
    main()
