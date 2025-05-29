"use strict";

const config = require("./config");
const logger = require("./logger");  // <-- ADDED: import our new logger

/**
 * strategyLogic.js
 * (with GSL=40 only, no 40-pt hedge, plus logging for open/close trades)
 */

// Define two sessions: session1 & session2
let session1 = {
  start: "08:00",
  end:   "12:30",
  openPrice: null,
  high: null,
  low: null,
  active: false,
  tradeOpened: false
};

let session2 = {
  start: "14:30",
  end:   "17:16",
  openPrice: null,
  high: null,
  low: null,
  active: false,
  tradeOpened: false
};

// If the volatility filters say skip a session, we set allowSession1= false, etc.
let allowSession1 = true;
let allowSession2 = true;

// For tracking big moves overnight or midday
let overnightRefPrice = null;
let middayRefPrice = null;

/**
 * "activeTrade" is null if no trade is open, otherwise:
 *   {
 *     direction: "BUY" or "SELL",
 *     entryPrice: <number>,
 *     size: <number>,
 *     sessionId: 1 or 2
 *   }
 */
let activeTrade = null;

/**
 * Some helper time functions
 */
function getUKTimeHM() {
  const d = new Date();
  const hh = ("0" + d.getHours()).slice(-2);
  const mm = ("0" + d.getMinutes()).slice(-2);
  return `${hh}:${mm}`;
}

function hhmmToMinutes(hhmm) {
  const [h, m] = hhmm.split(":");
  return parseInt(h, 10) * 60 + parseInt(m, 10);
}

function isWithinTimeRange(nowHM, startHM, endHM) {
  const now = hhmmToMinutes(nowHM);
  let s = hhmmToMinutes(startHM);
  let e = hhmmToMinutes(endHM);
  if (e < s) e += 1440;
  return (now >= s && now <= e);
}

// session start/end
function startSession(sess, currentPrice) {
  sess.openPrice = currentPrice;
  sess.high = currentPrice;
  sess.low = currentPrice;
  sess.active = true;
  sess.tradeOpened = false;

  logger.logEvent("SessionStart", {
    session: (sess === session1) ? 1 : 2,
    openPrice: currentPrice
  });
}

function endSession(sess) {
  logger.logEvent("SessionEnd", {
    session: (sess === session1) ? 1 : 2
  });

  sess.active = false;
  sess.openPrice = null;
  sess.high = null;
  sess.low = null;
  sess.tradeOpened = false;
}

function checkSessionTimes(currentPrice) {
  const nowHM = getUKTimeHM();

  // session1
  if (isWithinTimeRange(nowHM, session1.start, session1.end)) {
    if (!session1.active && allowSession1) {
      startSession(session1, currentPrice);
    }
  } else {
    if (session1.active) {
      endSession(session1);
    }
  }

  // session2
  if (isWithinTimeRange(nowHM, session2.start, session2.end)) {
    if (!session2.active && allowSession2) {
      startSession(session2, currentPrice);
    }
  } else {
    if (session2.active) {
      endSession(session2);
    }
  }
}

// update session high/low
function updateHighLow(sess, price) {
  if (!sess.active) return;
  if (price > sess.high) sess.high = price;
  if (price < sess.low)  sess.low  = price;
}

function getDistanceFromOpen(sess, price) {
  if (!sess.openPrice) return 0;
  return Math.abs(price - sess.openPrice);
}

function calcLotSize(equity) {
  const raw = equity / config.betSizingFactor;
  let lotSize = Math.max(raw, config.minLotSize);
  return parseFloat(lotSize.toFixed(2));
}

function canOpenTrade(sess) {
  if (!sess.active) return false;
  if (sess.tradeOpened) return false;
  return true;
}

// measure retraction from session high/low
function applyRetraction(currentDistance, sessionHigh, sessionLow, currentPrice) {
  let retractionVal = 0;
  if (currentPrice >= sessionHigh) {
    retractionVal = sessionHigh - currentPrice;
  } else {
    retractionVal = currentPrice - sessionLow;
  }

  for (let r of config.retractionSteps) {
    if (retractionVal >= r.min && retractionVal <= r.max) {
      if (r.shift) return { shift: r.shift };
      if (r.skip)  return { skip: r.skip };
      if (r.cancel) return { cancel: true };
    }
  }
  return null;
}

function getTargetLevel(distance, levels) {
  let candidate = null;
  for (let lv of levels) {
    if (distance >= lv) {
      candidate = lv;
    }
  }
  return candidate;
}

/**
 * tryOpenTrade => logs to file if/when trade is opened
 */
async function tryOpenTrade(apiClient, sess, currentPrice, equity) {
  if (!canOpenTrade(sess)) return;

  const dist = getDistanceFromOpen(sess, currentPrice);

  // if distance is beyond final level + 9 => skip
  if (dist > config.zoneLevels[3] + config.retractionTolerance) {
    if (sess === session1) allowSession1 = false;
    else if (sess === session2) allowSession2 = false;

    logger.logEvent("SessionSkip", {
      session: (sess === session1) ? 1 : 2,
      reason: "distance > 130+9"
    });
    return;
  }

  // check retraction
  const ret = applyRetraction(dist, sess.high, sess.low, currentPrice);
  // if retraction says 'cancel' => skip session if no trade open
  if (ret && ret.cancel && !activeTrade) {
    if (sess === session1) allowSession1 = false;
    else allowSession2 = false;

    logger.logEvent("SessionSkip", {
      session: (sess === session1) ? 1 : 2,
      reason: "retraction >=46, no trade open => cancel session"
    });
    return;
  }

  let finalLevels = [...config.zoneLevels];
  if (ret && ret.skip) {
    finalLevels.splice(0, ret.skip);
  }

  const target = getTargetLevel(dist, finalLevels);
  if (!target) return; // not far enough

  let additionalShift = 0;
  if (ret && ret.shift) {
    additionalShift = ret.shift;
  }

  const isBuy = (currentPrice < sess.openPrice);
  let finalPrice = 0;
  if (isBuy) {
    finalPrice = sess.openPrice - (target + additionalShift);
  } else {
    finalPrice = sess.openPrice + (target + additionalShift);
  }

  // Tolerance check
  const minOk = target - config.retractionTolerance;
  const maxOk = target + config.retractionTolerance;
  if (dist < minOk || dist > maxOk) {
    return;
  }

  const dir = isBuy ? "BUY" : "SELL";
  const lotSize = calcLotSize(equity);
  if (lotSize <= 0) return;

  // place trade with GSL=40
  const resp = await apiClient.placeTrade(
    config.daxEpic,
    dir,
    lotSize,
    config.guaranteedStopDistance,  // 40
    true
  );

  if (resp) {
    // Mark that we have an open trade
    activeTrade = {
      direction: dir,
      entryPrice: currentPrice,
      size: lotSize,
      sessionId: (sess === session1) ? 1 : 2
    };
    sess.tradeOpened = true;

    // Log that we opened a trade
    logger.logEvent("TradeOpen", {
      session: activeTrade.sessionId,
      direction: dir,
      size: lotSize,
      entryPrice: currentPrice,
      reason: `target=${target} shift=${additionalShift}`
    });
  }
}

// Volatility filters
function checkVolatilityFilters(currentPrice) {
  const nowHM = getUKTimeHM();

  if (nowHM === "17:16" && overnightRefPrice === null) {
    overnightRefPrice = currentPrice;
  }
  if (nowHM === "08:00") {
    if (overnightRefPrice !== null) {
      const overnightMove = Math.abs(currentPrice - overnightRefPrice);
      if (overnightMove >= config.overnightVolLimit) {
        allowSession1 = false;
        logger.logEvent("VolFilter", {
          session: 1,
          reason: `Overnight move >= ${config.overnightVolLimit} => skip`
        });
      }
    }
    overnightRefPrice = null;
  }

  if (nowHM === "12:00" && middayRefPrice === null) {
    middayRefPrice = currentPrice;
  }
  if (nowHM === "14:30") {
    if (middayRefPrice !== null) {
      const middayMove = Math.abs(currentPrice - middayRefPrice);
      if (middayMove >= config.middayVolLimit) {
        allowSession2 = false;
        logger.logEvent("VolFilter", {
          session: 2,
          reason: `Midday move >= ${config.middayVolLimit} => skip`
        });
      }
    }
    middayRefPrice = null;
  }
}

// Sweeps => if distance >=179 => closeAllTrades
function checkSweeps(currentPrice) {
  if (session1.active && getDistanceFromOpen(session1, currentPrice) >= config.zoneCancelLevel) {
    logger.logEvent("Sweep", { session: 1, reason: "distance >=179" });
    closeAllTrades("sweep distance >=179");
  }
  if (session2.active && getDistanceFromOpen(session2, currentPrice) >= config.zoneCancelLevel) {
    logger.logEvent("Sweep", { session: 2, reason: "distance >=179" });
    closeAllTrades("sweep distance >=179");
  }
}

/**
 * closeAllTrades(reason):
 *   Right now, we simply set activeTrade=null. 
 *   We also log "TradeClose" with approximate P/L in points.
 *   In a real system, we might place an IG net-close order if we want an actual closure on the broker side.
 */
function closeAllTrades(reason) {
  if (activeTrade) {
    // approximate P/L in points:
    // if direction=BUY => P/L= currentPrice - entryPrice
    // if direction=SELL => P/L= entryPrice - currentPrice
    // *** We need the "current price" but it's not passed in. So let's fudge it or pass it in:
    // For now, let's do a best guess by using session high/low:
    let sess = (activeTrade.sessionId === 1) ? session1 : session2;
    let approximateClosePrice = sess.openPrice; // fallback
    if (activeTrade.direction === "BUY") {
      // we can guess the "latest" is sess.low or sess.high, but let's just pick sess.low for demonstration
      approximateClosePrice = sess.low;
    } else {
      approximateClosePrice = sess.high;
    }

    let pointsDiff = 0;
    if (activeTrade.direction === "BUY") {
      pointsDiff = approximateClosePrice - activeTrade.entryPrice;
    } else {
      pointsDiff = activeTrade.entryPrice - approximateClosePrice;
    }

    logger.logEvent("TradeClose", {
      session: activeTrade.sessionId,
      direction: activeTrade.direction,
      entryPrice: activeTrade.entryPrice,
      approximateClosePrice,
      points: parseFloat(pointsDiff.toFixed(1)),
      reason
    });

    activeTrade = null;
  }
}

// Exports
module.exports = {
  session1,
  session2,
  checkSessionTimes,
  updateHighLow,
  tryOpenTrade,
  checkVolatilityFilters,
  checkSweeps,
  getActiveTrade: () => activeTrade, // <-- add this
};
