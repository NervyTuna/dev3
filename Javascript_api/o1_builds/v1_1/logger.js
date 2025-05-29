"use strict";

/**
 * logger.js
 *
 * A simple daily-file logger that appends to a log file named logs/YYYY-MM-DD.log
 * whenever you call logEvent(...).
 *
 * - logEvent(type, detailsObj) => appends a line with a timestamp, type, and JSON of detailsObj.
 *   E.g. logEvent("TradeOpen", { direction: "BUY", price: 15000, reason: "distance=45" });
 *
 * Make sure you have a folder called 'logs' in your project root, or change
 * the path below to something else.
 */

const fs = require("fs");
const path = require("path");

// ensure logs directory exists
const LOG_DIR = path.join(__dirname, "logs");
if (!fs.existsSync(LOG_DIR)) {
  fs.mkdirSync(LOG_DIR);
}

// helper: returns "2025-05-29" or today's date
function getDateString() {
  const d = new Date();
  const yyyy = d.getFullYear();
  const mm = ("0" + (d.getMonth() + 1)).slice(-2);
  const dd = ("0" + d.getDate()).slice(-2);
  return `${yyyy}-${mm}-${dd}`;
}

// gets logs/YYYY-MM-DD.log
function getLogFilePath() {
  return path.join(LOG_DIR, `${getDateString()}.log`);
}

/**
 * logEvent(type, details)
 *   - type: a short string describing event type, e.g. "TradeOpen", "TradeClose", "Error", ...
 *   - details: an object with any info you want to record (price, reason, etc.)
 */
function logEvent(type, details) {
  const now = new Date().toISOString(); // e.g. "2025-05-29T08:12:33.987Z"
  const line = `[${now}] [${type}] ${JSON.stringify(details)}\n`;

  // 1) append to daily file
  fs.appendFile(getLogFilePath(), line, (err) => {
    if (err) {
      console.error("[logger] Could not write to log file:", err);
    }
  });

  // 2) also print to console for immediate feedback
  console.log(line.trim());
}

module.exports = {
  logEvent
};
