"use strict";

/**
 * server.js
 *
 * Launches an Express server that:
 *  - Serves an EJS template for displaying logs, open/closed trades, account info, and a chart.
 *  - Periodically reads from the current day's log file.
 *  - Accesses the in-memory "activeTrade" from strategyLogic to show open positions.
 *
 * NOTE: In a real production environment, you'd unify this with your main loop
 * in index.js or run them side by side. For a quick approach, we can simply
 * require("./index.js") here so it starts the strategy too.
 *
 * Also uses Socket.io to update the front-end in real time if desired.
 */

const express = require("express");
const path = require("path");
const fs = require("fs");
const http = require("http");
const socketIO = require("socket.io");
const logger = require("./logger");
const strategy = require("./strategyLogic");
const config = require("./config");

// Optionally import your main strategy runner so it starts up
require("./index.js"); // This means index.js will run on startup, launching your strategy loop.

const app = express();
const server = http.createServer(app);
const io = socketIO(server); // for real-time updates

// Configure Express to use EJS templates (placed in /views folder)
app.set("view engine", "ejs");
app.set("views", path.join(__dirname, "views"));

// Serve static files (CSS, JS) from /public
app.use(express.static(path.join(__dirname, "public")));

// -------------- ROUTES --------------

// Root route => render index.ejs
app.get("/", (req, res) => {
  // We'll pass some data to the template. But we can also send real-time updates via Socket.io.
  res.render("index", {
    pageTitle: "DAX30 Trading Dashboard"
  });
});

// -------------- API ENDPOINTS --------------

// Return current open trade (if any)
app.get("/api/activeTrade", (req, res) => {
  // strategyLogic has an internal activeTrade object
  // We'll assume it's accessible. In your code, it might be exported or we store it differently.
  // Let's just read from 'strategyLogic.js' if we exposed it.
  // For now, we create a getter in strategyLogic to retrieve "activeTrade" safely.
  const activeTrade = strategy.getActiveTrade();
  res.json(activeTrade || {});
});

// Return closed trades for the day
// For a simpler approach, we can parse the log file lines that have "[TradeClose] ..."
app.get("/api/closedTrades", (req, res) => {
  const logLines = readTodaysLogLines();
  // filter for lines that contain "[TradeClose]"
  const closedTrades = logLines
    .filter(line => line.includes("[TradeClose]"))
    .map(line => parseLogLine(line));
  res.json(closedTrades);
});

// Return entire log as a list of objects (type, details, timestamp)
app.get("/api/logs", (req, res) => {
  const logLines = readTodaysLogLines();
  const parsed = logLines.map(line => parseLogLine(line)).filter(x => x !== null);
  res.json(parsed);
});

// Return current "balance" or "equity" – we can mock for now or read from strategy
app.get("/api/balance", (req, res) => {
  // In a real app, you'd call IG /accounts or keep track in your logic
  const mockEquity = 30000;
  res.json({ equity: mockEquity });
});

// Return current DAX price – if you have a function or variable that stores it
app.get("/api/daxPrice", (req, res) => {
  // If your code updates a global or exports a getter for the last known price:
  // For now, we might mock or read from strategy. We'll just return 0 if not found.
  res.json({ price: 0 });
});

// -------------- UTILS --------------

function getTodayLogPath() {
  // same approach as in logger.js
  const d = new Date();
  const yyyy = d.getFullYear();
  const mm = ("0" + (d.getMonth() + 1)).slice(-2);
  const dd = ("0" + d.getDate()).slice(-2);
  return path.join(__dirname, "logs", `${yyyy}-${mm}-${dd}.log`);
}

function readTodaysLogLines() {
  const logPath = getTodayLogPath();
  if (!fs.existsSync(logPath)) {
    return [];
  }
  try {
    const content = fs.readFileSync(logPath, "utf8");
    const lines = content.split("\n").filter(line => line.trim().length > 0);
    return lines;
  } catch (err) {
    console.error("Error reading log file:", err);
    return [];
  }
}

// parse a line like: 
// [2025-05-29T08:16:12.567Z] [TradeOpen] {"session":1,"direction":"BUY",...}
function parseLogLine(line) {
  // We can attempt a simple approach:
  // split by "] " => 3 parts => [2025-.., [TradeOpen, {...}]
  // or use a regex
  const match = line.match(/^\[(.*?)\]\s\[(.*?)\]\s(.*)$/);
  if (!match) return null;

  const timestamp = match[1];
  const eventType = match[2];
  let details = {};
  try {
    details = JSON.parse(match[3]);
  } catch (err) {
    // maybe not JSON
    details = { raw: match[3] };
  }

  return {
    timestamp,
    eventType,
    details
  };
}

// -------------- REAL-TIME (Socket.io) --------------

// optional: periodically broadcast updates to connected clients
setInterval(() => {
  const activeTrade = strategy.getActiveTrade();
  io.emit("activeTrade", activeTrade || {});
}, 5000); // every 5s

// you can also watch for file changes in the log to push them, or re-read every X seconds
// for simplicity, let's do every 5s
setInterval(() => {
  const logLines = readTodaysLogLines();
  const parsed = logLines.map(line => parseLogLine(line)).filter(x => x !== null);
  io.emit("logs", parsed);
}, 5000);

// -------------- START SERVER --------------
const PORT = process.env.PORT || 3000;
server.listen(PORT, () => {
  console.log(`Web UI listening on http://localhost:${PORT}`);
});
