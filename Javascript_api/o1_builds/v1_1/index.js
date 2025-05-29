"use strict";

const IGApiClient = require("./igApiClient");
const logic = require("./strategyLogic");
const config = require("./config");

/**
 * index.js
 * - Authenticates with IG
 * - Runs mainLoop every 1 minute
 * - mainLoop fetches DAX price, calls strategy logic in order
 */

async function getCurrentDaxPrice(apiClient) {
  const marketData = await apiClient.getMarketDetails(config.daxEpic);
  if (!marketData || !marketData.snapshot) {
    return 0;
  }
  return (marketData.snapshot.bid + marketData.snapshot.offer) / 2.0;
}

function getAccountEquity() {
  return 30000; // placeholder
}

async function mainLoop(apiClient) {
  const currentPrice = await getCurrentDaxPrice(apiClient);
  if (!currentPrice) {
    console.error("[mainLoop] Could not get DAX price");
    return;
  }

  // 1) volatility
  logic.checkVolatilityFilters(currentPrice);

  // 2) sweeps
  logic.checkSweeps(currentPrice);

  // 3) session times
  logic.checkSessionTimes(currentPrice);

  // 4) update high/low if active
  logic.updateHighLow(logic.session1, currentPrice);
  logic.updateHighLow(logic.session2, currentPrice);

  // 5) maybe open trades
  const eq = getAccountEquity();
  await logic.tryOpenTrade(apiClient, logic.session1, currentPrice, eq);
  await logic.tryOpenTrade(apiClient, logic.session2, currentPrice, eq);

  // No “checkTradeStop” call since the hedge was removed
}

(async function run() {
  const apiClient = new IGApiClient();
  const ok = await apiClient.authenticate();
  if (!ok) {
    console.error("Auth failed. Exiting...");
    process.exit(1);
  }

  console.log("[index.js] Auth successful. Starting strategy loop...");
  setInterval(() => mainLoop(apiClient), 60 * 1000);
})();
