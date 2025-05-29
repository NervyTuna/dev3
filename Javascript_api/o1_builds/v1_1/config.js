"use strict";

/**
 * config.js
 *
 * This file holds all the configuration values and credentials for the IG API
 * as well as the strategy parameters (entry levels, retraction logic, etc.).
 *
 * Key adjustments for your request:
 *  - Removed 'hedgeTrigger' (the 40 net-close).
 *  - Set the 'guaranteedStopDistance' to 40, so GSL=40 points.
 */

module.exports = {
  // IG API Credentials
  igUsername: "javascript_api",
  igPassword: "Newzealand6",
  igApiKey:   "d9e432394ff4ad5de9b061b35e927a2983bc1569",

  // "demo" => practice environment; "live" => real
  igApiEnv: "demo",

  /**
   * Market "EPIC" for GER30(£) in IG. (You can adjust if your broker uses a different EPIC.)
   * Example: "IX.D.DAX.IFD.IP" is often DAX (called GER30 or GER40).
   * If you have a specific "GBP" version of DAX, you may need a custom EPIC from IG.
   */
  daxEpic: "IX.D.DAX.IFD.IP",

  // ----- STRATEGY PARAMETERS -----
  zoneLevels: [45, 70, 100, 130],    // Distances from session open
  zoneCancelLevel: 179,             // 179 => "sweep close"
  guaranteedStopDistance: 40,       // was 42, now 40 for the only stop
  maxGSLPremium: 6,                 // GSL cost must not exceed 6 points (optional check)

  // Tolerance & Retraction
  retractionTolerance: 9,           // ±9 overshoot tolerance
  retractionSteps: [
    { min: 15, max: 29.9, shift: 18 },   // shift next entry by +18
    { min: 30, max: 35.9, skip: 1 },     // skip next level
    { min: 36, max: 45.9, skip: 2 },     // skip 2 levels
    { min: 46, max: 999, cancel: true }  // cancel session if no trade open
  ],

  // Volatility filters: skip sessions if big overnight/midday moves
  overnightVolLimit: 200,
  middayVolLimit: 150,

  // Bet sizing: e.g., lot size = (equity / betSizingFactor)
  betSizingFactor: 30000,
  minLotSize: 0.1,

  // Spread schedule: approximate DAX spread by time (UK). fallback if no match
  spreadSchedule: [
    { start: "00:00", end: "07:00", spread: 4.0 },
    { start: "07:00", end: "08:00", spread: 2.0 },
    { start: "08:00", end: "16:30", spread: 1.2 },
    { start: "16:30", end: "21:00", spread: 2.0 },
    { start: "21:00", end: "23:59", spread: 5.0 }
  ],
  fallbackSpread: 5.0
};
