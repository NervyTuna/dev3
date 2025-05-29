"use strict";

const axios = require("axios");
const config = require("./config");

/**
 * igApiClient.js
 *
 * This class handles:
 *  - Authentication (login to IG)
 *  - Retrieving market details (/markets/{epic})
 *  - Placing trades (opening or net-close)
 *
 * In your request, we removed the 40 net-close logic from the *strategy*, but
 * we still have a general placeTrade() method that can do forceOpen=true or false.
 * The difference is simply that *strategyLogic.js* no longer calls net-close at 40.
 */

class IGApiClient {
  constructor() {
    // from config
    this.username     = config.igUsername;
    this.password     = config.igPassword;
    this.apiKey       = config.igApiKey;
    this.environment  = config.igApiEnv;  // "demo" or "live"

    // set base URL for the environment
    this.baseURL = (this.environment === "live")
      ? "https://api.ig.com/gateway/deal"
      : "https://demo-api.ig.com/gateway/deal";

    // tokens stored after authenticate()
    this.accessToken  = null;
    this.refreshToken = null;
    this.accountId    = null;
  }

  /**
   * authenticate()
   *
   * Logs into IG using username/password.
   * On success, IG returns special headers (x-security-token, cst) that we must use
   * for subsequent requests to prove we are logged in.
   */
  async authenticate() {
    try {
      const response = await axios.post(
        `${this.baseURL}/session`,
        {
          identifier: this.username,
          password: this.password
        },
        {
          headers: {
            "X-IG-API-KEY": this.apiKey,
            "Content-Type": "application/json",
            "Accept": "application/json"
          }
        }
      );

      // retrieve tokens from headers
      this.accessToken  = response.headers["x-security-token"];
      this.refreshToken = response.headers["cst"];
      this.accountId    = response.data.currentAccountId;

      console.log("[IGApiClient] Auth succeeded. AccountId =", this.accountId);
      return true;
    } catch (err) {
      console.error("[IGApiClient] Auth failed:", err.message);
      return false;
    }
  }

  /**
   * _buildHeaders(version=2)
   *
   * Helper to build the required headers for further requests.
   */
  _buildHeaders(version=2) {
    return {
      "X-IG-API-KEY": this.apiKey,
      "X-SECURITY-TOKEN": this.accessToken,
      "CST": this.refreshToken,
      "Content-Type": "application/json",
      "Accept": `application/json; charset=UTF-8; version=${version}`
    };
  }

  /**
   * getMarketDetails(epic):
   *   queries IG for info about the market, including snapshot prices (bid/offer).
   */
  async getMarketDetails(epic) {
    const url = `${this.baseURL}/markets/${epic}`;
    try {
      const resp = await axios.get(url, { headers: this._buildHeaders() });
      return resp.data;
    } catch (err) {
      console.error("[getMarketDetails] Error:", err.message);
      return null;
    }
  }

  /**
   * placeTrade(epic, direction, size, guaranteedStopDist, forceOpen)
   *
   * - direction: "BUY" or "SELL"
   * - size: lot size (e.g. 0.2)
   * - guaranteedStopDist: e.g. 40
   * - forceOpen=true => open a new position
   * - forceOpen=false => net off or close an opposite position
   */
  async placeTrade(epic, direction, size, guaranteedStopDist, forceOpen) {
    const url = `${this.baseURL}/positions/otc`;
    const data = {
      epic,
      direction,
      size,
      orderType: "MARKET",
      timeInForce: "FILL_OR_KILL",
      guaranteedStop: true,     // We want guaranteed stops
      stopDistance: guaranteedStopDist,  // e.g. 40
      forceOpen,                // false => net off, true => open new
      currencyCode: "GBP"
    };

    try {
      const resp = await axios.post(url, data, { headers: this._buildHeaders(2) });
      console.log("[placeTrade] Order placed successfully:", resp.data.dealReference);
      return resp.data;
    } catch (err) {
      console.error("[placeTrade] Error placing trade:", err.response?.data || err.message);
      return null;
    }
  }
}

module.exports = IGApiClient;
