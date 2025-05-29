//+------------------------------------------------------------------+
//|                                            pro_open_2.mq4        |
//|        (Single-typo fix: changed OP_BUUY to OP_BUY on line 909)  |
//+------------------------------------------------------------------+

#property copyright "Your Name"
#property link      "https://www.example.com"
#property version   "1.11" // +9 tolerance fix for final 130-level
#property strict

// ---------------------------------------------------------------------------
//                    DAX30 Strategy EA with Full Retraction & Tolerance
//                 + Sweeps, Volatility Filters, Dynamic Spread, Diagnostics
// ---------------------------------------------------------------------------
//
// This EA is structured into 10 modules (~785 lines) to keep logic
// organized. It includes a check that if the distance from the
// session open exceeds 130 + 9 points, no more trades can be placed
// in that session.
//
// ---------------------------------------------------------------------------
//                             MODULE 1: INPUTS & GLOBALS
// ---------------------------------------------------------------------------

string SymbolToTrade; // Set dynamically in OnInit()

// Declare serverToUK as a global integer so references compile correctly:
int serverToUK = 0;

// Standard session/time inputs
input ENUM_TIMEFRAMES ChartTimeframe = PERIOD_M1;  // Working timeframe
input string Session1StartTime    = "08:00";       // Session 1 start (UK time)
input string Session1EndTime      = "12:30";       // Session 1 end   (UK time)
input string Session2StartTime    = "14:30";       // Session 2 start (UK time)
input string Session2EndTime      = "17:16";       // Session 2 end   (UK time)

// Levels for distance from open price
input int ZoneEntryLevel1 = 45;   // e.g. 45 points
input int ZoneEntryLevel2 = 70;
input int ZoneEntryLevel3 = 100;
input int ZoneEntryLevel4 = 130;

// Sweeps
input int ZoneCancelLevel = 179;  // e.g. 179 points from open => forced close

// Volatility limits
input double OvernightVolatilityLimit = 200.0; // 200+ points overnight => skip session 1
input double MiddayVolatilityLimit    = 150.0; // 150+ points midday    => skip session 2

// Tolerance & Retraction
input double RetractTolerance       = 9.0;   // ±9 point "overshoot" tolerance
input double RetractMin_1           = 15.0;  // 15–29.9 => +18 from current HL
input double RetractMax_1           = 29.9;
input double RetractAdjustBeyondHL  = 18.0;  // add 18 if retraction=1
input double RetractMin_2           = 30.0;  // 30–35.9 => skip next level
input double RetractMax_2           = 35.9;
input double RetractMin_3           = 36.0;  // 36–45.9 => skip 2 levels
input double RetractMax_3           = 45.9;
input double RetractCancelThreshold = 46.0;  // >=46 => session canceled if no open trades

// Adverse move & timed closes
input int AdverseMove1       = 15; // e.g. 15 pts adverse => break-even check
input int CloseTimeMinLevel1 = 16; // 16th minute => close for 45–69 trades
input int CloseTimeMinLevel2 = 31; // 31st minute => close for 70+ trades

// Bet sizing
input double BetSizingFactor = 30000.0;  // Adjusted for realistic lot sizes
input double MinTradeSize    = 0.1;      // Min lot
input int    SlippagePoints  = 3;        // Slippage in points
input string TradeComment    = "DAX30Strategy";
input int    StopLossPoints  = 40;       // e.g. 40-point mandatory SL

// ============ DIAGNOSTIC & SPREAD-OVERRIDE INPUTS ============

input bool   UseDebugDiagnostics = true;   // Print extra logs each tick
input bool   ForceSpread         = false;  // If true, override schedule-based spread
input double OverriddenSpread    = 2.0;    // e.g. 2.0 points if forced

// Spread schedule for GER30 (DAX), in UK local times.
// Use HH:MM, inclusive range.
// E.g. "08:00" to "16:30" => 08:00 <= now <= 16:30 => 1.2 spread
struct SpreadTime
{
   string startTime;      // "HH:MM"
   string endTime;        // "HH:MM"
   double spreadPoints;   // e.g. 1.2, 2.0, 5.0
};

// Example default schedule covering 00:00–23:59 in blocks:
SpreadTime spreadSchedule[] =
{
   {"00:00", "07:00", 4.0},
   {"07:00", "08:00", 2.0},
   {"08:00", "16:30", 1.2},
   {"16:30", "21:00", 2.0},
   {"21:00", "00:01", 5.0}  // Updated to ensure wrap-around coverage
};

double DefaultSpreadPoints = 5.0; // in case of no match

// Tracks each open trade in this EA
struct TradeData
{
   int ticket;
   double entryPrice;
   double adjustedEntryPrice;
   double peakHigh;
   double peakLow;
   datetime peakTime;
   int sessionNum;
   bool isActive;
};

TradeData trades[10];
int activeTradeCount = 0;

// Session-limited trades
int Session1TradeCount = 0;
int Session2TradeCount = 0;
bool session1Allowed   = true;
bool session2Allowed   = true;

// Global var prefix for storing adjusted bid/ask/spread
string globalVarPrefix;

// Magic number for trades
input int MagicNumber = 30001;  // Set externally per chart

// ---------------------------------------------------------------------------
//                           MODULE 2: TIME MANAGEMENT
// ---------------------------------------------------------------------------

bool IsUKDST(datetime time)
{
   MqlDateTime dt;
   TimeToStruct(time, dt);
   int year = dt.year;

   datetime marchLastSunday = 0;
   for(int dMarch=31; dMarch>=25; dMarch--)
   {
      datetime tempMarch = StringToTime(IntegerToString(year)+".03."+IntegerToString(dMarch)+" 01:00");
      if(TimeDayOfWeek(tempMarch)==0)
      {
         marchLastSunday = tempMarch;
         break;
      }
   }

   datetime octoberLastSunday = 0;
   for(int dOctober=31; dOctober>=25; dOctober--)
   {
      datetime tempOctober = StringToTime(IntegerToString(year)+".10."+IntegerToString(dOctober)+" 01:00");
      if(TimeDayOfWeek(tempOctober)==0)
      {
         octoberLastSunday = tempOctober;
         break;
      }
   }

   return (time >= marchLastSunday && time < octoberLastSunday);
}

// Not needed but retained
int GetServerToUKOffset()
{
    return 0;
}

datetime GetUKTime()
{
    // If your broker time is the same as UK time, no offset needed
    return TimeCurrent();
}

// Convert "HH:MM" => total minutes
int StrToTimeMins(string timeStr)
{
   int h = ((int)StringGetChar(timeStr, 0) - '0') * 10
           + ((int)StringGetChar(timeStr, 1) - '0');
   int m = ((int)StringGetChar(timeStr, 3) - '0') * 10
           + ((int)StringGetChar(timeStr, 4) - '0');
   return (h * 60) + m;
}

// Compare hour/min to schedule
double GetSpreadForTime()
{
   if(ForceSpread)
   {
      PrintFormat("[SpreadCalculation] ForceSpread=TRUE => OverriddenSpread=%.1f", OverriddenSpread);
      return OverriddenSpread;
   }

   datetime ukNow  = GetUKTime();
   int nowHour     = TimeHour(ukNow);
   int nowMinute   = TimeMinute(ukNow);
   int nowMins     = nowHour*60 + nowMinute;
   string hhmmStr  = TimeToString(ukNow, TIME_MINUTES);

   for(int i=0; i<ArraySize(spreadSchedule); i++)
   {
      int startMins = StrToTimeMins(spreadSchedule[i].startTime);
      int endMins   = StrToTimeMins(spreadSchedule[i].endTime);

      if(startMins > endMins)
      {
         // wrap-around case
         if(nowMins >= startMins || nowMins <= endMins)
         {
            PrintFormat("[SpreadSchedule] Time: %s => SpreadPoints=%.1f (wrap range)",
                        hhmmStr, spreadSchedule[i].spreadPoints);
            return spreadSchedule[i].spreadPoints;
         }
      }
      else
      {
         if(nowMins >= startMins && nowMins <= endMins)
         {
            PrintFormat("[SpreadSchedule] Time: %s => SpreadPoints=%.1f",
                        hhmmStr, spreadSchedule[i].spreadPoints);
            return spreadSchedule[i].spreadPoints;
         }
      }
   }

   PrintFormat("[SpreadSchedule] No schedule match => default %.1f", DefaultSpreadPoints);
   return DefaultSpreadPoints;
}

string SanitizeSymbol(string symbol)
{
   string s = symbol;
   StringReplace(s, "(", "_");
   StringReplace(s, ")", "_");
   StringReplace(s, "£", "GBP");
   return s;
}

// Adjust BID/ASK by dynamic or forced spread => global vars
bool AdjustPricesBySpread()
{
    double bid = SymbolInfoDouble(Symbol(), SYMBOL_BID);
    double ask = SymbolInfoDouble(Symbol(), SYMBOL_ASK);

    // Retry mechanism for missing prices
    if (bid <= 0 || ask <= 0)
    {
        Print("[AdjustPricesBySpread] Missing bid/ask prices. Retrying...");
        Sleep(500);
        RefreshRates();
        bid = SymbolInfoDouble(Symbol(), SYMBOL_BID);
        ask = SymbolInfoDouble(Symbol(), SYMBOL_ASK);
        if (bid <= 0 || ask <= 0)
        {
            Print("[AdjustPricesBySpread] Failed to retrieve valid prices.");
            return false;
        }
    }

    double spreadPoints = GetSpreadForTime();
    double adjustedBid = bid - (spreadPoints * Point);
    double adjustedAsk = ask + (spreadPoints * Point);

    adjustedBid = NormalizeDouble(adjustedBid, _Digits);
    adjustedAsk = NormalizeDouble(adjustedAsk, _Digits);

    GlobalVariableSet(globalVarPrefix + "_AdjBid", adjustedBid);
    GlobalVariableSet(globalVarPrefix + "_AdjAsk", adjustedAsk);
    GlobalVariableSet(globalVarPrefix + "_Spread", spreadPoints);

    return true;
}

// ---------------------------------------------------------------------------
//                           MODULE 3: SESSION HANDLING
// ---------------------------------------------------------------------------

struct SessionData
{
   datetime startTime;
   datetime endTime;
   double   openPrice;
   double   highPrice;
   double   lowPrice;
   bool     isInitialized;
   datetime peakTime;
   datetime startTimeUK;
   datetime endTimeUK;
};

SessionData session1;
SessionData session2;
datetime lastSessionInitDate = 0;

void InitializeSessionData()
{
    datetime todayUK = GetUKTime();
    MqlDateTime dUK;
    TimeToStruct(todayUK, dUK);
    string ymdUK = StringFormat("%04d.%02d.%02d", dUK.year, dUK.mon, dUK.day);

    session1.startTimeUK = StringToTime(ymdUK + " " + Session1StartTime);
    session1.endTimeUK   = StringToTime(ymdUK + " " + Session1EndTime);

    session2.startTimeUK = StringToTime(ymdUK + " " + Session2StartTime);
    session2.endTimeUK   = StringToTime(ymdUK + " " + Session2EndTime);

    session1.startTime = session1.startTimeUK;
    session1.endTime   = session1.endTimeUK;

    session2.startTime = session2.startTimeUK;
    session2.endTime   = session2.endTimeUK;

    Print("[InitSessionData] Session1 UK: ",
          TimeToString(session1.startTimeUK, TIME_MINUTES),
          " - ", TimeToString(session1.endTimeUK, TIME_MINUTES));
    Print("[InitSessionData] Session2 UK: ",
          TimeToString(session2.startTimeUK, TIME_MINUTES),
          " - ", TimeToString(session2.endTimeUK, TIME_MINUTES));
}

void ResetSessionData(SessionData &s, double bidPrice)
{
   if(bidPrice <= 0.0)
   {
      Print("[SessionResetError] Invalid bid: ", DoubleToString(bidPrice,2));
      return;
   }
   s.openPrice     = bidPrice;
   s.highPrice     = bidPrice;
   s.lowPrice      = bidPrice;
   s.isInitialized = true;
   s.peakTime      = TimeCurrent();

   Print("[SessionReset] OpenPrice: ", DoubleToString(s.openPrice,2));
}

void CheckSessionInitialization()
{
   datetime now = TimeCurrent();
   MqlDateTime stNow;
   TimeToStruct(now, stNow);
   datetime currentDate = StringToTime(StringFormat("%04d.%02d.%02d", stNow.year, stNow.mon, stNow.day));

   // Re-init if new day or sessions uninitialized
   if(lastSessionInitDate != currentDate ||
      (!session1.isInitialized && !session2.isInitialized))
   {
      Print("[DailySessionCheck] New trading day or uninitialized sessions => Initialize sessions");
      InitializeSessionData();
      lastSessionInitDate = currentDate;
      Session1TradeCount  = 0;
      Session2TradeCount  = 0;
      session1Allowed     = true;
      session2Allowed     = true;
   }

   double bid = SymbolInfoDouble(SymbolToTrade, SYMBOL_BID);
   if(bid <= 0)
   {
       bid = GlobalVariableGet(globalVarPrefix + "_AdjBid");
       if(bid <= 0)
       {
           Print("[SessionInitError] No valid bid or adjustedBid.");
           return;
       }
   }

   // Session 1 active?
   if(now >= session1.startTime && now < session1.endTime)
   {
       if(!session1.isInitialized)
       {
           Print("[Session1] Entering session window => reset data.");
           ResetSessionData(session1, bid);
           Session1TradeCount = 0;
       }
   }
   else if(session1.isInitialized)
   {
       session1.isInitialized = false;
       Print("[Session1] Exiting session window => isInitialized=false");
   }

   // Session 2 active?
   if(now >= session2.startTime && now < session2.endTime)
   {
       if(!session2.isInitialized)
       {
           Print("[Session2] Entering session window => reset data.");
           ResetSessionData(session2, bid);
           Session2TradeCount = 0;
       }
   }
   else if(session2.isInitialized)
   {
       session2.isInitialized = false;
       Print("[Session2] Exiting session window => isInitialized=false");
   }
}

void UpdateSessionHighLow(SessionData &s, double price)
{
   if(!s.isInitialized || price <= 0) return;
   if(price > s.highPrice)
   {
      s.highPrice = price;
      s.peakTime  = TimeCurrent();
   }
   if(price < s.lowPrice)
   {
      s.lowPrice = price;
      s.peakTime = TimeCurrent();
   }
}

// ---------------------------------------------------------------------------
//                          MODULE 4: ACTIVE TRADING ZONES
// ---------------------------------------------------------------------------

struct ActiveZone
{
   string startTime;
   string endTime;
   int requiredDistance;
};

ActiveZone session1Zones[] =
{
   // As-coded zones (some differ slightly from the new brief times):
   {"08:16","09:05", 45},
   {"09:30","09:45", 45}, // *** Differs from brief's "09:30–10:06" ***
   {"10:15","10:45", 70},
   {"10:45","11:45", 45},
   {"11:45","12:31", 45}
};

ActiveZone session2Zones[] =
{
   {"14:46","15:06", 45},
   {"15:15","15:45", 70},
   {"15:45","16:48", 45}
};

bool IsWithinTimeWindow(string startT, string endT)
{
    datetime nowUK = TimeCurrent();
    MqlDateTime stNowUK;
    TimeToStruct(nowUK, stNowUK);
    string ukDate = StringFormat("%04d.%02d.%02d", stNowUK.year, stNowUK.mon, stNowUK.day);

    datetime startUK = StringToTime(ukDate + " " + startT);
    datetime endUK   = StringToTime(ukDate + " " + endT);
    if (startUK > endUK) endUK += 86400;

    // Debug trace
    PrintFormat("[ClockTrace] nowUK=%s  startUK=%s  endUK=%s",
                TimeToString(nowUK, TIME_SECONDS),
                TimeToString(startUK, TIME_SECONDS),
                TimeToString(endUK, TIME_SECONDS));

    return (nowUK >= startUK && nowUK <= endUK);
}

bool IsInActiveTradingZone(SessionData &session, int sessionNum, double adjustedBid)
{
    if(!session.isInitialized || adjustedBid <= 0)
    {
        Print("[ActiveZoneCheck] Session not init or invalid adjustedBid => skip zone check");
        return false;
    }

    double distancePoints = MathAbs(adjustedBid - session.openPrice) / IdxPts(1.0);

    ActiveZone zones[];
    if(sessionNum == 1)
    {
        ArrayResize(zones, ArraySize(session1Zones));
        for(int i=0; i<ArraySize(zones); i++)
            zones[i] = session1Zones[i];
    }
    else
    {
        ArrayResize(zones, ArraySize(session2Zones));
        for(int j=0; j<ArraySize(zones); j++)
            zones[j] = session2Zones[j];
    }

    for(int k=0; k<ArraySize(zones); k++)
    {
        bool inWindow = IsWithinTimeWindow(zones[k].startTime, zones[k].endTime);
        if(inWindow)
        {
            if(distancePoints >= zones[k].requiredDistance)
            {
                Print("[ZoneMatch] distancePoints=", DoubleToString(distancePoints,1),
                      ", Required=", zones[k].requiredDistance);
                return true;
            }
            else
            {
                Print("[ActiveZoneCheck] Session#", sessionNum,
                      " in time window but distance=", DoubleToString(distancePoints,1),
                      " < required ", zones[k].requiredDistance);
            }
        }
    }

    return false;
}

// ---------------------------------------------------------------------------
//                         MODULE 5: RETRACTION & TOLERANCE
// ---------------------------------------------------------------------------

double CalculateRetraction(SessionData &s, double price)
{
    if(price >= s.openPrice)
        return (s.highPrice - price) / IdxPts(1.0);
    else
        return (price - s.lowPrice) / IdxPts(1.0);
}

int GetRetractionLevel(double retraction)
{
   if(retraction >= RetractMin_1 && retraction <= RetractMax_1) return 1;
   if(retraction >= RetractMin_2 && retraction <= RetractMax_2) return 2;
   if(retraction >= RetractMin_3 && retraction <= RetractMax_3) return 3;
   if(retraction >= RetractCancelThreshold)                    return -1;
   return 0;
}

int GetFinalEntryLevel(double distancePoints, int baseLevel, int retractionLevel)
{
   if(retractionLevel == -1) return -1;

   int adjustedLevel = baseLevel;
   if(retractionLevel == 2) adjustedLevel += 1; // skip next
   if(retractionLevel == 3) adjustedLevel += 2; // skip 2 levels
   if(adjustedLevel > 4) adjustedLevel = 4;

   int bestLevel = -1;
   for(int lvl = adjustedLevel; lvl <= 4; lvl++)
   {
      int neededDist = (lvl == 1) ? ZoneEntryLevel1 :
                       (lvl == 2) ? ZoneEntryLevel2 :
                       (lvl == 3) ? ZoneEntryLevel3 : ZoneEntryLevel4;

      if(distancePoints >= neededDist)
         bestLevel = lvl;

      PrintFormat("[FinalLevelDebug] distance=%.1f, neededDist=%d, bestLevel=%d",
                  distancePoints, neededDist, bestLevel);
   }
   return bestLevel;
}

double CalculateNewEntryPrice(SessionData &session, int finalLevel, bool isBuy, int retractionLevel)
{
   int distancePoints = 0;
   if(finalLevel==1) distancePoints = ZoneEntryLevel1;
   if(finalLevel==2) distancePoints = ZoneEntryLevel2;
   if(finalLevel==3) distancePoints = ZoneEntryLevel3;
   if(finalLevel==4) distancePoints = ZoneEntryLevel4;

   double addPoints = 0;
   if(retractionLevel == 1) addPoints = RetractAdjustBeyondHL;

   double spreadPoints = GlobalVariableGet(globalVarPrefix+"_Spread");
   if(spreadPoints < 0) spreadPoints = 0;

   if(isBuy)
      return session.openPrice - IdxPts(distancePoints + addPoints + spreadPoints);
   else
      return session.openPrice + IdxPts(distancePoints + addPoints + spreadPoints);
}

// ---------------------------------------------------------------------------
//                       MODULE 6: TRADE ENTRY & EXIT LOGIC
// ---------------------------------------------------------------------------

void PlaceTrade(string type, double entryPrice, double lotSize, int sessionNum)
{
    double price = (type == "BUY") ? SymbolInfoDouble(SymbolToTrade, SYMBOL_ASK)
                                   : SymbolInfoDouble(SymbolToTrade, SYMBOL_BID);

    if(price <= 0)
    {
        Print("[PlaceTradeError] Invalid market price. Type=", type,
              ", Price=", DoubleToString(price, 2));
        return;
    }

    double stopLevelPts = MarketInfo(SymbolToTrade, MODE_STOPLEVEL);
    double stopDistance = MathMax(IdxPts(StopLossPoints), (stopLevelPts + 1) * Point);

    double stopLoss = (type == "BUY")
                      ? NormalizeDouble(price - stopDistance, Digits)
                      : NormalizeDouble(price + stopDistance, Digits);

    Print("[PlaceTrade] Type=", type,
          ", session=", sessionNum,
          ", Price=", DoubleToString(price, 2),
          ", SL=", DoubleToString(stopLoss, 2),
          ", Lot=", DoubleToString(lotSize, 2));

    int ticket = OrderSend(SymbolToTrade, (type == "BUY") ? OP_BUY : OP_SELL,
                           lotSize, price, SlippagePoints, stopLoss,
                           0, TradeComment, MagicNumber, 0, clrBlue);

    if(ticket < 0)
    {
        Print("[OrderSendError] Failed to place trade. Error=", GetLastError());
    }
    else
    {
        Print("[OrderSendSuccess] Ticket=", ticket);

        // Register the trade in trades[]
        for(int i = 0; i < ArraySize(trades); i++)
        {
            if(!trades[i].isActive)  // free slot
            {
                trades[i].ticket        = ticket;
                trades[i].entryPrice    = price;
                trades[i].adjustedEntryPrice = entryPrice;
                trades[i].peakHigh      = price;
                trades[i].peakLow       = price;
                trades[i].peakTime      = TimeCurrent();
                trades[i].sessionNum    = sessionNum;
                trades[i].isActive      = true;
                activeTradeCount++;
                // Optionally increment session trade count:
                if(sessionNum == 1) Session1TradeCount++;
                else               Session2TradeCount++;
                break;
            }
        }
    }
}

bool IsPriceValid(double price)
{
   return (price > 0.0);
}

// *** CHANGED section to remove "possible loss of data" warnings ***
double CalculateLotSize()
{
   double equity = AccountEquity();
   double lotRaw = equity / BetSizingFactor; // e.g. 10,000 / 30000 = 0.33
   double step   = SymbolInfoDouble(_Symbol, SYMBOL_VOLUME_STEP);
   if(step <= 0) step = 0.01;

   double lot = MathFloor(lotRaw / step) * step; // snap to step
   lot        = MathMax(lot, MinTradeSize);      // honour min lot
   lot        = MathMin(lot, SymbolInfoDouble(_Symbol, SYMBOL_VOLUME_MAX));

   return NormalizeDouble(lot, 2);
}

bool CanTrade()
{
   long tradeMode;
   if(!SymbolInfoInteger(SymbolToTrade, SYMBOL_TRADE_MODE, tradeMode)
      || tradeMode != SYMBOL_TRADE_MODE_FULL)
   {
      Print("[CanTrade] Symbol ", SymbolToTrade,
            " is not tradable. TradeMode=", tradeMode);
      return false;
   }

   double lot = NormalizeDouble(AccountEquity() / BetSizingFactor, 2);
   lot = MathMax(MinTradeSize, NormalizeDouble(lot,2));
   if(lot <= 0)
   {
      Print("[CanTrade] Lot size=0 => skip");
      return false;
   }
   if(!AccountFreeMarginCheck(SymbolToTrade, OP_BUY, lot))
   {
      Print("[CanTrade] Not enough free margin");
      return false;
   }
   return true;
}

void CheckAndPlaceTrade(SessionData &session, double currentPrice, int sessionNum)
{
    if(!session.isInitialized) return;  // Don’t evaluate until session reset
    double adjustedBid = GlobalVariableGet(globalVarPrefix+"_AdjBid");
    if(adjustedBid <= 0) return;        // Wait until spread block ran

    // If the session is disallowed, skip
    if((sessionNum == 1 && !session1Allowed) ||
       (sessionNum == 2 && !session2Allowed))
    {
        Print("[SessionBlocked] session", sessionNum,
              " is disallowed by volatility or prior logic");
        return;
    }

    // --------------------------------------------------------
    // If distance > (130 + 9) => no more trades
    // --------------------------------------------------------
    double distancePoints = MathAbs(adjustedBid - session.openPrice) / IdxPts(1.0);
    if(distancePoints > (ZoneEntryLevel4 + RetractTolerance))
    {
       PrintFormat("[NoMoreTrades] Exceeded final entry level (%.1f) + tolerance (%.1f) => no trades in session %d",
                   (double)ZoneEntryLevel4, (double)RetractTolerance, sessionNum);
       if(sessionNum == 1) session1Allowed = false;
       else                session2Allowed = false;
       return;
    }

    Print("[TradeCondition] DistancePoints=", DoubleToString(distancePoints, 1),
          ", MinRequired=", ZoneEntryLevel1);

    if(!IsInActiveTradingZone(session, sessionNum, adjustedBid))
    {
        Print("[TradeZone] Not in active zone or distance insufficient");
        return;
    }

    double retraction = CalculateRetraction(session, adjustedBid);
    int retLevel      = GetRetractionLevel(retraction);
    Print("[TradeCondition] Retraction=", DoubleToString(retraction, 1),
          ", RetractionLevel=", retLevel,
          ", SessionAllowed=", (string)(sessionNum == 1 ? session1Allowed : session2Allowed));

    // If retraction >=46 and no trades => skip session, but we don't block if there's an open trade
    if(retLevel == -1 && CountOpenTrades() == 0)
    {
        Print("[SessionWarning] Retraction >=46 => skipping trade, but session remains active for possible open trades? (brief says 'cancel if no trades open')");
        return;
    }

    // final level check
    int finalLevel = GetFinalEntryLevel(distancePoints, 1, retLevel);
    Print("[FinalEntryCheck] FinalLevel=", finalLevel);
    if(finalLevel < 1)
    {
        Print("[TradeSkip] No valid final level");
        return;
    }

    bool isBuy = (adjustedBid < session.openPrice);
    string tType = isBuy ? "BUY" : "SELL";
    double ePrice = CalculateNewEntryPrice(session, finalLevel, isBuy, retLevel);

    if(!IsPriceValid(ePrice))
    {
        Print("[TradeSkip] Invalid entryPrice");
        return;
    }

    double lots = CalculateLotSize();
    if(lots <= 0)
    {
        Print("[TradeSkip] Lot size came back <=0 – no order sent");
        return;
    }

    // Place the trade
    PlaceTrade(tType, ePrice, lots, sessionNum);
}

// ---------------------------------------------------------------------------
// Checks trades for forced closings, minute-based closings, etc.
// ---------------------------------------------------------------------------
void CheckTradeClosure()
{
   datetime now = TimeCurrent();
   datetime forcedClose931 = StringToTime(TimeToString(now, TIME_DATE)+" 09:31");
   bool doForcedClose931 = (now >= forcedClose931);

   for(int i=0; i<ArraySize(trades); i++)
   {
      if(!trades[i].isActive) continue;
      if(!OrderSelect(trades[i].ticket, SELECT_BY_TICKET) ||
         OrderSymbol() != SymbolToTrade)
      {
         trades[i].isActive=false;
         activeTradeCount--;
         continue;
      }

      double openP  = OrderOpenPrice();
      double cPrice = (OrderType()==OP_BUY)
                      ? SymbolInfoDouble(SymbolToTrade, SYMBOL_BID)
                      : SymbolInfoDouble(SymbolToTrade, SYMBOL_ASK);

      if(cPrice <= 0) continue;

      double sessionOpen = (trades[i].sessionNum==1)
                           ? session1.openPrice
                           : session2.openPrice;
      double dist   = MathAbs(openP - sessionOpen) / Point;
      double adverse= (OrderType()==OP_BUY)? (openP - cPrice) : (cPrice - openP);

      int minsSincePeak = (int)((now - trades[i].peakTime)/60);

      // forced close at 09:31 for ANY session1 trades in code
      // (note: user’s brief wanted only for trades opened in 08:16–09:05,
      // but we haven't changed that logic here)
      if(trades[i].sessionNum==1 && doForcedClose931)
      {
         Print("[ForcedClose] 09:31 => closing ticket=", trades[i].ticket);
         if(CloseTradeSafely(trades[i].ticket))
         {
            trades[i].isActive=false;
            activeTradeCount--;
         }
         continue;
      }

      // For 45–69 => 15 adverse => break-even if returns to entry, or 16th minute
      if(dist >= 45 && dist < 70)
      {
         if(adverse >= AdverseMove1*Point && MathAbs(cPrice - openP)<= Point)
         {
            Print("[TradeClose] 15-pt adverse => break-even close, ticket=", trades[i].ticket);
            if(CloseTradeSafely(trades[i].ticket))
            {
               trades[i].isActive=false;
               activeTradeCount--;
            }
         }
         else
         {
            if(minsSincePeak >= CloseTimeMinLevel1)
            {
               Print("[TradeClose] 16th minute from peak => ticket=", trades[i].ticket);
               if(CloseTradeSafely(trades[i].ticket))
               {
                  trades[i].isActive=false;
                  activeTradeCount--;
               }
            }
         }
      }
      else if(dist >= 70 && dist < 130)
      {
         if(minsSincePeak >= CloseTimeMinLevel2)
         {
            Print("[TradeClose] 31st minute from peak (70–129) => ticket=", trades[i].ticket);
            if(CloseTradeSafely(trades[i].ticket))
            {
               trades[i].isActive=false;
               activeTradeCount--;
            }
         }
      }
      else if(dist >= 130 && dist <= 159.9)
      {
         if(minsSincePeak >= CloseTimeMinLevel2)
         {
            Print("[TradeClose] 31st minute from peak (130+) => ticket=", trades[i].ticket);
            if(CloseTradeSafely(trades[i].ticket))
            {
               trades[i].isActive=false;
               activeTradeCount--;
            }
         }
      }
   }
}

bool CloseTradeSafely(int ticket)
{
   if(!OrderSelect(ticket, SELECT_BY_TICKET, MODE_TRADES)) return false;
   for(int attempt=0; attempt<5; attempt++)
   {
      double price = (OrderType()==OP_BUY)
                     ? SymbolInfoDouble(SymbolToTrade, SYMBOL_BID)
                     : SymbolInfoDouble(SymbolToTrade, SYMBOL_ASK);
      if(price <= 0) continue;

      bool res = OrderClose(ticket, OrderLots(), price, SlippagePoints, clrRed);
      if(res)
      {
         Print("[CloseTradeSafely] Success on attempt#", (attempt+1),
               " ticket=", ticket);
         LogCloseDetails(ticket);
         return true;
      }
      Print("[CloseTradeSafely] Fail attempt#", (attempt+1),
            ", error=", GetLastError());
      Sleep(500);
      RefreshRates();
   }
   Print("[CloseTradeSafely] Gave up after 5 attempts => ticket=", ticket);
   return false;
}

// ---------------------------------------------------------------------------
//                          MODULE 7: SWEEPS & VOLATILITY FILTERS
// ---------------------------------------------------------------------------

datetime lastSweepCheck=0;
double overnightStartPrice=0;
double middayStartPrice=0;

void CloseAllTrades()
{
   Print("[CloseAllTrades] Sweeping close for all active trades.");
   for(int i=0; i<ArraySize(trades); i++)
   {
      if(!trades[i].isActive) continue;
      if(OrderSelect(trades[i].ticket, SELECT_BY_TICKET) &&
         OrderSymbol()==SymbolToTrade)
      {
         // FIXED here: replaced OP_BUUY -> OP_BUY
         double cp = (OrderType()==OP_BUY)
                     ? SymbolInfoDouble(SymbolToTrade, SYMBOL_BID)
                     : SymbolInfoDouble(SymbolToTrade, SYMBOL_ASK);
         if(cp <= 0) continue;

         bool result = OrderClose(trades[i].ticket, OrderLots(), cp, SlippagePoints, clrRed);
         if(result)
         {
            Print("[CloseAllTrades] Closed ticket=", trades[i].ticket);
            LogCloseDetails(trades[i].ticket);
            trades[i].isActive=false;
            activeTradeCount--;
         }
         else
         {
            Print("[CloseAllTrades] Close fail: ", GetLastError());
         }
      }
   }
}

void HandleSweepsAndVolatility()
{
   if(TimeCurrent() - lastSweepCheck < 60) return; // check once/min
   lastSweepCheck= TimeCurrent();

   double bidPrice = SymbolInfoDouble(SymbolToTrade, SYMBOL_BID);
   if(bidPrice <= 0.0)
   {
      Print("[SweepVolError] Invalid bid => skip");
      return;
   }

   double dist1 = (session1.isInitialized)
                  ? MathAbs(bidPrice - session1.openPrice)/IdxPts(1.0) : 0;
   double dist2 = (session2.isInitialized)
                  ? MathAbs(bidPrice - session2.openPrice)/IdxPts(1.0) : 0;

   // Sweep close if distance >= 179
   if(dist1 >= ZoneCancelLevel || dist2 >= ZoneCancelLevel)
   {
      Print("[SweepClose] distance>=179 => close all trades");
      CloseAllTrades();
   }

   datetime ukNow= GetUKTime();
   int ukH= TimeHour(ukNow);
   int ukM= TimeMinute(ukNow);

   // Track price at 17:16 for overnight
   if(ukH==17 && ukM>=16 && overnightStartPrice==0)
   {
      overnightStartPrice= bidPrice;
      Print("[OvernightTrack] StartPrice=", DoubleToString(overnightStartPrice,2));
   }

   // Track midday at 12:31 in code (slightly differs from brief's 12:00)
   if(ukH==12 && ukM==31 && middayStartPrice==0)
   {
      middayStartPrice= bidPrice;
      Print("[MiddayTrack] StartPrice=", DoubleToString(middayStartPrice,2));
   }

   // Next day at 08:00 => check overnight volatility
   if(ukH==8 && ukM==0)
   {
      if(overnightStartPrice != 0 &&
         MathAbs(bidPrice - overnightStartPrice) >= OvernightVolatilityLimit)
      {
         session1Allowed=false;
         Print("[VolatilityCancel] session1 => overnight move >=200 => session1Allowed=false");
      }
      else
      {
         session1Allowed=true;
      }
      overnightStartPrice=0;
   }

   // 14:30 => check midday volatility
   if(ukH==14 && ukM==30)
   {
      if(middayStartPrice != 0 &&
         MathAbs(bidPrice - middayStartPrice) >= MiddayVolatilityLimit)
      {
         session2Allowed=false;
         Print("[VolatilityCancel] session2 => midday move >=150 => session2Allowed=false");
      }
      else
      {
         session2Allowed=true;
      }
      middayStartPrice=0;
   }
}

// ---------------------------------------------------------------------------
//                          MODULE 8: MONEY MANAGEMENT
// ---------------------------------------------------------------------------

int CountOpenTrades()
{
   return activeTradeCount;
}

// ---------------------------------------------------------------------------
//                         MODULE 9: UTILITY FUNCTIONS
// ---------------------------------------------------------------------------

void UpdateTradePeakDuringOpen()
{
   for(int i=0; i<ArraySize(trades); i++)
   {
      if(!trades[i].isActive) continue;
      if(!OrderSelect(trades[i].ticket, SELECT_BY_TICKET) ||
         OrderSymbol()!=SymbolToTrade)
      {
         continue;
      }

      if(OrderType()==OP_BUY)
      {
         if(trades[i].sessionNum==1 && session1.isInitialized)
         {
            if(session1.lowPrice < trades[i].peakLow)
            {
               trades[i].peakLow = session1.lowPrice;
               trades[i].peakTime= TimeCurrent();
            }
         }
         else if(trades[i].sessionNum==2 && session2.isInitialized)
         {
            if(session2.lowPrice < trades[i].peakLow)
            {
               trades[i].peakLow = session2.lowPrice;
               trades[i].peakTime= TimeCurrent();
            }
         }
      }
      else if(OrderType()==OP_SELL)
      {
         if(trades[i].sessionNum==1 && session1.isInitialized)
         {
            if(session1.highPrice > trades[i].peakHigh)
            {
               trades[i].peakHigh = session1.highPrice;
               trades[i].peakTime= TimeCurrent();
            }
         }
         else if(trades[i].sessionNum==2 && session2.isInitialized)
         {
            if(session2.highPrice > trades[i].peakHigh)
            {
               trades[i].peakHigh = session2.highPrice;
               trades[i].peakTime= TimeCurrent();
            }
         }
      }
   }
}

// ---------------------------------------------------------------------------
//                         MODULE 10: MAIN EVENT HANDLERS
// ---------------------------------------------------------------------------

int OnInit()
{
    SymbolToTrade   = Symbol();
    globalVarPrefix = SanitizeSymbol(SymbolToTrade);

    serverToUK      = GetServerToUKOffset();
    PrintFormat("[OnInit] serverToUK offset = %d h", serverToUK);

    if(MathAbs(serverToUK) > 15)
    {
        PrintFormat("[OnInitError] serverToUK = %d h looks corrupt, aborting.", serverToUK);
        return INIT_FAILED;
    }

    datetime ukTime = GetUKTime();
    Print("[OnInit] Current BrokerTime=", TimeToString(TimeCurrent(), TIME_MINUTES),
          ", UKTime=", TimeToString(ukTime, TIME_MINUTES));

    InitializeSessionData();
    return INIT_SUCCEEDED;
}

void OnDeinit(const int reason)
{
   Print("[OnDeinit] reason: ", reason);
}

void OnTick()
{
    if(!AdjustPricesBySpread()) return;  // Bail on very first 0-price tick

    // 1) Session init
    CheckSessionInitialization();

    // 2) Sweeps & Volatility
    HandleSweepsAndVolatility();

    // 3) Update session high/low
    double currentPrice = SymbolInfoDouble(SymbolToTrade, SYMBOL_BID);
    UpdateSessionHighLow(session1, currentPrice);
    UpdateSessionHighLow(session2, currentPrice);

    // 4) Update trade peaks
    UpdateTradePeakDuringOpen();

    // 5) Attempt new trades in session1
    CheckAndPlaceTrade(session1, currentPrice, 1);

    // 6) Attempt new trades in session2
    CheckAndPlaceTrade(session2, currentPrice, 2);

    // 7) Check for closures
    CheckTradeClosure();

    // Diagnostics
    Print("[SessionDebug] session1Initialized=",
          (string)session1.isInitialized,
          ", session2Initialized=",
          (string)session2.isInitialized);
}

// ---------------------------------------------------------------------------
// universal trade-closure logger
// ---------------------------------------------------------------------------
void LogCloseDetails(const int ticket)
{
    if (!OrderSelect(ticket, SELECT_BY_TICKET, MODE_HISTORY))
    {
        if (!OrderSelect(ticket, SELECT_BY_TICKET, MODE_TRADES))
        {
            Print("[LogCloseDetails] Could not locate order #", ticket,
                  " in history or trades, err=", GetLastError());
            return;
        }
    }

    string  side   = (OrderType() == OP_BUY) ? "BUY" : "SELL";
    double  openP  = OrderOpenPrice();
    double  closeP = OrderClosePrice();
    double  lots   = OrderLots();
    double  pl     = OrderProfit() + OrderSwap() + OrderCommission();

    datetime openT  = OrderOpenTime();
    datetime closeT = OrderCloseTime();
    long     secs   = (long)(closeT - openT);

    PrintFormat("[TradeClosed] #%d %s %.2f → %.2f  lots=%.2f  P/L=%.2f  held=%ld s",
                ticket, side, openP, closeP, lots, pl, secs);
    Print("[Debug] ClosePrice=", DoubleToString(closeP, 2));
}

// ---------------------------------------------------------------------------
// Index to broker points
// ---------------------------------------------------------------------------
#define IDX 10
double IdxPts(double x)
{
    return x * Point * IDX;
}
// ---- End of File ----
