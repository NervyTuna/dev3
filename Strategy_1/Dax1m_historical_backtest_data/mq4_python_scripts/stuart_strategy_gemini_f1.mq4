//+------------------------------------------------------------------+
//|            DAX30 Strategy EA - Demo Account Version              |
//| Updated for IG.com.au GER30 with UK time handling                |
//| Fixed session tracking, trade memory, and trade triggering       |
//| Added diagnostic logging and Point validation                  |
//| Compiler fixes from v1.06                                        |
//+------------------------------------------------------------------+

#property copyright "Your Name"

// === Stuart Strategy Fixes (2025‑05‑13) ==================================
// 1. 1 index point on GER30 = 10 broker “points” (Point = 0.1).
// 2. Introduce IDX = 10 and helper IdxPts(x) for safe conversions.
// 3. All price offsets (levels, SL, volatility thresholds) now call
//    IdxPts() instead of raw * Point.
// ==========================================================================
#define IDX 10 // Defines how many broker "Points" make 1 index point (e.g., for DAX, if Point=0.1, IDX=10 means 1 DAX pt = 10 * 0.1 = 1.0 price units)

double IdxPts(double idxValue) { // Converts index points (like 40 DAX points) to price offset
   double currentPoint = MarketInfo(Symbol(), MODE_POINT);
   if (currentPoint <= 0) {
      PrintFormat("Error in IdxPts: Invalid Point value (%.5f) for symbol %s. Cannot calculate price offset for %.1f index points.", currentPoint, Symbol(), idxValue);
      // Return a very large number to likely prevent trade or a sensible default if applicable
      // For safety, returning 0 here means no offset, which might be less harmful than a wrong offset.
      // Or, the functions calling this should also check.
      return 0.0; 
   }
   return NormalizeDouble(idxValue * currentPoint * IDX, Digits);
}


#property link      "https://www.example.com"
#property version   "1.07" // Updated version with compiler fixes
#property strict

//--- MODULE 1: INPUTS & GLOBAL SETTINGS
string SymbolToTrade; 
input ENUM_TIMEFRAMES ChartTimeframe = PERIOD_M1; 
input string Session1StartTime    = "08:00";     
input string Session1EndTime      = "12:30";     
input string Session2StartTime    = "14:30";     
input string Session2EndTime      = "17:16";     
input int ZoneEntryLevel1         = 45;          
input int ZoneEntryLevel2         = 70;          
input int ZoneEntryLevel3         = 100;         
input int ZoneEntryLevel4         = 130;         
input int ZoneCancelLevel         = 160;         
input double OvernightVolatilityLimit = 200;     
input double MiddayVolatilityLimit    = 150;     
input double RetractTolerance     = 9.0;         // Index points
input double RetractMin_1         = 15.0;        // Index points
input double RetractMax_1         = 29.9;        // Index points
input double RetractAdjustBeyondHL = 18.0;       // Index points
input double RetractMin_2         = 30.0;        // Index points
input double RetractMax_2         = 35.9;        // Index points
input double RetractMin_3         = 36.0;        // Index points
input double RetractMax_3         = 45.9;        // Index points
input double RetractCancelThreshold = 46.0;      // Index points
input int AdverseMove1            = 15;          // Index points
input int CloseTimeMinLevel1      = 16;          
input int CloseTimeMinLevel2      = 31;          
input double BetSizingFactor      = 800.0;       
input double MinTradeSize         = 0.1;         
input int SlippagePoints          = 3;           // Broker points for OrderSend
input string TradeComment         = "DAX30Strategy"; 
input int StopLossPoints          = 40;          // Index points

struct SpreadTime {
   string startTime; // CET string
   string endTime;   // CET string
   double spreadPoints; // Index points (e.g., 1.2 for DAX)
};

SpreadTime spreadSchedule[] = {
   {"01:15", "08:00", 4.0}, // CET times, spread in index points
   {"08:00", "09:00", 2.0},
   {"09:00", "17:30", 1.2},
   {"17:30", "22:00", 2.0},
   {"22:00", "01:15", 5.0}  // Next day 01:15 CET
};

struct TradeData {
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

int Session1TradeCount = 0;
int Session2TradeCount = 0;
bool session1Allowed = true;
bool session2Allowed = true;

int levels[4]; 
string globalVarPrefix; 

//+------------------------------------------------------------------+
//|              MODULE 2: TIME MANAGEMENT (UK TIME)                 |
//+------------------------------------------------------------------+
bool IsUKDST(datetime timeToCheck) { // Parameter is any server time
   MqlDateTime dt;
   TimeToStruct(timeToCheck, dt);
   int year = dt.year;

   // Find last Sunday in March for the given year (UK DST starts 01:00 GMT)
   datetime marchLastSundayGMT = 0;
   for (int dMarch = 31; dMarch >= 25; dMarch--) {
      datetime tempDate = StringToTime(StringFormat("%04d.03.%02d 01:00", year, dMarch));
      if (tempDate != 0 && TimeDayOfWeek(tempDate) == 0) { // Sunday
         marchLastSundayGMT = tempDate;
         break;
      }
   }

   // Find last Sunday in October for the given year (UK DST ends 01:00 GMT, which is 02:00 BST)
   datetime octoberLastSundayGMT = 0;
   for (int dOctober = 31; dOctober >= 25; dOctober--) {
      datetime tempDate = StringToTime(StringFormat("%04d.10.%02d 01:00", year, dOctober));
       if (tempDate != 0 && TimeDayOfWeek(tempDate) == 0) { // Sunday
         octoberLastSundayGMT = tempDate;
         break;
      }
   }
   if (marchLastSundayGMT == 0 || octoberLastSundayGMT == 0) return false; // Error in date calculation

   // Check if the 'timeToCheck' (adjusted to GMT) falls within the DST period
   datetime timeToCheckGMT = timeToCheck - TimeGMTOffset(); // Convert server time to GMT
   return (timeToCheckGMT >= marchLastSundayGMT && timeToCheckGMT < octoberLastSundayGMT);
}

datetime GetUKTime() {
   datetime serverTime = TimeCurrent();
   int gmtOffsetSeconds = TimeGMTOffset(); // Server's offset from GMT in seconds
   datetime gmtTime = serverTime - gmtOffsetSeconds;
   
   if (IsUKDST(serverTime)) { // Check if UK is in DST based on current server time
      return gmtTime + 3600; // UK Time = GMT + 1 hour (BST)
   } else {
      return gmtTime; // UK Time = GMT
   }
}

datetime CETToUKTime(string cetTimeStr) { // cetTimeStr is "HH:MM"
    datetime ukNow = GetUKTime();
    string ukDateStr = TimeToString(ukNow, TIME_DATE); // Current UK date
    datetime cetTimeOnUKDate = StringToTime(ukDateStr + " " + cetTimeStr); // CET time on current UK date (naive)

    if (cetTimeOnUKDate == 0) {
        Print("Error converting CET time string '", cetTimeStr, "' with UK date '", ukDateStr, "'");
        return 0;
    }

    // CET is GMT+1 (standard) or GMT+2 (CEST/DST)
    // UK is GMT+0 (standard) or GMT+1 (BST/DST)

    // Determine if CET's region would be in DST. For simplicity, assume CET DST aligns with UK DST for now.
    // A more robust solution would use specific CET DST rules or a library if available.
    bool isServerTimeInUKDST = IsUKDST(TimeCurrent()); // Is the UK currently in DST?
    
    // Assuming CET observes DST at the same period as UK for this conversion.
    // So, if UK is in BST, assume CET is in CEST. If UK is in GMT, assume CET is in CET.
    int cetOffsetFromGMT = isServerTimeInUKDST ? 2 : 1; // CEST = GMT+2, CET = GMT+1
    int ukOffsetFromGMT  = isServerTimeInUKDST ? 1 : 0; // BST  = GMT+1, GMT = GMT+0
    
    // Difference in hours that CET is ahead of UK time
    int cetAheadOfUK_Hours = cetOffsetFromGMT - ukOffsetFromGMT;

    // To get the equivalent UK time for a given CET time, we "go back" by cetAheadOfUK_Hours
    // This is effectively converting the CET time (on UK's current date) to GMT, then to UK time.
    // OR, more directly: UK_Time_for_CET_event = CET_event_time (as if it happened on UK date) - hours_CET_is_ahead
    datetime equivalentUKTime = cetTimeOnUKDate - (cetAheadOfUK_Hours * 3600);
    
    // PrintFormat("CETToUKTime: CET Str: %s, UKDate: %s, CET_on_UKDate: %s, isUKDST: %s, CETaheadUK: %d, EquivUKTime: %s",
    //             cetTimeStr, ukDateStr, TimeToString(cetTimeOnUKDate), (string)isServerTimeInUKDST, cetAheadOfUK_Hours, TimeToString(equivalentUKTime));

    return equivalentUKTime;
}


double GetSpreadForTime() { // Removed 'now' parameter, uses GetUKTime()
   datetime ukNow = GetUKTime();
   string ukTimeStr = TimeToString(ukNow, TIME_MINUTES);

   for (int i = 0; i < ArraySize(spreadSchedule); i++) {
      datetime startTimeUK = CETToUKTime(spreadSchedule[i].startTime);
      datetime endTimeUK = CETToUKTime(spreadSchedule[i].endTime);

      if (startTimeUK == 0 || endTimeUK == 0) { // Error in conversion
          Print("GetSpreadForTime: Error converting schedule times for entry ", i);
          continue; 
      }
      
      // Handle schedules that cross midnight (e.g. 22:00 CET to 01:15 CET next day)
      if (StringCompare(spreadSchedule[i].startTime, spreadSchedule[i].endTime) > 0) { // Start time string is "later" than end time string
         // This means the period crosses midnight CET.
         // Convert to UK dates considering this.
         // Example: 22:00 CET to 01:15 CET.
         // If current UK time is e.g. 21:30 (which is 22:30 CET), it's in the 22:00 part.
         // If current UK time is e.g. 00:30 (which is 01:30 CET), it's in the 01:15 part.
         
         // We need to check if ukNow is (startTimeUK today TO 23:59 UK today) OR (00:00 UK today TO endTimeUK today)
         // This requires careful handling if CETToUKTime already places it on "correct" UK day.
         // The current CETToUKTime uses current UK date for both start/end CET string conversion.

         // If endTimeUK is "earlier" in the day than startTimeUK, it implies overnight for UK context
         if (endTimeUK < startTimeUK) { 
            if (ukNow >= startTimeUK || ukNow <= endTimeUK) { // Matches (22:00-23:59) OR (00:00-01:15)
                 Print("Spread (UK Time " + ukTimeStr + "): " + DoubleToString(spreadSchedule[i].spreadPoints, 1) + " (Overnight schedule)");
                 return spreadSchedule[i].spreadPoints;
            }
         } else { // Should not happen if start string > end string, but as a fallback
             if (ukNow >= startTimeUK && ukNow <= endTimeUK) {
                 Print("Spread (UK Time " + ukTimeStr + "): " + DoubleToString(spreadSchedule[i].spreadPoints, 1));
                 return spreadSchedule[i].spreadPoints;
             }
         }
      } else { // Standard same-day schedule
         if (ukNow >= startTimeUK && ukNow <= endTimeUK) {
            Print("Spread (UK Time " + ukTimeStr + "): " + DoubleToString(spreadSchedule[i].spreadPoints, 1));
            return spreadSchedule[i].spreadPoints;
         }
      }
   }
   Print("No spread schedule match for UK Time: " + ukTimeStr + ", defaulting to 5.0 index points.");
   return 5.0; 
}

string SanitizeSymbol(string symbol) {
   string sanitized = symbol;
   StringReplace(sanitized, "(", "_");
   StringReplace(sanitized, ")", "_");
   StringReplace(sanitized, "£", "GBP"); 
   return sanitized;
}

void AdjustPricesBySpread() {
   double currentPoint = MarketInfo(SymbolToTrade, MODE_POINT);
   int currentDigits = MarketInfo(SymbolToTrade, MODE_DIGITS);
   if (currentPoint <= 0) {
      PrintFormat("CRITICAL Error in AdjustPricesBySpread: Invalid Point (%.5f)", currentPoint);
      GlobalVariableSet(globalVarPrefix + "_AdjBid", 0); // Prevent use of bad data
      GlobalVariableSet(globalVarPrefix + "_AdjAsk", 0);
      GlobalVariableSet(globalVarPrefix + "_Spread", 0);
      return;
   }

   double scheduledSpreadIdxPts = GetSpreadForTime(); // This is in index points (e.g. 1.2)
   double marketBid = SymbolInfoDouble(SymbolToTrade, SYMBOL_BID);
   double marketAsk = SymbolInfoDouble(SymbolToTrade, SYMBOL_ASK);

   if (!IsPriceValid(marketBid) || !IsPriceValid(marketAsk)) {
      Print("Error: Invalid market bid/ask prices in AdjustPricesBySpread. Bid: " + DoubleToString(marketBid, currentDigits) + ", Ask: " + DoubleToString(marketAsk, currentDigits));
      GlobalVariableSet(globalVarPrefix + "_AdjBid", 0);
      GlobalVariableSet(globalVarPrefix + "_AdjAsk", 0);
      GlobalVariableSet(globalVarPrefix + "_Spread", 0);
      return;
   }
   
   // For EA's internal logic, simulate prices based on the scheduled spread
   double midPrice = NormalizeDouble((marketBid + marketAsk) / 2.0, currentDigits + 1); // Mid with more precision
   double halfSpreadInPrice = NormalizeDouble(IdxPts(scheduledSpreadIdxPts) / 2.0, currentDigits + 1);

   double logicalBid = NormalizeDouble(midPrice - halfSpreadInPrice, currentDigits);
   double logicalAsk = NormalizeDouble(midPrice + halfSpreadInPrice, currentDigits);

   PrintFormat("AdjustPrices: Market B/A: %.*f/%.*f. SchedSpreadIdx: %.1f. Logical B/A for EA: %.*f/%.*f",
               currentDigits, marketBid, currentDigits, marketAsk, scheduledSpreadIdxPts,
               currentDigits, logicalBid, currentDigits, logicalAsk);

   GlobalVariableSet(globalVarPrefix + "_AdjBid", logicalBid);
   GlobalVariableSet(globalVarPrefix + "_AdjAsk", logicalAsk);
   GlobalVariableSet(globalVarPrefix + "_Spread", scheduledSpreadIdxPts); // Store scheduled spread in INDEX POINTS
}

// MODULE 3: SESSION HANDLING (largely same, ensure UK time for session defs)
struct SessionData {
   datetime startTimeUK; // Changed to reflect UK time
   datetime endTimeUK;   // Changed to reflect UK time
   double openPrice;
   double highPrice;
   double lowPrice;
   bool isInitialized;
   datetime peakTime; // Server time of peak
};
SessionData session1;
SessionData session2;
datetime lastSessionInitDateUK = 0; // Store UK date for reset logic

void InitializeSessionData() {
   datetime ukNow = GetUKTime();
   string ukDateStr = TimeToString(ukNow, TIME_DATE);
   
   session1.startTimeUK = StringToTime(ukDateStr + " " + Session1StartTime);
   session1.endTimeUK   = StringToTime(ukDateStr + " " + Session1EndTime);
   session2.startTimeUK = StringToTime(ukDateStr + " " + Session2StartTime);
   session2.endTimeUK   = StringToTime(ukDateStr + " " + Session2EndTime);
   
   if (session1.startTimeUK > session1.endTimeUK && session1.endTimeUK != 0) session1.endTimeUK += 86400; 
   if (session2.startTimeUK > session2.endTimeUK && session2.endTimeUK != 0) session2.endTimeUK += 86400; 
   
   session1.isInitialized = false;
   session2.isInitialized = false;
   
   Print("Session 1 UK Time - Start: " + TimeToString(session1.startTimeUK, TIME_MINUTES) + 
         " End: " + TimeToString(session1.endTimeUK, TIME_MINUTES));
   Print("Session 2 UK Time - Start: " + TimeToString(session2.startTimeUK, TIME_MINUTES) + 
         " End: " + TimeToString(session2.endTimeUK, TIME_MINUTES));
}
void ResetSessionData(SessionData &session, double priceToUse) { 
   if (!IsPriceValid(priceToUse)) { 
      Print("Error: Invalid price for session initialization: " + DoubleToString(priceToUse, Digits));
      return;
   }
   session.openPrice = priceToUse;
   session.highPrice = priceToUse;
   session.lowPrice = priceToUse;
   session.isInitialized = true;
   session.peakTime = TimeCurrent(); 
}
void CheckSessionInitialization() {
   datetime ukNow = GetUKTime();
   datetime ukDateOnly = StringToTime(TimeToString(ukNow, TIME_DATE));
   
   if (lastSessionInitDateUK != ukDateOnly) {
      InitializeSessionData();
      lastSessionInitDateUK = ukDateOnly;
      Session1TradeCount = 0;
      Session2TradeCount = 0;
      session1Allowed = true; // Reset daily allowance
      session2Allowed = true;
      Print("Daily session data reset for UK date " + TimeToString(ukDateOnly, TIME_DATE));
   }
   
   double currentMarketBid = SymbolInfoDouble(SymbolToTrade, SYMBOL_BID);
   if (!IsPriceValid(currentMarketBid)) {
      Print("Error: Invalid current market bid price in CheckSessionInitialization: " + DoubleToString(currentMarketBid, Digits));
      return;
   }
      
   if (ukNow >= session1.startTimeUK && ukNow <= session1.endTimeUK) { 
      if (!session1.isInitialized) {
         ResetSessionData(session1, currentMarketBid);
         Print("Session 1 Initialized (UK Time). Open Price: " + DoubleToString(session1.openPrice, Digits));
      }
   } else {
      if (session1.isInitialized) { 
         session1.isInitialized = false;
         Print("Session 1 Data Reset - Outside UK Time Window");
      }
   }
      
   if (ukNow >= session2.startTimeUK && ukNow <= session2.endTimeUK) { 
      if (!session2.isInitialized) {
         ResetSessionData(session2, currentMarketBid);
         Print("Session 2 Initialized (UK Time). Open Price: " + DoubleToString(session2.openPrice, Digits));
      }
   } else {
      if (session2.isInitialized) { 
         session2.isInitialized = false;
         Print("Session 2 Data Reset - Outside UK Time Window");
      }
   }
}
void UpdateSessionHighLow(SessionData &s, double price) {
   if (!s.isInitialized || !IsPriceValid(price)) return; 
   if (price > s.highPrice) {
      s.highPrice = price;
      s.peakTime = TimeCurrent(); 
   }
   if (price < s.lowPrice) {
      s.lowPrice = price;
      s.peakTime = TimeCurrent(); 
   }
}

// MODULE 4: ACTIVE TRADING ZONES
struct ActiveZone {
   string startTimeUK; // Changed to UK
   string endTimeUK;   // Changed to UK
   int requiredDistanceIdx; // Changed name for clarity (Index Points)
};
ActiveZone session1Zones[] = { // Times are UK HH:MM, distance in Index Points
   {"08:16", "09:05", 45}, {"09:30", "09:45", 45}, {"10:15", "10:45", 70},
   {"10:45", "11:45", 45}, {"11:45", "12:31", 45}
};
ActiveZone session2Zones[] = { // Times are UK HH:MM, distance in Index Points
   {"14:46", "15:06", 45}, {"15:15", "15:45", 70}, {"15:45", "16:48", 45}
};

bool IsWithinTimeWindow(string startUK_HHMM, string endUK_HHMM) { 
   datetime ukNow = GetUKTime(); 
   string ukDateStr = TimeToString(ukNow, TIME_DATE);
   
   datetime startTimeFullUK = StringToTime(ukDateStr + " " + startUK_HHMM);
   datetime endTimeFullUK = StringToTime(ukDateStr + " " + endUK_HHMM);

   if (startTimeFullUK == 0 || endTimeFullUK == 0) {
      Print("Error converting zone UK time strings: ", startUK_HHMM, " or ", endUK_HHMM);
      return false;
   }
      
   if (startTimeFullUK > endTimeFullUK && endTimeFullUK != 0) endTimeFullUK += 86400; 
      
   return (ukNow >= startTimeFullUK && ukNow <= endTimeFullUK);
}

bool IsInActiveTradingZone(SessionData &session, int sessionNum, double currentMarketPrice) { 
   if (!session.isInitialized || !IsPriceValid(currentMarketPrice)) {
      Print("IsInActiveTradingZone: Session not initialized or invalid current market price (" + DoubleToString(currentMarketPrice, Digits) + ")");
      return false;
   }
   
   double currentPoint = MarketInfo(SymbolToTrade, MODE_POINT);
   if (currentPoint <= 0) {
      PrintFormat("CRITICAL Error in IsInActiveTradingZone: Invalid Point (%.5f)", currentPoint);
      return false;
   }
   
   double distanceBrokerPts = MathAbs(currentMarketPrice - session.openPrice); 
   double distanceIdxPts = distanceBrokerPts / (currentPoint * IDX); 

   PrintFormat("IsInActiveTradingZone - Session: %d, UK Time: %s, CurrentMktPrice: %.*f, SessionOpen: %.*f, DistanceIdxPts: %.1f",
               sessionNum, TimeToString(GetUKTime(), TIME_MINUTES), Digits, currentMarketPrice, Digits, session.openPrice, distanceIdxPts);
   
   if (distanceIdxPts < levels[0]) { 
      Print("Distance too small: " + DoubleToString(distanceIdxPts, 1) + " < " + IntegerToString(levels[0]));
      return false;
   }
      
   ActiveZone zonesToIterate[]; // CORRECTED: Local dynamic array
   int size;

   if (sessionNum == 1) {
       size = ArraySize(session1Zones);
       ArrayResize(zonesToIterate, size);
       for(int k=0; k < size; k++) zonesToIterate[k] = session1Zones[k];
   } else {
       size = ArraySize(session2Zones);
       ArrayResize(zonesToIterate, size);
       for(int k=0; k < size; k++) zonesToIterate[k] = session2Zones[k];
   }

   for (int i = 0; i < ArraySize(zonesToIterate); i++) {
      bool inWindow = IsWithinTimeWindow(zonesToIterate[i].startTimeUK, zonesToIterate[i].endTimeUK);
      double targetDistanceIdx = zonesToIterate[i].requiredDistanceIdx; 
      bool meetsDistance = distanceIdxPts >= targetDistanceIdx;
      
      PrintFormat("Zone %d: UK Start: %s, UK End: %s, InWindow: %s, TargetDistIdx: %.1f, ActualDistIdx: %.1f",
                  i, zonesToIterate[i].startTimeUK, zonesToIterate[i].endTimeUK, 
                  (string)inWindow, targetDistanceIdx, distanceIdxPts);
      
      if (inWindow && meetsDistance) {
         Print("Trade Trigger Possible - Zone " + IntegerToString(i) + ", ActualDistIdx: " + DoubleToString(distanceIdxPts, 1));
         return true;
      }
   }
   Print("No trade zone triggered.");
   return false;
}

// MODULE 5: RETRACTION & TOLERANCE
bool IsWithinTolerance(double price, double targetPrice) {
   double currentPoint = MarketInfo(SymbolToTrade, MODE_POINT);
   if (currentPoint <= 0) {
      PrintFormat("CRITICAL Error in IsWithinTolerance: Invalid Point (%.5f)", currentPoint);
      return false; 
   }
   double scheduledSpreadIdxPts = GlobalVariableGet(globalVarPrefix + "_Spread"); 
   // Tolerance is RetractTolerance (idx_pts) + scheduledSpread (idx_pts)
   // Convert total tolerance in idx_pts to price offset using IdxPts()
   return MathAbs(price - targetPrice) <= IdxPts(RetractTolerance + scheduledSpreadIdxPts);
}
double CalculateRetraction(SessionData &session, double currentPrice, bool isBuy) {
   if (!IsPriceValid(currentPrice) || !session.isInitialized) {
      Print("Error: Invalid current price ("+DoubleToString(currentPrice,Digits)+") or session not init in CalculateRetraction");
      return 0;
   }
   double currentPoint = MarketInfo(SymbolToTrade, MODE_POINT);
   if (currentPoint <= 0) {
      PrintFormat("CRITICAL Error in CalculateRetraction: Invalid Point (%.5f)", currentPoint);
      return 0; 
   }

   double retractionBrokerPts;
   if (isBuy) { 
      retractionBrokerPts = currentPrice - session.lowPrice; // Price units
   } else { 
      retractionBrokerPts = session.highPrice - currentPrice; // Price units
   }
   double retractionIdxPts = retractionBrokerPts / (currentPoint * IDX); 

   PrintFormat("RetractionCalc - IsBuy: %s, SessionHigh: %.*f, SessionLow: %.*f, CurrentPrice: %.*f, RetractionIdxPts: %.1f",
               (string)isBuy, Digits, session.highPrice, Digits, session.lowPrice, Digits, currentPrice, retractionIdxPts);
   return retractionIdxPts; 
}
int GetRetractionLevel(double retractionIdxPts) { 
   if (retractionIdxPts >= RetractMin_1 && retractionIdxPts <= RetractMax_1) return 1;
   else if (retractionIdxPts >= RetractMin_2 && retractionIdxPts <= RetractMax_2) return 2;
   else if (retractionIdxPts >= RetractMin_3 && retractionIdxPts <= RetractMax_3) return 3;
   else if (retractionIdxPts >= RetractCancelThreshold) return -1; 
   return 0; 
}
int AdjustEntryLevel(int originalLevelIndex, int retractionCategory) { 
   if (retractionCategory == 1) return originalLevelIndex; 
   else if (retractionCategory == 2) return originalLevelIndex + 1; 
   else if (retractionCategory == 3) return originalLevelIndex + 2; 
   else if (retractionCategory == -1) return -1; 
   return originalLevelIndex; 
}

// MODULE 6: TRADE ENTRY & EXIT LOGIC
int PlaceTrade(string type, double intendedEntryPrice, double lotSize, int sessionNum) { 
   double currentPoint = MarketInfo(SymbolToTrade, MODE_POINT);
   int currentDigits = MarketInfo(SymbolToTrade, MODE_DIGITS);
   if (currentPoint <= 0) {
      PrintFormat("CRITICAL Error in PlaceTrade: Invalid Point (%.5f). Cannot place trade.", currentPoint);
      return -1;
   }

   double priceForOrderSend; 
   if (type == "BUY") priceForOrderSend = SymbolInfoDouble(SymbolToTrade, SYMBOL_ASK);
   else priceForOrderSend = SymbolInfoDouble(SymbolToTrade, SYMBOL_BID);

   if (!IsPriceValid(priceForOrderSend)) {
      Print("Error: Invalid market price for OrderSend in PlaceTrade. Type: " + type + ", Price: " + DoubleToString(priceForOrderSend, currentDigits));
      return -1;
   }
   
   double stopLossOffsetPrice = IdxPts(StopLossPoints); 
   double stopLossPrice;
   if (type == "BUY") stopLossPrice = NormalizeDouble(intendedEntryPrice - stopLossOffsetPrice, currentDigits);
   else stopLossPrice = NormalizeDouble(intendedEntryPrice + stopLossOffsetPrice, currentDigits);

   PrintFormat("PlaceTrade Attempt - Type: %s, IntendedEntry: %.*f, OrderSendPrice: %.*f, SL: %.*f, Lot: %.2f",
               type, currentDigits, intendedEntryPrice, currentDigits, priceForOrderSend, currentDigits, stopLossPrice, lotSize);
   
   int orderTypeCmd = (type == "BUY") ? OP_BUY : OP_SELL; // Renamed for clarity
   int ticket = -1;

   for (int attempt = 0; attempt < 5; attempt++) {
      RefreshRates(); 
      if (type == "BUY") priceForOrderSend = SymbolInfoDouble(SymbolToTrade, SYMBOL_ASK);
      else priceForOrderSend = SymbolInfoDouble(SymbolToTrade, SYMBOL_BID);

      if (!IsPriceValid(priceForOrderSend)) {
         Print("PlaceTrade Attempt " + (string)(attempt+1) + ": Invalid market price for OrderSend. Price: " + DoubleToString(priceForOrderSend, currentDigits));
         Sleep(500);
         continue;
      }
      
      double stopLevelPrice = MarketInfo(SymbolToTrade, MODE_STOPLEVEL) * currentPoint;
      if (type == "BUY" && stopLossPrice >= priceForOrderSend - stopLevelPrice) {
         PrintFormat("PlaceTrade Attempt %d: Calc SL (%.*f) too close to Ask (%.*f) or wrong side. Adjusting.", attempt+1, currentDigits, stopLossPrice, currentDigits, priceForOrderSend);
         stopLossPrice = NormalizeDouble(priceForOrderSend - (stopLevelPrice + IdxPts(1)), currentDigits); 
         Print("Adjusted SL to: " + DoubleToString(stopLossPrice, currentDigits));
      } else if (type == "SELL" && stopLossPrice <= priceForOrderSend + stopLevelPrice) {
         PrintFormat("PlaceTrade Attempt %d: Calc SL (%.*f) too close to Bid (%.*f) or wrong side. Adjusting.", attempt+1, currentDigits, stopLossPrice, currentDigits, priceForOrderSend);
         stopLossPrice = NormalizeDouble(priceForOrderSend + (stopLevelPrice + IdxPts(1)), currentDigits); 
         Print("Adjusted SL to: " + DoubleToString(stopLossPrice, currentDigits));
      }

      ticket = OrderSend(SymbolToTrade, orderTypeCmd, lotSize, priceForOrderSend, SlippagePoints, 
                         stopLossPrice, 0, TradeComment, 0, 0, clrBlue);
      if (ticket >= 0) {
         if (OrderSelect(ticket, SELECT_BY_TICKET)) {
            PrintFormat("Trade Details Post-Open: Ticket %d, OpenPrice=%.*f, SL=%.*f, TP=%.*f, Point=%.5f, Digits=%d, Lots=%.2f",
                        ticket, currentDigits, OrderOpenPrice(), currentDigits, OrderStopLoss(), currentDigits, OrderTakeProfit(),
                        currentPoint, currentDigits, OrderLots());

            if (MathAbs(OrderStopLoss() - stopLossPrice) <= currentPoint * 2.0) { 
               Print("Trade opened successfully. Ticket: " + IntegerToString(ticket));
               for (int j = 0; j < ArraySize(trades); j++) { 
                  if (!trades[j].isActive) {
                     trades[j].ticket = ticket;
                     trades[j].entryPrice = OrderOpenPrice(); 
                     trades[j].adjustedEntryPrice = intendedEntryPrice; 
                     trades[j].peakHigh = OrderOpenPrice(); 
                     trades[j].peakLow = OrderOpenPrice();  
                     trades[j].peakTime = TimeCurrent();
                     trades[j].sessionNum = sessionNum;
                     trades[j].isActive = true;
                     activeTradeCount++;
                     Print("Trade Stored - Ticket: " + IntegerToString(ticket) + " for Intended Entry: " + DoubleToString(intendedEntryPrice, currentDigits));
                     return ticket; 
                  }
               }
               Print("Error: No inactive slot in trades array to store new trade " + IntegerToString(ticket)); // Should not happen if array is large enough
               CloseTradeSafely(ticket); // Close it as we can't track it
               return -1;
            } else {
               PrintFormat("Stop loss mismatch. Ticket %d. Expected SL: %.*f, Actual SL: %.*f. Closing trade.", 
                           ticket, currentDigits, stopLossPrice, currentDigits, OrderStopLoss());
               CloseTradeSafely(ticket); 
               return -1; 
            }
         } else {
             Print("OrderSend succeeded but OrderSelect failed. Ticket: " + IntegerToString(ticket) + " Error: " + IntegerToString(GetLastError()));
             return -1; // Consider it a failure if we can't select to verify
         }
      } else { 
         int err = GetLastError();
         Print("Trade attempt " + IntegerToString(attempt + 1) + " failed. Error: " + IntegerToString(err));
         if (err == ERR_NO_CONNECTION || err == ERR_TRADE_TIMEOUT || err == ERR_SERVER_BUSY) Sleep(1000); 
         else Sleep(500);
      }
   }
   Print("Failed to open trade after 5 attempts.");
   return -1; 
}
void CheckAndPlaceTrade(SessionData &session, double currentMarketPrice, int sessionNum) { 
   if (!IsPriceValid(currentMarketPrice)) {
      Print("Error: Invalid current market price in CheckAndPlaceTrade: " + DoubleToString(currentMarketPrice, Digits));
      return;
   }
   PrintFormat("CheckAndPlaceTrade - Session: %d, UK Time: %s, CurrentMktPrice: %.*f",
               sessionNum, TimeToString(GetUKTime(), TIME_MINUTES), Digits, currentMarketPrice);
   
   if ((sessionNum == 1 && !session1Allowed) || (sessionNum == 2 && !session2Allowed)) {
      Print("Trade blocked: Session " + IntegerToString(sessionNum) + " cancelled due to volatility/rules.");
      return;
   }
   if ((sessionNum == 1 && Session1TradeCount >= 1) || 
       (sessionNum == 2 && Session2TradeCount >= 1)) {
      Print("Trade blocked: One trade per session limit reached for session " + IntegerToString(sessionNum) + ".");
      return;
   }
      
   bool isBuySetup = currentMarketPrice < session.openPrice; 
   
   int activeLevelIndex = -1;
   double currentPoint = MarketInfo(SymbolToTrade, MODE_POINT);
   if(currentPoint <= 0) { Print("CheckAndPlaceTrade: Invalid Point."); return; }

   for (int i = 0; i < ArraySize(levels); i++) {
      double levelPriceOffset = IdxPts(levels[i]);
      double levelEntryPrice = isBuySetup ? session.openPrice - levelPriceOffset : session.openPrice + levelPriceOffset;
      
      double tolerancePriceOffset = IdxPts(RetractTolerance); // Tolerance in price units
      if (isBuySetup) { // For BUY, current market price (use Ask) should be around or slightly above the level
         if (SymbolInfoDouble(SymbolToTrade, SYMBOL_ASK) >= levelEntryPrice - tolerancePriceOffset) { 
            activeLevelIndex = i; 
            break; 
         }
      } else { // For SELL, current market price (use Bid) should be around or slightly below the level
         if (SymbolInfoDouble(SymbolToTrade, SYMBOL_BID) <= levelEntryPrice + tolerancePriceOffset) {
            activeLevelIndex = i; 
            break;
         }
      }
   }

   if (activeLevelIndex == -1) {
      Print("No active entry level found for current price and tolerance.");
      return; 
   }
      
   double retractionIdxPts = CalculateRetraction(session, currentMarketPrice, isBuySetup);
   int retractionCategory = GetRetractionLevel(retractionIdxPts); 

   if (retractionCategory == -1 && CountOpenTrades() == 0) { 
      if (sessionNum == 1) session1Allowed = false;
      else session2Allowed = false;
      Print("Session " + IntegerToString(sessionNum) + " trading disallowed due to large retraction (" + DoubleToString(retractionIdxPts,1) + " idx pts).");
      return;
   }

   int finalLevelIndex = AdjustEntryLevel(activeLevelIndex, retractionCategory);
   if (finalLevelIndex == -1 || finalLevelIndex >= ArraySize(levels)) { 
      Print("Retraction adjustment resulted in no valid trade level (Index: " + IntegerToString(finalLevelIndex) + ").");
      return;
   }

   int targetLevelIdxPoints = levels[finalLevelIndex]; 
   double intendedEntryPrice = isBuySetup ? session.openPrice - IdxPts(targetLevelIdxPoints) 
                                         : session.openPrice + IdxPts(targetLevelIdxPoints);
   
   double priceToCompareForTolerance = isBuySetup ? SymbolInfoDouble(SymbolToTrade, SYMBOL_ASK) : SymbolInfoDouble(SymbolToTrade, SYMBOL_BID);
   if (!IsPriceValid(priceToCompareForTolerance)) {
       Print("CheckAndPlaceTrade: Invalid market price for final tolerance check.");
       return;
   }

   if (IsWithinTolerance(priceToCompareForTolerance, intendedEntryPrice)) {
      double lotSize = CalculateLotSize(); 
      if (lotSize >= MinTradeSize && CanTrade()) { // Ensure lotSize meets minimum before CanTrade
         string tradeTypeStr = isBuySetup ? "BUY" : "SELL"; // Renamed
         int opened_ticket = PlaceTrade(tradeTypeStr, intendedEntryPrice, lotSize, sessionNum); 
         if (opened_ticket > 0) { 
             if (sessionNum == 1) Session1TradeCount++;
             else Session2TradeCount++;
         }
      } else {
         Print("Cannot trade: Lot size ("+DoubleToString(lotSize,2)+") is too small or insufficient margin.");
      }
   } else {
       PrintFormat("Final tolerance check failed: PriceToCompare (%.*f) not within tolerance of IntendedEntry (%.*f)",
                   Digits, priceToCompareForTolerance, Digits, intendedEntryPrice);
   }
}
void CheckTradeClosure() {
   datetime now_server = TimeCurrent(); // Server time
   datetime ukNow = GetUKTime(); 
      
   string hardCloseTimeStrUK_S1Z0 = TimeToString(ukNow, TIME_DATE) + " 09:31"; 
   datetime hardCloseDateTimeUK_S1Z0 = StringToTime(hardCloseTimeStrUK_S1Z0);

   double currentPoint = MarketInfo(SymbolToTrade, MODE_POINT);
   if (currentPoint <= 0) {
      PrintFormat("CRITICAL Error in CheckTradeClosure: Invalid Point (%.5f)", currentPoint);
      return;
   }
   
   for (int i = 0; i < ArraySize(trades); i++) {
      if (!trades[i].isActive) continue;
      
      if (OrderSelect(trades[i].ticket, SELECT_BY_TICKET) && OrderSymbol() == SymbolToTrade) {
         double actualOpenPrice = OrderOpenPrice(); 
         double intendedEntryLevelPrice = trades[i].adjustedEntryPrice; 
         
         double currentMarketPriceForExit = (OrderType() == OP_BUY) ? SymbolInfoDouble(SymbolToTrade, SYMBOL_BID) 
                                                                     : SymbolInfoDouble(SymbolToTrade, SYMBOL_ASK);
         if (!IsPriceValid(currentMarketPriceForExit)) {
             Print("CheckTradeClosure: Invalid current market price for ticket ", trades[i].ticket);
             continue;
         }
         
         double sessionOpenForThisTrade = 0;
         if(trades[i].sessionNum == 1 && session1.isInitialized) sessionOpenForThisTrade = session1.openPrice;
         else if (trades[i].sessionNum == 2 && session2.isInitialized) sessionOpenForThisTrade = session2.openPrice;
         
         if (!IsPriceValid(sessionOpenForThisTrade)) {
             PrintFormat("CheckTradeClosure: Session %d open price invalid or session not initialized for trade %d.", 
                         trades[i].sessionNum, trades[i].ticket);
             continue;
         }
                  
         double distanceIntendedLevelFromSessionOpenIdx = MathAbs(intendedEntryLevelPrice - sessionOpenForThisTrade) / (currentPoint * IDX); 

         double adverseMoveInPriceUnits = (OrderType() == OP_BUY) ? (actualOpenPrice - currentMarketPriceForExit) 
                                                                  : (currentMarketPriceForExit - actualOpenPrice); 
         
         if(OrderType() == OP_BUY && currentMarketPriceForExit > trades[i].peakHigh) {
            trades[i].peakHigh = currentMarketPriceForExit; trades[i].peakTime = now_server;
         } else if (OrderType() == OP_SELL && currentMarketPriceForExit < trades[i].peakLow) {
            trades[i].peakLow = currentMarketPriceForExit; trades[i].peakTime = now_server;
         }
         int minutesSincePeak = (trades[i].peakTime != 0) ? (int)((now_server - trades[i].peakTime) / 60) : 9999;

         // Hard Close for Session 1, Zone 0 (08:16-09:05 UK) if trade from this zone is open at 09:31 UK
         // This requires knowing which zone the trade originated from, which is not stored.
         // Assuming any Session 1 trade open at 09:31 UK is subject to this if it was from the early window.
         // For simplicity, if it's a Session 1 trade and it's 09:31 UK:
         if (trades[i].sessionNum == 1 && ukNow >= hardCloseDateTimeUK_S1Z0 && 
             TimeHour(OrderOpenTime()) < 9 && TimeMinute(OrderOpenTime()) < 31 ) { // Crude check if opened before 09:31
             PrintFormat("Trade %d: Hard close at UK %s for potential early Session 1 trade.", trades[i].ticket, TimeToString(hardCloseDateTimeUK_S1Z0, TIME_MINUTES));
             if (CloseTradeSafely(trades[i].ticket)) {
                trades[i].isActive = false; activeTradeCount--;
             }
             continue; 
         }
                  
         if (distanceIntendedLevelFromSessionOpenIdx >= levels[0] && distanceIntendedLevelFromSessionOpenIdx < levels[1]) { 
            if (adverseMoveInPriceUnits >= IdxPts(AdverseMove1)) { 
               bool atOrWorseBE = (OrderType() == OP_BUY && currentMarketPriceForExit <= actualOpenPrice) ||
                                  (OrderType() == OP_SELL && currentMarketPriceForExit >= actualOpenPrice);
               if (atOrWorseBE) {
                  PrintFormat("Trade %d: Breakeven triggered. AdverseMovePrice: %.*f >= %.*f. Closing.", 
                              trades[i].ticket, Digits, adverseMoveInPriceUnits, Digits, IdxPts(AdverseMove1));
                  if (CloseTradeSafely(trades[i].ticket)) {
                     trades[i].isActive = false; activeTradeCount--;
                  }
                  continue;
               }
            }
            if (minutesSincePeak >= CloseTimeMinLevel1) {
               PrintFormat("Trade %d: Timed exit for Level 1. MinsSincePeak: %d >= %d. Closing.", 
                           trades[i].ticket, minutesSincePeak, CloseTimeMinLevel1);
               if (CloseTradeSafely(trades[i].ticket)) {
                  trades[i].isActive = false; activeTradeCount--;
               }
               continue;
            }
         } else if (distanceIntendedLevelFromSessionOpenIdx >= levels[1] && distanceIntendedLevelFromSessionOpenIdx < ZoneCancelLevel) { 
            if (minutesSincePeak >= CloseTimeMinLevel2) {
               PrintFormat("Trade %d: Timed exit for Level 2+. MinsSincePeak: %d >= %d. Closing.", 
                           trades[i].ticket, minutesSincePeak, CloseTimeMinLevel2);
               if (CloseTradeSafely(trades[i].ticket)) {
                  trades[i].isActive = false; activeTradeCount--;
               }
               continue;
            }
         }
      } else { 
         if(trades[i].isActive) { 
             Print("OrderSelect failed for active trade ticket: " + IntegerToString(trades[i].ticket) + " Error: " + IntegerToString(GetLastError()) + ". Marking inactive.");
             trades[i].isActive = false;
             activeTradeCount--; // Ensure count is decremented
         }
      }
   }
}

// MODULE 7: SWEEPS & VOLATILITY FILTERS
datetime lastSweepCheckTime = 0; // Renamed
double overnightVolatilityStartPrice = 0.0; 
double middayVolatilityStartPrice = 0.0;    

void HandleSweepsAndVolatility() {
   datetime now_server = TimeCurrent(); 
   if (lastSweepCheckTime != 0 && now_server - lastSweepCheckTime < 55) return; 
   lastSweepCheckTime = now_server;
   
   double currentMarketBid = SymbolInfoDouble(SymbolToTrade, SYMBOL_BID);
   if (!IsPriceValid(currentMarketBid)) {
      Print("Error: Invalid bid price in HandleSweepsAndVolatility: " + DoubleToString(currentMarketBid, Digits));
      return;
   }
      
   if (!IsTradingAllowed(now_server)) { // IsTradingAllowed uses GetUKTime()
      if(CountOpenTrades() > 0) { 
          Print("All trades closed: Outside main trading hours (HandleSweeps).");
          CloseAllTrades();
      }
      return; 
   }
   
   double currentPoint = MarketInfo(SymbolToTrade, MODE_POINT);
   if (currentPoint <= 0) {
      PrintFormat("CRITICAL Error in HandleSweepsAndVolatility: Invalid Point (%.5f)", currentPoint);
      return;
   }
   
   if (session1.isInitialized && session1Allowed) {
      double distS1OpenIdx = MathAbs(currentMarketBid - session1.openPrice) / (currentPoint * IDX);
      if (distS1OpenIdx >= ZoneCancelLevel) {
         PrintFormat("Sweep Close (Session 1): DistIdx %.1f >= %d. Closing all trades.", distS1OpenIdx, ZoneCancelLevel);
         CloseAllTrades();
         session1Allowed = false; 
         return; 
      }
   }
   if (session2.isInitialized && session2Allowed) {
      double distS2OpenIdx = MathAbs(currentMarketBid - session2.openPrice) / (currentPoint * IDX);
      if (distS2OpenIdx >= ZoneCancelLevel) {
         PrintFormat("Sweep Close (Session 2): DistIdx %.1f >= %d. Closing all trades.", distS2OpenIdx, ZoneCancelLevel);
         CloseAllTrades();
         session2Allowed = false; 
         return; 
      }
   }
      
   datetime ukNow = GetUKTime();
   int ukHour = TimeHour(ukNow);
   int ukMinute = TimeMinute(ukNow);

   if (ukHour == 17 && ukMinute == 16 && overnightVolatilityStartPrice == 0.0) { 
      overnightVolatilityStartPrice = currentMarketBid;
      Print("Overnight volatility start price captured (UK 17:16): " + DoubleToString(overnightVolatilityStartPrice, Digits));
   }
   if (ukHour == 12 && ukMinute == 31 && middayVolatilityStartPrice == 0.0) { 
      middayVolatilityStartPrice = currentMarketBid;
      Print("Midday volatility start price captured (UK 12:31): " + DoubleToString(middayVolatilityStartPrice, Digits));
   }

   datetime s1StartTimeUK = StringToTime(TimeToString(ukNow, TIME_DATE) + " " + Session1StartTime);
   if (ukHour == TimeHour(s1StartTimeUK) && ukMinute == TimeMinute(s1StartTimeUK)) {
      if (overnightVolatilityStartPrice != 0.0) {
         double overnightMoveIdx = MathAbs(currentMarketBid - overnightVolatilityStartPrice) / (currentPoint * IDX);
         PrintFormat("Overnight Volatility Check (UK %s): StartPrice %.*f, CurrentPrice %.*f, MoveIdxPts %.1f, Limit %.1f",
                     Session1StartTime, Digits, overnightVolatilityStartPrice, Digits, currentMarketBid, overnightMoveIdx, OvernightVolatilityLimit);
         if (overnightMoveIdx >= OvernightVolatilityLimit) {
            session1Allowed = false;
            Print("Session 1 trading DISALLOWED due to overnight volatility.");
         } else {
            session1Allowed = true; // Explicitly allow if check passes
         }
         overnightVolatilityStartPrice = 0.0; 
      } else {
         session1Allowed = true; // No prior overnight price, so allow
      }
   }
   datetime s2StartTimeUK = StringToTime(TimeToString(ukNow, TIME_DATE) + " " + Session2StartTime);
   if (ukHour == TimeHour(s2StartTimeUK) && ukMinute == TimeMinute(s2StartTimeUK)) {
      if (middayVolatilityStartPrice != 0.0) {
         double middayMoveIdx = MathAbs(currentMarketBid - middayVolatilityStartPrice) / (currentPoint * IDX);
         PrintFormat("Midday Volatility Check (UK %s): StartPrice %.*f, CurrentPrice %.*f, MoveIdxPts %.1f, Limit %.1f",
                     Session2StartTime, Digits, middayVolatilityStartPrice, Digits, currentMarketBid, middayMoveIdx, MiddayVolatilityLimit);
         if (middayMoveIdx >= MiddayVolatilityLimit) {
            session2Allowed = false;
            Print("Session 2 trading DISALLOWED due to midday volatility.");
         } else {
            session2Allowed = true; // Explicitly allow
         }
         middayVolatilityStartPrice = 0.0; 
      } else {
          session2Allowed = true; // No prior midday price, so allow
      }
   }
}
void CloseAllTrades() {
   int totalOpenOrders = OrdersTotal(); // Check how many orders are open in general
   Print("CloseAllTrades called. OrdersTotal(): ", totalOpenOrders, ", EA tracked activeTradeCount: ", activeTradeCount);
   if(totalOpenOrders == 0 && activeTradeCount == 0) return;

   for (int i = OrdersTotal() - 1; i >= 0; i--) { 
      if (OrderSelect(i, SELECT_BY_POS)) { // Select by position
          if(OrderSymbol() == SymbolToTrade && OrderMagicNumber() == 0) { // Check symbol and magic
             int ticketToClose = OrderTicket();
             Print("Attempting to close trade ticket: " + IntegerToString(ticketToClose));
             if (CloseTradeSafely(ticketToClose)) {
                // Update internal tracking
                for(int t=0; t < ArraySize(trades); t++) {
                   if(trades[t].isActive && trades[t].ticket == ticketToClose) {
                      trades[t].isActive = false;
                      // activeTradeCount--; // Decrement is handled in CountOpenTrades or after successful close
                      break;
                   }
                }
             }
          }
      }
   }
   activeTradeCount = CountOpenTrades(); // Recalculate after attempting closures
   if(activeTradeCount == 0) Print("All trades for this EA should now be closed.");
   else Print("Warning: CloseAllTrades finished, but activeTradeCount is still: ", activeTradeCount);
}

// MODULE 8: MONEY MANAGEMENT
double CalculateLotSize() {
   double equity = AccountEquity();
   double point = MarketInfo(SymbolToTrade, MODE_POINT);
   if(point <= 0) { Print("CalculateLotSize: Invalid Point value."); return 0.0;}

   // Assuming BetSizingFactor relates to account currency per index point risk,
   // or similar. This part of logic needs to be very clear.
   // If BetSizingFactor is "equity per lot", then lot = equity / BetSizingFactor
   // If it's e.g. $800 per DAX point, and stop is 40 DAX points:
   // Risk per trade = (equity / BetSizingFactor_relative_to_equity) * StopLossPoints(idx)
   // LotSize = (Equity * RiskPercent_per_trade) / (StopLoss_In_Currency)
   // StopLoss_In_Currency = StopLossPoints(idx) * TickValue_per_idx_point
   // TickValue_per_idx_point = MarketInfo(Symbol(), MODE_TICKVALUE) / (MarketInfo(Symbol(), MODE_TICKSIZE) / point) / IDX
   // This is complex. For now, using the simpler formula:
   double lot = NormalizeDouble(equity / BetSizingFactor, 2); 

   if (lot < MarketInfo(SymbolToTrade, MODE_MINLOT)) {
       lot = MarketInfo(SymbolToTrade, MODE_MINLOT); // Attempt to use broker's min lot
       Print("Calculated lot was too small, attempting to use broker MINLOT: ", DoubleToString(lot,2));
   }
    if (lot < MinTradeSize) { // Now check against EA's input MinTradeSize
      Print("Lot size " + DoubleToString(lot, 2) + " is less than EA MinTradeSize " + DoubleToString(MinTradeSize,2) + ". Cannot trade.");
      return 0.0; 
   }
   
   double lotStep = MarketInfo(SymbolToTrade, MODE_LOTSTEP);
   if (lotStep > 0) {
      lot = NormalizeDouble(MathFloor(lot / lotStep) * lotStep, 2);
   }

   if (lot < MinTradeSize) { 
        Print("Lot size after step normalization " + DoubleToString(lot, 2) + " is less than EA MinTradeSize " + DoubleToString(MinTradeSize,2));
        return 0.0;
   }
   if (lot > MarketInfo(SymbolToTrade, MODE_MAXLOT)) {
       lot = MarketInfo(SymbolToTrade, MODE_MAXLOT);
       Print("Calculated lot exceeded MAXLOT, using MAXLOT: ", DoubleToString(lot,2));
   }

   Print("Lot size calculated: " + DoubleToString(lot, 2));
   return lot;
}
bool CanTrade() {
   double lotToTrade = CalculateLotSize(); 
   if (lotToTrade < MinTradeSize) { 
      // Print("Cannot trade: Calculated lot size " + DoubleToString(lotToTrade,2) + " is less than EA minimum " + DoubleToString(MinTradeSize,2) + ".");
      return false;
   }
      
   if (!AccountFreeMarginCheck(SymbolToTrade, OP_BUY, lotToTrade)) { 
      PrintFormat("Not enough free margin for lot %.2f. Free Margin: %.2f", lotToTrade, AccountFreeMargin());
      return false;
   }
   return true;
}

// MODULE 9: UTILITY FUNCTIONS
bool IsTradingAllowed(datetime serverNow) { 
   datetime ukNow = GetUKTime(); 
   int ukHour = TimeHour(ukNow);
   int ukMinute = TimeMinute(ukNow);
   
   if (ukHour > 17 || (ukHour == 17 && ukMinute >= 16)) return false; // After 17:16 UK
   if (ukHour < 8) return false; // Before 08:00 UK
   if ((ukHour == 12 && ukMinute >= 31) || (ukHour == 13) || (ukHour == 14 && ukMinute < 30)) return false; // Midday break
      
   return true;
}
int CountOpenTrades() {
   int count = 0;
   for(int i=0; i < ArraySize(trades); i++) {
      if(trades[i].isActive) {
         if(OrderSelect(trades[i].ticket, SELECT_Bindex