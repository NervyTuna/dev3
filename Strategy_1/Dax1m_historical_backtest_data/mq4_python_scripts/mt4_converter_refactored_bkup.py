//+------------------------------------------------------------------+
//|            DAX30 Strategy EA - Demo Account Version              |
//| Updated for IG.com.au GER30 with UK time handling                |d fill missing bars,
//| Fixed session tracking, trade memory, and trade triggering       |nments.
//+------------------------------------------------------------------+
Usage:
#property copyright "Your Name"ored.py INPUT_CSV YEARS [--output_base_dir DIR]
#property link      "https://www.example.com"t C]
#property version   "1.04" // Updated version for fixes-verbose]
#property strictl_suffix SUFFIX] [--combine_years]
        python mt4_converter_refactored.py INPUT_CSV YEARS [--output_base_dir DIR] [--digits D] [--spread S] [--copyright C] [--input_timezone TZ] [--no_remove_weekends] [--verbose] [--symbol_suffix SUFFIX] [--combine_years]
//--- MODULE 1: INPUTS & GLOBAL SETTINGSpy dax-1m.csv 2024 --combine_years
string SymbolToTrade; // Set dynamically in OnInit()v 2024 --combine_years --symbol_suffix _OFFLINE
input ENUM_TIMEFRAMES ChartTimeframe = PERIOD_M1; // Working timeframe (M1 for GER30)
input string Session1StartTime    = "08:00";     // Session 1 start UK time
input string Session1EndTime      = "12:30";     // Session 1 end UK time
input string Session2StartTime    = "14:30";     // Session 2 start UK time
input string Session2EndTime      = "17:16";     // Session 2 end UK time
input int ZoneEntryLevel1         = 45;          // Entry level 1 (points)
input int ZoneEntryLevel2         = 70;          // Entry level 2 (points)
input int ZoneEntryLevel3         = 100;         // Entry level 3 (points)
input int ZoneEntryLevel4         = 130;         // Entry level 4 (points)
input int ZoneCancelLevel         = 179;         // Cancel level (points)
input double OvernightVolatilityLimit = 200;     // Overnight volatility limit
input double MiddayVolatilityLimit    = 150;     // Midday volatility limit
input double RetractTolerance     = 9.0;         // Tolerance for entry (±points)
input double RetractMin_1         = 15.0;        // Retraction level 1 min
input double RetractMax_1         = 29.9;        // Retraction level 1 max
input double RetractAdjustBeyondHL = 18.0;       // Retraction level 1 adjustment
input double RetractMin_2         = 30.0;        // Retraction level 2 min
input double RetractMax_2         = 35.9;        // Retraction level 2 max
input double RetractMin_3         = 36.0;        // Retraction level 3 min
input double RetractMax_3         = 45.9;        // Retraction level 3 max
input double RetractCancelThreshold = 46.0;      // Retraction cancel threshold
input int AdverseMove1            = 15;          // Adverse move for breakeven
input int CloseTimeMinLevel1      = 16;          // Close time for level 1 (min)
input int CloseTimeMinLevel2      = 31;          // Close time for levels 2+ (min)
input double BetSizingFactor      = 800.0;       // Bet sizing factor
input double MinTradeSize         = 0.1;         // Minimum trade size
input int SlippagePoints          = 3;           // Slippage (points)
input string TradeComment         = "DAX30Strategy"; // Trade comment
input int StopLossPoints          = 40;          // Mandatory stop loss (points)
    "d"   # close
// Spread schedule for GER30 (DAX) on IG (CET, converted to UK time)
struct SpreadTime {
   string startTime;ume
   string endTime;
   double spreadPoints;
};f setup_logging(verbose: bool):
    level = logging.DEBUG if verbose else logging.INFO
SpreadTime spreadSchedule[] = {"%(asctime)s %(levelname)s: %(message)s", level=level)
   {"00:15", "07:00", 4.0},  // 01:15–08:00 CET
   {"07:00", "08:00", 2.0},  // 08:00–09:00 CET
   {"08:00", "16:30", 1.2},  // 09:00–17:30 CETn="Convert DAX CSV to MT4 HST/CSV")
   {"16:30", "21:00", 2.0},  // 17:30–22:00 CET to input DAX CSV file")
   {"21:00", "00:15", 5.0}   // 22:00–01:15 CETrange (e.g., '2024' or '2000-2025')")
};  parser.add_argument("--output_base_dir", default="mt4_output_data",
                        help="Directory for output files")
// Trade tracking struct"--digits", type=int, default=1,
struct TradeData {      help="Decimal places for prices (default: 1)")
   int ticket;_argument("--spread", type=int, default=0,
   double entryPrice;   help="Spread in points for HST records")
   double adjustedEntryPrice;pyright", default="(C) Processed by script",
   double peakHigh;     help="Copyright string for HST header")
   double peakLow;ument("--input_timezone", default="Europe/Berlin",
   datetime peakTime;   help="Timezone of input data (Olson name)")
   int sessionNum;ument("--no_remove_weekends", action="store_true",
   bool isActive;       dest="keep_weekends",
};                      help="Keep weekend data (default: remove Saturdays/Sundays)")
    parser.add_argument("--verbose", action="store_true",
TradeData trades[10];   help="Enable debug logging")
int activeTradeCount = 0;--symbol_suffix", default="",
                        help="Suffix for symbol names (e.g., '_OFFLINE')")
// Session tracking variablesmbine_years", action="store_true",
int Session1TradeCount = 0;p="Generate a single HST/CSV file for all specified years")
int Session2TradeCount = 0;s()
bool session1Allowed = true;
bool session2Allowed = true;r):
    if "-" in year_arg:
//+------------------------------------------------------------------+
//|              MODULE 2: TIME MANAGEMENT (UK TIME)                 |
//+------------------------------------------------------------------+

bool IsUKDST(datetime time) {str, tz_local: pytz.timezone):
   MqlDateTime dt;Reading CSV: %s", path)
   TimeToStruct(time, dt);, 'Open', 'High', 'Low', 'Close', 'Volume']
   int year = dt.year;ath, sep=';', header=None, names=cols, dayfirst=True, dtype=str)
   datetime marchLastSunday = 0;coerce errors to NaN
   for (int dMarch = 31; dMarch >= 25; dMarch--) {olume']:
      datetime tempMarch = StringToTime(IntegerToString(year) + ".03." + IntegerToString(dMarch) + " 01:00");
      if (TimeDayOfWeek(tempMarch) == 0) {erce')
         marchLastSunday = tempMarch;
         break;e'] = pd.to_datetime(df['Date'] + ' ' + df['Time'],
      }                             format='%d/%m/%Y %H:%M:%S',
   }                                errors='coerce')
   datetime octoberLastSunday = 0;'Open', 'High', 'Low', 'Close', 'Volume'],
   for (int dOctober = 31; dOctober >= 25; dOctober--) {
      datetime tempOctober = StringToTime(IntegerToString(year) + ".10." + IntegerToString(dOctober) + " 01:00");
      if (TimeDayOfWeek(tempOctober) == 0) {mbda x: tz_local.localize(x))
         octoberLastSunday = tempOctober;onvert(pytz.UTC)
         break;x('dt_utc', inplace=True)
      }= df[['Open', 'High', 'Low', 'Close', 'Volume']]
   }df = df.sort_index()
   return (time >= marchLastSunday && time < octoberLastSunday);dex.min(), df.index.max())
}   return df

int GetServerToUKOffset() {: pd.DataFrame, keep_weekends: bool):
   datetime serverTime = TimeCurrent();ex.max()
   MqlDateTime serverStruct;(start, end, freq='min', tz=pytz.UTC)
   TimeToStruct(serverTime, serverStruct);
    missing = df['Open'].isna().sum()
   bool isUKDST = IsUKDST(serverTime);
   int ukOffsetFromGMT = isUKDST ? 1 : 0;  // BST = GMT+1, GMT = 0
        df[['Open','High','Low','Close']] = df[['Open','High','Low','Close']].ffill()
   int serverGMTOffset = TimeGMTOffset() / 3600;  // Convert seconds to hours
    if not keep_weekends:
   int offsetHours = serverGMTOffset - ukOffsetFromGMT;
   return offsetHours;"Removed weekends, remaining entries: %d", len(df))
}   return df

datetime GetUKTime() {DataFrame, filepath: str, symbol: str,
   int offsetHours = GetServerToUKOffset();pyright_str: str, spread: int):
   return TimeCurrent() - offsetHours * 3600;ist_ok=True)
}   with open(filepath, 'wb') as f:
        # Header
datetime CETTimeToUKTime(string cetTime) {, errors='replace')[:11].ljust(12, b'\0')
   datetime cetDateTime = StringToTime(TimeToString(TimeCurrent(), TIME_DATE) + " " + cetTime);
   bool isCETDST = IsUKDST(cetDateTime); // CET DST (CEST) aligns with UK DST
   bool isUKDST = IsUKDST(cetDateTime);_FORMAT, HST_VERSION, copy_bytes,
   int offsetHours;          sym_bytes, period, digits,
                             timestamp, 0, b'\0'*52)
   if (isUKDST && isCETDST) offsetHours = 0;  // Both in DST (BST and CEST), no offset
   else if (!isUKDST && !isCETDST) offsetHours = 1;  // Neither in DST (GMT and CET), 1-hour offset
   else if (isUKDST && !isCETDST) offsetHours = -1;  // UK in BST, CET not in DST (rare)
   else offsetHours = 1;  // UK not in DST, CET in CESTstamp()),
                               float(row['Open']), float(row['High']),
   return cetDateTime - offsetHours * 3600;ow']), float(row['Close']),
}                              int(row['Volume']), spread, 0)
            f.write(data)
double GetSpreadForTime(datetime now) {ath)
   datetime ukNow = GetUKTime();
   string currentTime = TimeToString(ukNow, TIME_MINUTES);t):
   for (int i = 0; i < ArraySize(spreadSchedule); i++) {)
      datetime startTime = CETTimeToUKTime(spreadSchedule[i].startTime);
      datetime endTime = CETTimeToUKTime(spreadSchedule[i].endTime);
      if (ukNow >= startTime && ukNow <= endTime) {
         Print("Time: " + currentTime + " Spread Set to: " + DoubleToString(spreadSchedule[i].spreadPoints, 1));
         return spreadSchedule[i].spreadPoints;:.{digits}f}"),
      } '<LOW>': df['Low'].map(lambda x: f"{x:.{digits}f}"),
   }    '<CLOSE>': df['Close'].map(lambda x: f"{x:.{digits}f}"),
   Print("No spread schedule match, defaulting to 5.0");
   return 5.0;
}   out.to_csv(filepath, index=False)
    logging.info("Wrote CSV: %s", filepath)
string globalVarPrefix; // Declare prefix for global variables
def main():
string SanitizeSymbol(string symbol) {
   string sanitized = symbol;e)
   StringReplace(sanitized, "(", "_");
   StringReplace(sanitized, ")", "_");input_timezone)
   StringReplace(sanitized, "£", "GBP");
   return sanitized;r("Invalid timezone '%s': %s", args.input_timezone, e)
}       return
    df = read_and_clean_csv(args.input_csv, tz_local)
void AdjustPricesBySpread() {
   double spreadPoints = GetSpreadForTime(TimeCurrent());check the input file.")
   double bid = SymbolInfoDouble(SymbolToTrade, SYMBOL_BID);
   double ask = SymbolInfoDouble(SymbolToTrade, SYMBOL_ASK);
   if (bid <= 0 || ask <= 0) {ears)
      Print("Error: Invalid bid/ask prices. Bid: " + DoubleToString(bid, 2) + ", Ask: " + DoubleToString(ask, 2));
      return; [
   }    {"env": "IG-DEMO", "file_sym": f"GER30(£){suffix}", "hdr_sym": f"GER30(£){suffix}"},
   double adjustedBid = bid - spreadPoints * Point;ix}",    "hdr_sym": f"GER30{suffix}"}
   double adjustedAsk = ask + spreadPoints * Point;
   Print("Adjusted Bid: " + DoubleToString(adjustedBid, 2) + " Adjusted Ask: " + DoubleToString(adjustedAsk, 2) + " Spread: " + DoubleToString(spreadPoints, 1));
   GlobalVariableSet(globalVarPrefix + "_AdjBid", adjustedBid);
   GlobalVariableSet(globalVarPrefix + "_AdjAsk", adjustedAsk);
   GlobalVariableSet(globalVarPrefix + "_Spread", spreadPoints);", years)
}           return
        for cfg in configs:
//+------------------------------------------------------------------+
//|               MODULE 3: SESSION HANDLING                         |
//+------------------------------------------------------------------+
            create_hst(df_combined, os.path.join(base, hst_fn),
struct SessionData {   cfg['hdr_sym'], 1, args.digits,
   datetime startTime; args.copyright, args.spread)
   datetime endTime;sv(df_combined, os.path.join(base, csv_fn), args.digits)
   double openPrice;
   double highPrice;years:
   double lowPrice;f[df.index.year == year]
   bool isInitialized;ty:
   datetime peakTime;ng.warning("No data for year %d", year)
};              continue
            for cfg in configs:
SessionData session1;= os.path.join(args.output_base_dir, cfg["env"])
SessionData session2;n = f"{cfg['file_sym']}1_{year}.hst"
datetime lastSessionInitDate = 0;file_sym']}1_{year}.csv"
                create_hst(sub, os.path.join(base, hst_fn),
void InitializeSessionData() {['hdr_sym'], 1, args.digits,
   datetime now = TimeCurrent();copyright, args.spread)
   MqlDateTime nowStruct;v(sub, os.path.join(base, csv_fn), args.digits)
   TimeToStruct(now, nowStruct);
   string currentDate = StringFormat("%04d.%02d.%02d", nowStruct.year, nowStruct.mon, nowStruct.day);
    main()   session1.startTime = StringToTime(currentDate + " " + Session1StartTime);   session1.endTime   = StringToTime(currentDate + " " + Session1EndTime);   session2.startTime = StringToTime(currentDate + " " + Session2StartTime);   session2.endTime   = StringToTime(currentDate + " " + Session2EndTime);      if (session1.startTime > session1.endTime) session1.endTime += 86400;   if (session2.startTime > session2.endTime) session2.endTime += 86400;      session1.isInitialized = false;   session2.isInitialized = false;      Print("Session 1 - Start: " + TimeToString(session1.startTime, TIME_MINUTES) +          " End: " + TimeToString(session1.endTime, TIME_MINUTES));   Print("Session 2 - Start: " + TimeToString(session2.startTime, TIME_MINUTES) +          " End: " + TimeToString(session2.endTime, TIME_MINUTES));}void ResetSessionData(SessionData &session, double bidPrice) {   if (bidPrice <= 0) {      Print("Error: Invalid bid price for session initialization: " + DoubleToString(bidPrice, 2));      return;   }   session.openPrice = bidPrice;   session.highPrice = bidPrice;   session.lowPrice = bidPrice;   session.isInitialized = true;   session.peakTime = TimeCurrent();   Print("Session Initialized - Open Price: " + DoubleToString(session.openPrice, 2));}void CheckSessionInitialization() {   datetime now = TimeCurrent();   MqlDateTime nowStruct;   TimeToStruct(now, nowStruct);   datetime currentDate = StringToTime(StringFormat("%04d.%02d.%02d", nowStruct.year, nowStruct.mon, nowStruct.day));      if (lastSessionInitDate != currentDate) {      InitializeSessionData();      lastSessionInitDate = currentDate;      Session1TradeCount = 0;      Session2TradeCount = 0;      session1Allowed = true;      session2Allowed = true;      Print("Daily session initialization for " + TimeToString(currentDate, TIME_DATE));   }      double bid = SymbolInfoDouble(SymbolToTrade, SYMBOL_BID);   if (bid <= 0) {      Print("Error: Invalid bid price in CheckSessionInitialization: " + DoubleToString(bid, 2));      return;   }      if (now >= session1.startTime && now < session1.endTime) {      if (!session1.isInitialized) {         ResetSessionData(session1, bid);         Session1TradeCount = 0;         Print("Session 1 Initialized. Open Price: " + DoubleToString(session1.openPrice, 2));      }   } else if (session1.isInitialized) {      session1.isInitialized = false;      Print("Session 1 Reset - Outside Time Window");   }      if (now >= session2.startTime && now < session2.endTime) {      if (!session2.isInitialized) {         ResetSessionData(session2, bid);         Session2TradeCount = 0;         Print("Session 2 Initialized. Open Price: " + DoubleToString(session2.openPrice, 2));      }   } else if (session2.isInitialized) {      session2.isInitialized = false;      Print("Session 2 Reset - Outside Time Window");   }}void UpdateSessionHighLow(SessionData &s, double price) {   if (!s.isInitialized || price <= 0) return;   if (price > s.highPrice) {      s.highPrice = price;      s.peakTime = TimeCurrent();   }   if (price < s.lowPrice) {      s.lowPrice = price;      s.peakTime = TimeCurrent();   }}//+------------------------------------------------------------------+//|               MODULE 4: ACTIVE TRADING ZONES                     |//+------------------------------------------------------------------+struct ActiveZone {   string startTime;   string endTime;   int requiredDistance;};ActiveZone session1Zones[] = {   {"08:16", "09:05", 45},   {"09:30", "09:45", 45},   {"10:15", "10:45", 70},   {"10:45", "11:45", 45},   {"11:45", "12:31", 45}};ActiveZone session2Zones[] = {   {"14:46", "15:06", 45},   {"15:15", "15:45", 70},   {"15:45", "16:48", 45}};bool IsWithinTimeWindow(string startT, string endT) {   datetime now = TimeCurrent();   MqlDateTime nowStruct;   TimeToStruct(now, nowStruct);   string currentDate = StringFormat("%04d.%02d.%02d", nowStruct.year, nowStruct.mon, nowStruct.day);      datetime startTime = StringToTime(currentDate + " " + startT);   datetime endTime = StringToTime(currentDate + " " + endT);   if (startTime > endTime) endTime += 86400;   return (now >= startTime && now <= endTime);}bool IsInActiveTradingZone(SessionData &session, int sessionNum, double currentPrice) {   if (!session.isInitialized || currentPrice <= 0) {      Print("Session not initialized or invalid price - cannot check trading zones");      return false;   }      double adjustedBid = GlobalVariableGet(globalVarPrefix + "_AdjBid");   if (adjustedBid <= 0) {      Print("Error: Invalid adjusted bid price: " + DoubleToString(adjustedBid, 2));      return false;   }   double spreadPoints = GlobalVariableGet(globalVarPrefix + "_Spread");   double distancePoints = MathAbs(adjustedBid - session.openPrice) / Point;      Print("IsInActiveTradingZone - Session: " + IntegerToString(sessionNum) +          ", Time: " + TimeToString(TimeCurrent(), TIME_MINUTES) +         ", Adjusted Bid: " + DoubleToString(adjustedBid, 2) +          ", Session Open: " + DoubleToString(session.openPrice, 2) +          ", Distance: " + DoubleToString(distancePoints, 1) +          ", Spread: " + DoubleToString(spreadPoints, 1));      if (distancePoints < ZoneEntryLevel1) {      Print("Distance too small: " + DoubleToString(distancePoints, 1) + " < " + IntegerToString(ZoneEntryLevel1));      return false;   }      ActiveZone zones[];   ArrayResize(zones, sessionNum == 1 ? ArraySize(session1Zones) : ArraySize(session2Zones));   for (int i = 0; i < ArraySize(zones); i++) {      zones[i] = sessionNum == 1 ? session1Zones[i] : session2Zones[i];      bool inWindow = IsWithinTimeWindow(zones[i].startTime, zones[i].endTime);      double targetDistance = zones[i].requiredDistance;      bool meetsDistance = distancePoints >= targetDistance;            Print("Zone " + IntegerToString(i) + ": Start: " + zones[i].startTime +             ", End: " + zones[i].endTime +             ", In Window: " + (inWindow ? "true" : "false") +             ", Target Distance: " + DoubleToString(targetDistance, 1) +             ", Actual Distance: " + DoubleToString(distancePoints, 1));            if (inWindow && meetsDistance) {         Print("Trade Trigger Possible - Zone " + IntegerToString(i) + ", Distance: " + DoubleToString(distancePoints, 1));         return true;      }   }   Print("No trade zone triggered.");   return false;}//+------------------------------------------------------------------+//|              MODULE 5: RETRACTION & TOLERANCE                    |//+------------------------------------------------------------------+bool IsWithinTolerance(double price, double targetPrice) {   double spreadPoints = GlobalVariableGet(globalVarPrefix + "_Spread");   return MathAbs(price - targetPrice) <= (RetractTolerance + spreadPoints);}double CalculateRetraction(SessionData &session, double currentPrice) {   if (currentPrice <= 0) {      Print("Error: Invalid current price in CalculateRetraction: " + DoubleToString(currentPrice, 2));      return 0;   }   double spreadPoints = GlobalVariableGet(globalVarPrefix + "_Spread");   double raw = MathMax(session.highPrice - currentPrice, currentPrice - session.lowPrice);   double retraction = raw / Point;   Print("Retraction Calc - High: " + DoubleToString(session.highPrice, 2) +          ", Low: " + DoubleToString(session.lowPrice, 2) +          ", Current Price: " + DoubleToString(currentPrice, 2) +          ", Spread: " + DoubleToString(spreadPoints, 1) +          ", Retraction: " + DoubleToString(retraction, 1));   return retraction;}int GetRetractionLevel(double retraction) {   if (retraction >= RetractMin_1 && retraction <= RetractMax_1)      return 1;   else if (retraction >= RetractMin_2 && retraction <= RetractMax_2)      return 2;   else if (retraction >= RetractMin_3 && retraction <= RetractMax_3)      return 3;   else if (retraction >= RetractCancelThreshold)      return -1;   return 0;}int AdjustEntryLevel(int originalLevel, int retractionLevel) {   if (retractionLevel == 1)      return originalLevel;   else if (retractionLevel == 2)      return originalLevel + 1;   else if (retractionLevel == 3)      return originalLevel + 2;   if (retraction >= (RetractCancelThreshold - 10) && CountOpenTrades() == 0) {      return -1;   return originalLevel;}double CalculateNewEntryPrice(SessionData &session, double sessionOpen, int adjustedLevel, bool isBuy, int retractionLevel) {   int distancePoints = 0;   if (adjustedLevel == 1)      distancePoints = ZoneEntryLevel1;   else if (adjustedLevel == 2)      distancePoints = ZoneEntryLevel2;   else if (adjustedLevel == 3)      distancePoints = ZoneEntryLevel3;   else if (adjustedLevel == 4)      distancePoints = ZoneEntryLevel4;   double spreadPoints = GlobalVariableGet(globalVarPrefix + "_Spread");   double adjust = (retractionLevel == 1) ? RetractAdjustBeyondHL : 0;   if (isBuy)      return sessionOpen - (distancePoints + adjust + spreadPoints) * Point;   else      return sessionOpen + (distancePoints + adjust + spreadPoints) * Point;}//+------------------------------------------------------------------+//|              MODULE 6: TRADE ENTRY & EXIT LOGIC                  |//+------------------------------------------------------------------+void PlaceTrade(string type, double entryPrice, double lotSize, int sessionNum) {   double adjustedBid = GlobalVariableGet(globalVarPrefix + "_AdjBid");   double adjustedAsk = GlobalVariableGet(globalVarPrefix + "_AdjAsk");   if (adjustedBid <= 0 || adjustedAsk <= 0) {      Print("Error: Invalid adjusted prices. Bid: " + DoubleToString(adjustedBid, 2) + ", Ask: " + DoubleToString(adjustedAsk, 2));      return;   }   double spreadPoints = GlobalVariableGet(globalVarPrefix + "_Spread");   int typeOrder = (type == "BUY") ? OP_BUY : OP_SELL;   double price = (type == "BUY") ? adjustedAsk : adjustedBid;   double stopLossPrice = (type == "BUY")                          ? entryPrice - StopLossPoints * Point                          : entryPrice + StopLossPoints * Point;   Print("Trade Attempt - Type: " + type +          ", Entry Price: " + DoubleToString(entryPrice, 2) +          ", Stop Loss: " + DoubleToString(stopLossPrice, 2) +          ", Spread: " + DoubleToString(spreadPoints, 1));   int ticket = -1;   for (int attempt = 0; attempt < 5; attempt++) {      ticket = OrderSend(SymbolToTrade, typeOrder, lotSize, price, SlippagePoints,                          stopLossPrice, 0, TradeComment, 0, 0, clrBlue);      if (ticket >= 0) {         if (OrderSelect(ticket, SELECT_BY_TICKET)) {            double actualSL = OrderStopLoss();            if (MathAbs(actualSL - stopLossPrice) <= Point) {               Print("Trade opened successfully. Ticket: " + IntegerToString(ticket));               for (int i = 0; i < ArraySize(trades); i++) {                  if (!trades[i].isActive) {                     trades[i].ticket = ticket;                     trades[i].entryPrice = OrderOpenPrice();                     trades[i].adjustedEntryPrice = entryPrice;                     trades[i].peakHigh = price;                     trades[i].peakLow = price;                     trades[i].peakTime = TimeCurrent();                     trades[i].sessionNum = sessionNum;                     trades[i].isActive = true;                     activeTradeCount++;                     Print("Trade Stored - Ticket: " + IntegerToString(ticket));                     break;                  }               }               return;            } else {               Print("Stop loss not set correctly. Closing trade.");               bool result = OrderClose(ticket, lotSize, price, SlippagePoints, clrRed);               if (!result) {                  Print("Failed to close order: " + IntegerToString(GetLastError()));               }            }         }      }      Print("Trade attempt " + IntegerToString(attempt + 1) + " failed. Error: " + IntegerToString(GetLastError()));      Sleep(500);      RefreshRates();      price = (type == "BUY") ? SymbolInfoDouble(SymbolToTrade, SYMBOL_ASK)                               : SymbolInfoDouble(SymbolToTrade, SYMBOL_BID);   }   Print("Failed to open trade after 5 attempts.");}void CheckAndPlaceTrade(SessionData &session, double currentPrice, int sessionNum) {   if (currentPrice <= 0) {      Print("Error: Invalid current price in CheckAndPlaceTrade: " + DoubleToString(currentPrice, 2));      return;   }   Print("CheckAndPlaceTrade - Session: " + IntegerToString(sessionNum) +          ", Time: " + TimeToString(TimeCurrent(), TIME_MINUTES) +         ", Current Price: " + DoubleToString(currentPrice, 2));      if ((sessionNum == 1 && !session1Allowed) || (sessionNum == 2 && !session2Allowed)) {      Print("Trade blocked: Session cancelled due to volatility.");      return;   }   if ((sessionNum == 1 && Session1TradeCount >= 1) ||        (sessionNum == 2 && Session2TradeCount >= 1)) {      Print("Trade blocked: One trade per session limit reached.");      return;   }      double adjustedBid = GlobalVariableGet(globalVarPrefix + "_AdjBid");   if (adjustedBid <= 0) {      Print("Error: Invalid adjusted bid price in CheckAndPlaceTrade: " + DoubleToString(adjustedBid, 2));      return;   }   double distancePoints = MathAbs(adjustedBid - session.openPrice) / Point;      if (distancePoints < ZoneEntryLevel1) {      Print("Distance too small: " + DoubleToString(distancePoints, 1));      return;   }      int entryLevel = 0;   if (distancePoints >= ZoneEntryLevel1) entryLevel = 1;   if (distancePoints >= ZoneEntryLevel2) entryLevel = 2;   if (distancePoints >= ZoneEntryLevel3) entryLevel = 3;   if (distancePoints >= ZoneEntryLevel4) entryLevel = 4;      Print("Entry Level: " + IntegerToString(entryLevel));   if (entryLevel == 0) return;      double retraction = CalculateRetraction(session, currentPrice);   int retractionLevel = GetRetractionLevel(retraction);   Print("Retraction: " + DoubleToString(retraction, 1) + ", Retraction Level: " + IntegerToString(retractionLevel));      if (retractionLevel == -1 && CountOpenTrades() == 0) {      if (sessionNum == 1) session1Allowed = false;      else session2Allowed = false;      Print("Session " + IntegerToString(sessionNum) + " cancelled due to retraction.");      return;   }      int adjustedLevel = AdjustEntryLevel(entryLevel, retractionLevel);   // Full recursion to resolve multiple tolerance breaches   while(true) {      int nextLevel = AdjustEntryLevel(adjustedLevel, retractionLevel);      if (nextLevel == -1) return;      if (nextLevel == adjustedLevel) break;      adjustedLevel = nextLevel;   }   if (adjustedLevel == -1) return;      bool isBuy = (adjustedBid < session.openPrice);   double entryPrice = CalculateNewEntryPrice(session, session.openPrice, adjustedLevel, isBuy, retractionLevel);   Print("Trade Setup - Is Buy: " + (isBuy ? "true" : "false") + ", Entry Price: " + DoubleToString(entryPrice, 2));      if (!IsPriceValid(entryPrice)) return;      double lotSize = CalculateLotSize();   if (!CanTrade()) return;      string tradeType = isBuy ? "BUY" : "SELL";   PlaceTrade(tradeType, entryPrice, lotSize, sessionNum);   if (sessionNum == 1) Session1TradeCount++;   else Session2TradeCount++;}void CheckTradeClosure() {   datetime now = TimeCurrent();   bool isFirstZone = IsWithinTimeWindow("08:16", "09:05") &&                       now <= StringToTime(TimeToString(now, TIME_DATE) + " 09:31");      for (int i = 0; i < ArraySize(trades); i++) {      if (!trades[i].isActive) continue;            if (OrderSelect(trades[i].ticket, SELECT_BY_TICKET) && OrderSymbol() == SymbolToTrade) {         double openPrice = OrderOpenPrice();         double currentPrice = (OrderType() == OP_BUY) ? SymbolInfoDouble(SymbolToTrade, SYMBOL_BID)                                                        : SymbolInfoDouble(SymbolToTrade, SYMBOL_ASK);         if (currentPrice <= 0) continue;         double sessionOpen = trades[i].sessionNum == 1 ? session1.openPrice : session2.openPrice;         double distancePoints = MathAbs(openPrice - sessionOpen) / Point;         double adverseMove = (OrderType() == OP_BUY) ? (openPrice - currentPrice)                                                       : (currentPrice - openPrice);         int minutesSincePeak = (int)((now - trades[i].peakTime) / 60);                  if (isFirstZone && now >= StringToTime(TimeToString(now, TIME_DATE) + " 09:31")) {            bool result = CloseTradeSafely(trades[i].ticket);            if (result) {               trades[i].isActive = false;               activeTradeCount--;               Print("Trade closed: Hard close at 09:31.");            }            continue;         }                  if (distancePoints >= 45 && distancePoints < 70) {            if (adverseMove >= AdverseMove1 * Point &&                 MathAbs(currentPrice - openPrice) <= Point) {               bool result = CloseTradeSafely(trades[i].ticket);               if (result) {                  trades[i].isActive = false;                  activeTradeCount--;                  Print("Trade closed at breakeven: 15-point adverse move.");               }            } else if (minutesSincePeak >= CloseTimeMinLevel1) {               bool result = CloseTradeSafely(trades[i].ticket);               if (result) {                  trades[i].isActive = false;                  activeTradeCount--;                  Print("Trade closed: 16th minute from peak.");               }            }         } else if (distancePoints >= 70 && distancePoints <= 159.9) {            if (minutesSincePeak >= CloseTimeMinLevel2) {               bool result = CloseTradeSafely(trades[i].ticket);               if (result) {                  trades[i].isActive = false;                  activeTradeCount--;                  Print("Trade closed: 31st minute from peak.");               }            }         }      } else {         trades[i].isActive = false;         activeTradeCount--;      }   }}//+------------------------------------------------------------------+//|             MODULE 7: SWEEPS & VOLATILITY FILTERS                |//+------------------------------------------------------------------+datetime lastSweepCheck = 0;double overnightStartPrice = 0.0;double middayStartPrice = 0.0;void HandleSweepsAndVolatility() {   datetime now = TimeCurrent();   if (now - lastSweepCheck < 60) return;   lastSweepCheck = now;      double bidPrice = SymbolInfoDouble(SymbolToTrade, SYMBOL_BID);   if (bidPrice <= 0) {      Print("Error: Invalid bid price in HandleSweepsAndVolatility: " + DoubleToString(bidPrice, 2));      return;   }   if (!IsTradingAllowed(now)) {      CloseAllTrades();      Print("Trades closed: No trading allowed.");   }      double distanceFromSession1Open = session1.isInitialized ? MathAbs(bidPrice - session1.openPrice) / Point : 0;   double distanceFromSession2Open = session2.isInitialized ? MathAbs(bidPrice - session2.openPrice) / Point : 0;   if ((distanceFromSession1Open >= ZoneCancelLevel) ||        (distanceFromSession2Open >= ZoneCancelLevel)) {      CloseAllTrades();      Print("Sweep Close triggered by 179+ point move.");   }      int ukHour = TimeHour(GetUKTime());   int ukMinute = TimeMinute(GetUKTime());   if (ukHour == 17 && ukMinute >= 16 && overnightStartPrice == 0) {      overnightStartPrice = bidPrice;   }   if (ukHour == 12 && ukMinute == 31 && middayStartPrice == 0) {      middayStartPrice = bidPrice;   }   if (ukHour == 8 && ukMinute == 0) {      if (overnightStartPrice != 0 && MathAbs(bidPrice - overnightStartPrice) >= OvernightVolatilityLimit) {         session1Allowed = false;         Print("Session 1 cancelled: Overnight move >= 200 points.");      } else {         session1Allowed = true;      }      overnightStartPrice = 0;   }   if (ukHour == 14 && ukMinute == 30) {      if (middayStartPrice != 0 && MathAbs(bidPrice - middayStartPrice) >= MiddayVolatilityLimit) {         session2Allowed = false;         Print("Session 2 cancelled: Midday move >= 150 points.");      } else {         session2Allowed = true;      }      middayStartPrice = 0;   }}void CloseAllTrades() {   for (int i = 0; i < ArraySize(trades); i++) {      if (!trades[i].isActive) continue;      if (OrderSelect(trades[i].ticket, SELECT_BY_TICKET) && OrderSymbol() == SymbolToTrade) {         double closePrice = (OrderType() == OP_BUY) ? SymbolInfoDouble(SymbolToTrade, SYMBOL_BID)                                                      : SymbolInfoDouble(SymbolToTrade, SYMBOL_ASK);         if (closePrice <= 0) continue;         bool result = OrderClose(trades[i].ticket, OrderLots(), closePrice, SlippagePoints, clrRed);         if (result) {            trades[i].isActive = false;            activeTradeCount--;         } else {            Print("Failed to close order: " + IntegerToString(GetLastError()));         }      }   }}//+------------------------------------------------------------------+//|              MODULE 8: MONEY MANAGEMENT                          |//+------------------------------------------------------------------+double CalculateLotSize() {   double equity = AccountEquity();   double lot = NormalizeDouble(equity / BetSizingFactor, 2);   if (lot < MinTradeSize) {      Print("Lot size too small for demo account: " + DoubleToString(lot, 2));      return 0.0;   }   Print("Lot size calculated for demo: " + DoubleToString(lot, 2));   return lot;}bool CanTrade() {   double lot = CalculateLotSize();   if (lot <= 0.0) return false;   if (!AccountFreeMarginCheck(SymbolToTrade, OP_BUY, lot)) {      Print("Not enough free margin.");      return false;   }   return true;}//+------------------------------------------------------------------+//|                   MODULE 9: UTILITY FUNCTIONS                    |//+------------------------------------------------------------------+bool IsTradingAllowed(datetime now) {   datetime ukNow = GetUKTime();   int hour = TimeHour(ukNow);   int minute = TimeMinute(ukNow);   if (hour >= 17 && (hour != 17 || minute >= 16)) return false;   if (hour < 8) return false;   if ((hour == 12 && minute >= 31) || (hour == 13) || (hour == 14 && minute < 30)) return false;   return true;}int CountOpenTrades() {   return activeTradeCount;}bool CloseTradeSafely(int ticket) {   if (!OrderSelect(ticket, SELECT_BY_TICKET)) return false;   for (int attempt = 0; attempt < 5; attempt++) {      double price = (OrderType() == OP_BUY) ? SymbolInfoDouble(SymbolToTrade, SYMBOL_BID)                                              : SymbolInfoDouble(SymbolToTrade, SYMBOL_ASK);      if (price <= 0) continue;      bool result = OrderClose(ticket, OrderLots(), price, SlippagePoints, clrRed);      if (result) {         Print("Trade closed successfully on attempt " + IntegerToString(attempt + 1));         return true;      }      Sleep(500);      RefreshRates();   }   Print("Failed to close trade after 5 attempts.");   return false;}bool IsPriceValid(double price) {   return (price > 0);}//+------------------------------------------------------------------+//|              MODULE 10: MAIN EVENT HANDLERS                      |//+------------------------------------------------------------------+int OnInit() {   SymbolToTrade = Symbol(); // Use the chart’s symbol dynamically   globalVarPrefix = SanitizeSymbol(SymbolToTrade); // Set sanitized prefix   Print("Using symbol: " + SymbolToTrade + ", Global Var Prefix: " + globalVarPrefix);      Print("DAX30 Strategy EA Initialized for Demo at: " + TimeToString(TimeCurrent(), TIME_DATE | TIME_MINUTES));   lastSessionInitDate = 0;      for (int i = 0; i < ArraySize(trades); i++) {      trades[i].isActive = false;   }   activeTradeCount = 0;      return INIT_SUCCEEDED;}void OnDeinit(const int reason) {   Print("DAX30 Strategy EA Deinitialized at: " + TimeToString(TimeCurrent(), TIME_DATE | TIME_MINUTES));}void OnTick() {   datetime now = TimeCurrent();   if (!IsTradingAllowed(now)) {      Print("OnTick - Skipping: Outside trading hours at " + TimeToString(now, TIME_MINUTES));      return;   }      double currentPrice = SymbolInfoDouble(SymbolToTrade, SYMBOL_BID);   if (currentPrice <= 0) {      Print("Error: Invalid bid price in OnTick: " + DoubleToString(currentPrice, 2));      return;   }      AdjustPricesBySpread(); // Calculate and set adjusted prices      Print("OnTick - Time: " + TimeToString(now, TIME_MINUTES) +          ", Price: " + DoubleToString(currentPrice, 2));      HandleSweepsAndVolatility();   CheckSessionInitialization();   UpdateSessionHighLow(session1, currentPrice);   UpdateSessionHighLow(session2, currentPrice);      if (now >= session1.startTime && now <= session1.endTime && session1Allowed) {      if (IsInActiveTradingZone(session1, 1, currentPrice)) {         CheckAndPlaceTrade(session1, currentPrice, 1);      }   }   if (now >= session2.startTime && now <= session2.endTime && session2Allowed) {      if (IsInActiveTradingZone(session2, 2, currentPrice)) {         CheckAndPlaceTrade(session2, currentPrice, 2);      }   }   CheckTradeClosure();}void UpdateTradePeakDuringOpen() {   for (int i = 0; i < ArraySize(trades); i++) {      if (!trades[i].isActive) continue;      if (OrderSelect(trades[i].ticket, SELECT_BY_TICKET) && OrderSymbol() == SymbolToTrade) {         if (OrderType() == OP_BUY) {            if (trades[i].sessionNum == 1 && session1.lowPrice < trades[i].peakLow) {               trades[i].peakLow = session1.lowPrice;               trades[i].peakTime = TimeCurrent();            } else if (trades[i].sessionNum == 2 && session2.lowPrice < trades[i].peakLow) {               trades[i].peakLow = session2.lowPrice;               trades[i].peakTime = TimeCurrent();            }         } else if (OrderType() == OP_SELL) {            if (trades[i].sessionNum == 1 && session1.highPrice > trades[i].peakHigh) {               trades[i].peakHigh = session1.highPrice;               trades[i].peakTime = TimeCurrent();            } else if (trades[i].sessionNum == 2 && session2.highPrice > trades[i].peakHigh) {               trades[i].peakHigh = session2.highPrice;               trades[i].peakTime = TimeCurrent();            }         }      }   }}//+------------------------------------------------------------------+