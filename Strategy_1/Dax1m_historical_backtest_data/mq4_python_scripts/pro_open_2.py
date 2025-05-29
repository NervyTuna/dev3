//+------------------------------------------------------------------+
//|                                                   GER30EA.mq4    |
//|        Full Strategy with Zone-Specific Distances & No-Close     |
//+------------------------------------------------------------------+
#property copyright "Your Name"
#property link      "https://www.example.com"
#property version   "3.0"
#property strict

// ---------------------------------------------------------------------------
//                          MODULE 1: GLOBAL INPUTS & STRUCTS
// ---------------------------------------------------------------------------

// We'll default the symbol to the current chart, but you can override if needed.
string   SymbolToTrade    = "";

// Magic number for trades
input int MagicNumber     = 90001;

// Broker points vs. index points multiplier.
// If 1 broker point = 0.1 DAX index point, set IDX=10 (so that 1 "index point" = 10 broker points).
#define IDX 10

// Tolerance in *index points*
input double TolerancePoints = 9.0; // e.g. 9 => 9 * IDX = 90 broker points

// StopLoss in index points (40 => 40 * IDX in broker points)
input double StopLossPoints  = 40.0;

// We keep a "universal" list of possible levels, to handle retraction skip
double entryLevels[] = {45, 70, 100, 130};

// Retraction table
// We'll implement them as 4 "bins": retraction min, retraction max, skipLevels, addPoints.
struct RetractionRange
{
   double minRetract;
   double maxRetract;
   int    skipLevels;  // 0=just addPoints, 1=skip next level, 2=skip 2 levels, -1=cancel session
   double addPoints;   // e.g. 18 if 15–29.9
};
RetractionRange retractionTable[4];

// Session times
// Session 1: 08:00–12:30
string Session1Start = "08:00";
string Session1End   = "12:30";

// Session 2: 14:30–17:16
string Session2Start = "14:30";
string Session2End   = "17:16";

// Each zone has: startHHMM, endHHMM, forcedClose, sessionNum,
//   baseDistance, noCloseRules
struct ZoneInfo
{
   string   startHHMM;
   string   endHHMM;
   datetime forcedClose; // We'll store actual forced-close time once we parse it
   int      sessionNum;
   double   baseDistance;   // e.g. 45 or 70
   bool     noCloseRules;   // if true => skip break-even/time-based
};
ZoneInfo session1Zones[];
ZoneInfo session2Zones[];

// Flags for session allowed
bool session1Allowed = true;
bool session2Allowed = true;

// If retraction≥46 and no trades open => block entire session
bool blockSession1   = false;
bool blockSession2   = false;

// "Used" levels for each session in each zone
// We'll store them in a 2D array [zoneIndex][0..3] if needed, or we can store used globally
// The brief does not explicitly say we cannot reuse a level in a different zone. Usually
// you only block re-using the same level within the same session. We'll do per-session basis.
bool usedLevelSession1[4];
bool usedLevelSession2[4];

// Volatility constraints
double overnightStartPrice = 0.0; // price at 17:16
bool   skipTomorrowSession1 = false;

double middayStartPrice     = 0.0; // price at 12:00
bool   skipTodaySession2    = false;

// We'll do a once-per-minute OnTimer check for sweeps & volatility
datetime lastSweepCheck     = 0;

// For dynamic spread handling
bool   ForceSpread          = false;   // override if user wants
double OverriddenSpread     = 2.0;     // forced spread value
double DefaultSpreadPoints  = 5.0;
int    SlippagePoints       = 3;

// Spread schedule
struct SpreadTime
{
   string startTime;
   string endTime;
   double spreadPoints;
};
SpreadTime spreadSchedule[] =
{
   {"01:15","08:00", 4.0},
   {"08:00","09:00", 2.0},
   {"09:00","17:30", 1.2},
   {"17:30","22:00", 2.0},
   {"22:00","23:59", 5.0},
   {"00:00","01:15", 5.0}
};

// We'll track adjusted bid/ask in global variables
string globalVarPrefix;

// For money management: bet size = equity/800
double BetDivisor = 800.0;

// We'll store session data for open, high, low
struct SessionData
{
   datetime startTime;
   datetime endTime;
   double   openPrice;
   double   highPrice;
   double   lowPrice;
   bool     isActive;
};
SessionData s1;
SessionData s2;

// We'll track trades in an array for logging
struct TradeInfo
{
   int      ticket;
   int      sessionNumber;
   double   entryPrice;
   double   peakHigh;
   double   peakLow;
   datetime peakTime;  // Added peakTime field
   datetime openTime;
   datetime forcedCloseTime;
   bool     active;
   double   finalDistanceUsed;
   bool     noCloseRules;
};
TradeInfo tradeArray[10];

// ---------------------------------------------------------------------------
//                          MODULE 2: INITIALIZATION
// ---------------------------------------------------------------------------
int OnInit()
{
   if(SymbolToTrade=="")
      SymbolToTrade = Symbol();

   globalVarPrefix = SanitizeSymbol(SymbolToTrade) + "_SpreadVars";

   // Retraction table
   // 15–29.9 => skipLevels=0 => +18
   // 30–35.9 => skipLevels=1
   // 36–45.9 => skipLevels=2
   // >=46    => skipLevels=-1 => session cancel
   retractionTable[0].minRetract  = 15.0;
   retractionTable[0].maxRetract  = 29.9;
   retractionTable[0].skipLevels  = 0;
   retractionTable[0].addPoints   = 18.0;

   retractionTable[1].minRetract  = 30.0;
   retractionTable[1].maxRetract  = 35.9;
   retractionTable[1].skipLevels  = 1;
   retractionTable[1].addPoints   = 0.0;

   retractionTable[2].minRetract  = 36.0;
   retractionTable[2].maxRetract  = 45.9;
   retractionTable[2].skipLevels  = 2;
   retractionTable[2].addPoints   = 0.0;

   retractionTable[3].minRetract  = 46.0;
   retractionTable[3].maxRetract  = 999999.0;
   retractionTable[3].skipLevels  = -1;  // session cancel
   retractionTable[3].addPoints   = 0.0;

   // -----------------------------------------------------------------------
   // Build out session 1 zones, each with a baseDistance + forcedClose + noCloseRules
   // The brief states:
   //   1) 08:16–09:05 => base=45 => forcedClose=09:31 => noCloseRules=TRUE
   //   2) 09:30–09:45 => base=45 => forcedClose=10:06 => normal close rules
   //   3) 10:15–10:45 => base=70 => forcedClose=12:31 => normal close rules
   //   4) 10:45–11:45 => base=45 => forcedClose=12:31 => normal close rules
   ArrayResize(session1Zones,4);

   session1Zones[0].startHHMM     = "08:16";
   session1Zones[0].endHHMM       = "09:05";
   session1Zones[0].forcedClose   = 0; // fill later
   session1Zones[0].sessionNum    = 1;
   session1Zones[0].baseDistance  = 45;
   session1Zones[0].noCloseRules  = true; // special early zone

   session1Zones[1].startHHMM     = "09:30";
   session1Zones[1].endHHMM       = "09:45";
   session1Zones[1].forcedClose   = 0;
   session1Zones[1].sessionNum    = 1;
   session1Zones[1].baseDistance  = 45;
   session1Zones[1].noCloseRules  = false;

   session1Zones[2].startHHMM     = "10:15";
   session1Zones[2].endHHMM       = "10:45";
   session1Zones[2].forcedClose   = 0;
   session1Zones[2].sessionNum    = 1;
   session1Zones[2].baseDistance  = 70;
   session1Zones[2].noCloseRules  = false;

   session1Zones[3].startHHMM     = "10:45";
   session1Zones[3].endHHMM       = "11:45";
   session1Zones[3].forcedClose   = 0;
   session1Zones[3].sessionNum    = 1;
   session1Zones[3].baseDistance  = 45;
   session1Zones[3].noCloseRules  = false;

   // -----------------------------------------------------------------------
   // Session 2 zones:
   //   1) 14:46–15:06 => base=45 => forcedClose=17:16 => normal
   //   2) 15:15–15:45 => base=70 => forcedClose=17:16 => normal
   //   3) 15:45–16:48 => base=45 => forcedClose=17:16 => normal
   ArrayResize(session2Zones,3);

   session2Zones[0].startHHMM     = "14:46";
   session2Zones[0].endHHMM       = "15:06";
   session2Zones[0].forcedClose   = 0;
   session2Zones[0].sessionNum    = 2;
   session2Zones[0].baseDistance  = 45;
   session2Zones[0].noCloseRules  = false;

   session2Zones[1].startHHMM     = "15:15";
   session2Zones[1].endHHMM       = "15:45";
   session2Zones[1].forcedClose   = 0;
   session2Zones[1].sessionNum    = 2;
   session2Zones[1].baseDistance  = 70;
   session2Zones[1].noCloseRules  = false;

   session2Zones[2].startHHMM     = "15:45";
   session2Zones[2].endHHMM       = "16:48";
   session2Zones[2].forcedClose   = 0;
   session2Zones[2].sessionNum    = 2;
   session2Zones[2].baseDistance  = 45;
   session2Zones[2].noCloseRules  = false;

   // Initialize used arrays
   for(int i=0; i<4; i++)
   {
      usedLevelSession1[i] = false;
      usedLevelSession2[i] = false;
   }

   // Setup session data
   SetupSessions();

   // Set 1-minute timer
   EventSetTimer(60);

   Print("[OnInit] Completed. Symbol=", SymbolToTrade,", Magic=",MagicNumber);
   return(INIT_SUCCEEDED);
}

void OnDeinit(const int reason)
{
   EventKillTimer();
   Print("[OnDeinit] reason=", reason);
}

// ---------------------------------------------------------------------------
//                          MODULE 3: TIME & SESSION SETUP
// ---------------------------------------------------------------------------
void SetupSessions()
{
   MqlDateTime dtNow;
   TimeToStruct(TimeCurrent(), dtNow);
   string ymd = StringFormat("%04d.%02d.%02d", dtNow.year, dtNow.mon, dtNow.day);

   // Session 1
   s1.startTime = StringToTime(ymd+" "+Session1Start);
   s1.endTime   = StringToTime(ymd+" "+Session1End);
   s1.isActive  = false;

   // Session 2
   s2.startTime = StringToTime(ymd+" "+Session2Start);
   s2.endTime   = StringToTime(ymd+" "+Session2End);
   s2.isActive  = false;

   // forced close times for session 1
   // zone0 => 09:31, zone1 => 10:06, zone2 & zone3 => 12:31
   for(int i=0; i<ArraySize(session1Zones); i++)
   {
      if(i==0)
         session1Zones[i].forcedClose = StringToTime(ymd+" 09:31");
      else if(i==1)
         session1Zones[i].forcedClose = StringToTime(ymd+" 10:06");
      else
         session1Zones[i].forcedClose = StringToTime(ymd+" 12:31");
   }
   // forced close times for session 2 => all 17:16
   for(int j=0; j<ArraySize(session2Zones); j++)
   {
      session2Zones[j].forcedClose = StringToTime(ymd+" 17:16");
   }

   session1Allowed = true;
   session2Allowed = true;
   blockSession1   = false;
   blockSession2   = false;

   skipTomorrowSession1 = false;
   skipTodaySession2    = false;

   s1.isActive = false;
   s2.isActive = false;

   Print("[SetupSessions] S1:", Session1Start,"-",Session1End,
         " S2:", Session2Start,"-",Session2End);
}

// We'll call this each tick to see if it's time to activate or deactivate sessions
void CheckSessionState()
{
   datetime nowT = TimeCurrent();

   // Session 1
   if(!s1.isActive && nowT >= s1.startTime && nowT < s1.endTime)
   {
      s1.isActive = true;
      double bidVal = SymbolInfoDouble(SymbolToTrade, SYMBOL_BID);
      if(bidVal<=0) { RefreshRates(); bidVal=SymbolInfoDouble(SymbolToTrade,SYMBOL_BID);}
      if(bidVal>0)
      {
         s1.openPrice = bidVal;
         s1.highPrice = bidVal;
         s1.lowPrice  = bidVal;
         Print("[Session1] Activated. openPrice=", s1.openPrice);
      }
   }
   else if(s1.isActive && nowT >= s1.endTime)
   {
      s1.isActive = false;
      Print("[Session1] Ended for the day.");
   }

   // Session 2
   if(!s2.isActive && nowT >= s2.startTime && nowT < s2.endTime)
   {
      s2.isActive = true;
      double bidVal2= SymbolInfoDouble(SymbolToTrade, SYMBOL_BID);
      if(bidVal2<=0) { RefreshRates(); bidVal2=SymbolInfoDouble(SymbolToTrade,SYMBOL_BID);}
      if(bidVal2>0)
      {
         s2.openPrice = bidVal2;
         s2.highPrice = bidVal2;
         s2.lowPrice  = bidVal2;
         Print("[Session2] Activated. openPrice=", s2.openPrice);
      }
   }
   else if(s2.isActive && nowT >= s2.endTime)
   {
      s2.isActive = false;
      Print("[Session2] Ended for the day.");
   }
}

// ---------------------------------------------------------------------------
//                          MODULE 4: ONTICK & ONTIMER
// ---------------------------------------------------------------------------
void OnTick()
{
   // 1) Adjust bid/ask by dynamic spread
   if(!AdjustPricesBySpread()) return;

   // 2) Check session states
   CheckSessionState();

   // 3) If session active, update high/low
   double adjBid = GlobalVariableGet(globalVarPrefix+"_AdjBid");
   if(adjBid>0)
   {
      if(s1.isActive) UpdateHighLow(s1, adjBid);
      if(s2.isActive) UpdateHighLow(s2, adjBid);
   }

   // 4) Attempt to place new trade for session1
   if(s1.isActive && session1Allowed && !blockSession1 && !skipTomorrowSession1)
      CheckZonesAndMaybeOpen(s1, session1Zones, ArraySize(session1Zones), 1);

   // 5) Attempt for session2
   if(s2.isActive && session2Allowed && !blockSession2 && !skipTodaySession2)
      CheckZonesAndMaybeOpen(s2, session2Zones, ArraySize(session2Zones), 2);

   // 6) Manage open trades
   ManageOpenTrades();
}

void OnTimer()
{
   // once per minute
   HandleSweepsAndVolatility();
}

// ---------------------------------------------------------------------------
//                           MODULE 5: SPREAD & PRICING
// ---------------------------------------------------------------------------
bool AdjustPricesBySpread()
{
   double bid = SymbolInfoDouble(SymbolToTrade, SYMBOL_BID);
   double ask = SymbolInfoDouble(SymbolToTrade, SYMBOL_ASK);
   if(bid<=0 || ask<=0)
   {
      RefreshRates();
      bid=SymbolInfoDouble(SymbolToTrade,SYMBOL_BID);
      ask=SymbolInfoDouble(SymbolToTrade,SYMBOL_ASK);
      if(bid<=0 || ask<=0) return false;
   }

   double spr = GetSpreadForTime();
   double adjBid = bid - (spr * Point);
   double adjAsk = ask + (spr * Point);
   adjBid = NormalizeDouble(adjBid, _Digits);
   adjAsk = NormalizeDouble(adjAsk, _Digits);

   GlobalVariableSet(globalVarPrefix+"_AdjBid", adjBid);
   GlobalVariableSet(globalVarPrefix+"_AdjAsk", adjAsk);
   GlobalVariableSet(globalVarPrefix+"_Spread", spr);

   return true;
}

double GetSpreadForTime()
{
   if(ForceSpread) return OverriddenSpread;

   datetime nowT = TimeCurrent();
   int h = TimeHour(nowT);
   int m = TimeMinute(nowT);
   int totalMins = h*60 + m;

   for(int i=0; i<ArraySize(spreadSchedule); i++)
   {
      int st = StrToMins(spreadSchedule[i].startTime);
      int en = StrToMins(spreadSchedule[i].endTime);
      if(st<=en)
      {
         if(totalMins>=st && totalMins<=en)
            return spreadSchedule[i].spreadPoints;
      }
      else
      {
         // wrap midnight
         if(totalMins>=st || totalMins<=en)
            return spreadSchedule[i].spreadPoints;
      }
   }
   return DefaultSpreadPoints;
}

int StrToMins(string hhmm)
{
   int h = (int)StringToInteger(StringSubstr(hhmm,0,2));
   int m = (int)StringToInteger(StringSubstr(hhmm,3,2));
   return h*60 + m;
}

string SanitizeSymbol(string s)
{
   StringReplace(s,"(","_");
   StringReplace(s,")","_");
   StringReplace(s,"£","GBP");
   return s;
}

// ---------------------------------------------------------------------------
//                         MODULE 6: HIGH/LOW UPDATING
// ---------------------------------------------------------------------------
void UpdateHighLow(SessionData &s, double price)
{
   if(!s.isActive) return;
   if(price> s.highPrice) s.highPrice = price;
   if(price< s.lowPrice ) s.lowPrice  = price;
}

// ---------------------------------------------------------------------------
//                       MODULE 7: ZONE CHECK & TRADE ENTRY
// ---------------------------------------------------------------------------
void CheckZonesAndMaybeOpen(SessionData &sess, ZoneInfo &zones[], int zCount, int sessionNum)
{
   if(!sess.isActive) return;

   // If there's already an open trade for this session => skip
   if(GetOpenTradeCountForSession(sessionNum)>=1) return;

   // If retraction≥46 => block session if no trades open
   double r = CalculateRetraction(sess);
   if(r>=46.0 && GetOpenTradeCountForSession(sessionNum)==0)
   {
      if(sessionNum==1) blockSession1=true; else blockSession2=true;
      Print("[Retraction>=46] Cancel session #", sessionNum);
      return;
   }

   datetime nowT = TimeCurrent();
   double adjBid = GlobalVariableGet(globalVarPrefix+"_AdjBid");
   if(adjBid<=0) return;

   // We see if "nowT" is inside any zone => that zone's baseDistance + forcedClose + noCloseRules
   // We'll open only once if found a zone
   for(int i=0; i<zCount; i++)
   {
      if(zones[i].sessionNum != sessionNum) continue;

      datetime zs = StringToTime(GetDateStr()+" "+zones[i].startHHMM);
      datetime ze = StringToTime(GetDateStr()+" "+zones[i].endHHMM);
      if(nowT>=zs && nowT<=ze)
      {
         // This is our zone
         double baseDist = zones[i].baseDistance;
         bool   skipCloseRules = zones[i].noCloseRules; 
         datetime forcedC = zones[i].forcedClose;

         // We'll figure out if price is above or below the session open
         bool isAbove=false;
         double distFromOpen=0;
         if(adjBid>= sess.openPrice)
         {
            isAbove=true;
            distFromOpen=(adjBid - sess.openPrice)*IDX/Point;
         }
         else
         {
            isAbove=false;
            distFromOpen=(sess.openPrice - adjBid)*IDX/Point;
         }

         // 1) find baseIndex for baseDist in the global array [45,70,100,130]
         int baseIndex = -1;
         for(int k=0; k<4; k++)
         {
            if(MathAbs(entryLevels[k]-baseDist)<0.001)
            {
               baseIndex=k;
               break;
            }
         }
         if(baseIndex<0)
         {
            Print("[CheckZones] Could not find baseIndex for dist=",baseDist);
            return;
         }

         // 2) apply retraction skip => finalIndex
         int finalIndex = DetermineFinalIndexWithRetraction(baseIndex, r);
         if(finalIndex<0) return; // session block or no valid

         double finalLvl = entryLevels[finalIndex];
         // If finalLvl > (130+Tolerance) => no trade
         if(finalLvl>(130+TolerancePoints)) return;

         // 3) check if this level is used
         if(sessionNum==1 && usedLevelSession1[finalIndex]) return;
         if(sessionNum==2 && usedLevelSession2[finalIndex]) return;

         // 4) check if currentDist is within [finalLvl, finalLvl+Tolerance]
         if(distFromOpen< finalLvl) return; // not reached
         if(distFromOpen> (finalLvl + TolerancePoints)) return; // overshot
         
         // 5) place trade
         double lots = CalculateLotSize();
         if(lots<=0) return;

         int cmd = (isAbove)? OP_SELL : OP_BUY;
         bool success = PlaceTradeWithRetry(cmd, lots, sessionNum, finalLvl, forcedC, skipCloseRules);
         if(success)
         {
            // mark used
            if(sessionNum==1) usedLevelSession1[finalIndex]=true;
            else usedLevelSession2[finalIndex]=true;
         }

         // Only one trade attempt per tick, so return
         return;
      }
   }
}

// We shift from baseIndex by skip if ret≥15, etc. We also addPoints if skipLevels=0 but addPoints>0
int DetermineFinalIndexWithRetraction(int baseIdx, double ret)
{
   int retAction = CheckRetractionRange(ret);
   if(retAction<0) return -1; // session cancel

   int skip  = retractionTable[retAction].skipLevels;
   double addPts = retractionTable[retAction].addPoints;

   int finalIndex = baseIdx + skip;
   if(finalIndex>3) finalIndex=3;

   double baseVal   = entryLevels[baseIdx];
   double nextVal   = entryLevels[finalIndex];
   double withAdd   = baseVal + addPts;
   double chosenDist= MathMax(withAdd, nextVal);

   // see which level that belongs to in [45,70,100,130]
   // or if it surpasses 130 => no trade
   if(chosenDist > 130+TolerancePoints) return -1; 
   // find final index
   int matched=-1;
   for(int i=0; i<4; i++)
   {
      if(MathAbs(chosenDist - entryLevels[i])<0.5)
      {
         matched=i; break;
      }
      if(i<3 && chosenDist> entryLevels[i] && chosenDist< entryLevels[i+1])
      {
         matched = i+1;
         break;
      }
      if(i==3 && chosenDist>entryLevels[3]) 
      {
         matched=3;
         break;
      }
   }
   return matched;
}

double CalculateRetraction(SessionData &sess)
{
   double bidAdj = GlobalVariableGet(globalVarPrefix+"_AdjBid");
   if(bidAdj<=0) return 0;
   if(bidAdj >= sess.openPrice)
   {
      // retraction from top
      double dist = (sess.highPrice - bidAdj)*IDX/Point;
      return dist;
   }
   else
   {
      double dist = (bidAdj - sess.lowPrice)*IDX/Point;
      return dist;
   }
}

int CheckRetractionRange(double r)
{
   // returns index into retractionTable or -1 if session-cancel
   for(int i=0; i<4; i++)
   {
      if(r>= retractionTable[i].minRetract && r<= retractionTable[i].maxRetract)
      {
         if(retractionTable[i].skipLevels==-1) return -1; 
         return i;
      }
   }
   // if below 15 => no retraction
   return 0;
}

// ---------------------------------------------------------------------------
//                          MODULE 8: PLACING TRADES
// ---------------------------------------------------------------------------
bool PlaceTradeWithRetry(int cmd, double lots, int sessionNum, double finalDist, datetime forcedCloseTime, bool noCloseRules)
{
   int attempts = 0;
   double price = (cmd == OP_BUY) ? GlobalVariableGet(globalVarPrefix + "_AdjAsk") : GlobalVariableGet(globalVarPrefix + "_AdjBid");
   if (price <= 0) return false;

   double slDistPoints = StopLossPoints * IDX * Point;
   double slPrice = (cmd == OP_BUY) ? (price - slDistPoints) : (price + slDistPoints);
   slPrice = NormalizeDouble(slPrice, _Digits);

   double stopLevelPoints = MarketInfo(SymbolToTrade, MODE_STOPLEVEL) * Point;
   if (slDistPoints < stopLevelPoints)
   {
      Print("[TradeOpen] StopLoss < broker STOPLEVEL => cannot open trade.");
      return false;
   }

   int ticket = -1;
   for (attempts = 0; attempts < 5; attempts++)
   {
      RefreshRates();
      double usedPrice = (cmd == OP_BUY) ? SymbolInfoDouble(SymbolToTrade, SYMBOL_ASK) : SymbolInfoDouble(SymbolToTrade, SYMBOL_BID);
      if (usedPrice <= 0)
      {
         Sleep(200);
         continue;
      }
      usedPrice = NormalizeDouble(usedPrice, _Digits);

      ticket = OrderSend(SymbolToTrade, cmd, lots, usedPrice, SlippagePoints,
                         (cmd == OP_BUY) ? (usedPrice - slDistPoints) : (usedPrice + slDistPoints),
                         0, "GER30EA", MagicNumber, 0, clrBlue);
      if (ticket > 0)
      {
         Print("[TradeOpen] success #", attempts + 1, " ticket=", ticket, ", lots=", lots,
               ", SL=", DoubleToString(slPrice, _Digits), ", finalDist=", finalDist, ", noCloseRules=", noCloseRules);
         for (int i = 0; i < ArraySize(tradeArray); i++)
         {
            if (!tradeArray[i].active)
            {
               tradeArray[i].ticket = ticket;
               tradeArray[i].sessionNumber = sessionNum;
               tradeArray[i].entryPrice = usedPrice;
               tradeArray[i].peakHigh = usedPrice;
               tradeArray[i].peakLow = usedPrice;
               tradeArray[i].peakTime = TimeCurrent();  // Initialize peakTime
               tradeArray[i].openTime = TimeCurrent();
               tradeArray[i].forcedCloseTime = forcedCloseTime;
               tradeArray[i].active = true;
               tradeArray[i].finalDistanceUsed = finalDist;
               tradeArray[i].noCloseRules = noCloseRules;
               break;
            }
         }
         return true;
      }
      else
      {
         Print("[TradeOpen] fail #", attempts + 1, ", err=", GetLastError());
         Sleep(500);
      }
   }
   return false;
}

double CalculateLotSize()
{
   double eq = AccountEquity();
   double raw = eq / BetDivisor;
   double step = SymbolInfoDouble(SymbolToTrade, SYMBOL_VOLUME_STEP);
   if(step<=0) step=0.01;
   double lots = MathFloor(raw/step)*step;
   double minL= SymbolInfoDouble(SymbolToTrade, SYMBOL_VOLUME_MIN);
   double maxL= SymbolInfoDouble(SymbolToTrade, SYMBOL_VOLUME_MAX);
   if(lots< minL) return 0.0;
   if(lots> maxL) lots = maxL;
   return NormalizeDouble(lots,2);
}

int GetOpenTradeCountForSession(int sessNum)
{
   int cnt=0;
   for(int i=0; i<ArraySize(tradeArray); i++)
   {
      if(tradeArray[i].active && tradeArray[i].sessionNumber==sessNum)
         cnt++;
   }
   return cnt;
}

// ---------------------------------------------------------------------------
//                     MODULE 9: MANAGE OPEN TRADES (CLOSE LOGIC)
// ---------------------------------------------------------------------------
void ManageOpenTrades()
{
   datetime nowT = TimeCurrent();
   for (int i = 0; i < ArraySize(tradeArray); i++)
   {
      if (!tradeArray[i].active) continue;

      int tk = tradeArray[i].ticket;
      if (!OrderSelect(tk, SELECT_BY_TICKET, MODE_TRADES))
      {
         tradeArray[i].active = false;
         continue;
      }

      if (nowT >= tradeArray[i].forcedCloseTime)
      {
         CloseTradeWithRetry(tk);
         tradeArray[i].active = false;
         continue;
      }

      if (tradeArray[i].noCloseRules) continue;

      double cp = (OrderType() == OP_BUY) ? SymbolInfoDouble(SymbolToTrade, SYMBOL_BID) : SymbolInfoDouble(SymbolToTrade, SYMBOL_ASK);
      if (cp > 0)
      {
         if (OrderType() == OP_BUY)
         {
            if (cp > tradeArray[i].peakHigh)
            {
               tradeArray[i].peakHigh = cp;
               tradeArray[i].peakTime = nowT;  // Update peakTime
            }
            if (cp < tradeArray[i].peakLow) tradeArray[i].peakLow = cp;
         }
         else
         {
            if (cp < tradeArray[i].peakLow)
            {
               tradeArray[i].peakLow = cp;
               tradeArray[i].peakTime = nowT;  // Update peakTime
            }
            if (cp > tradeArray[i].peakHigh) tradeArray[i].peakHigh = cp;
         }
      }

      int holdMins = (int)((nowT - tradeArray[i].peakTime) / 60);  // Use peakTime for hold duration
      double usedDist = tradeArray[i].finalDistanceUsed;

      if (usedDist >= 45 && usedDist < 70)
      {
         double openP = OrderOpenPrice();
         double adv = (OrderType() == OP_BUY) ? (openP - cp) * IDX / Point : (cp - openP) * IDX / Point;

         if (adv >= 15)
         {
            double diff = MathAbs(cp - openP) * IDX / Point;
            if (diff < 1.0)
            {
               Print("[BreakEvenClose] #", tk, " => 15 adverse & returned => close@0");
               CloseTradeWithRetry(tk);
               tradeArray[i].active = false;
               continue;
            }
         }
         if (holdMins >= 16)
         {
            Print("[TimeClose16] #", tk, ", hold=", holdMins, " => close");
            CloseTradeWithRetry(tk);
            tradeArray[i].active = false;
            continue;
         }
      }
      else if (usedDist >= 70 && usedDist <= 159.9)
      {
         if (holdMins >= 31)
         {
            Print("[TimeClose31] #", tk, ", hold=", holdMins, " => close");
            CloseTradeWithRetry(tk);
            tradeArray[i].active = false;
            continue;
         }
      }
   }
}

bool CloseTradeWithRetry(int ticket)
{
   if(!OrderSelect(ticket, SELECT_BY_TICKET)) return false;
   for(int i=0; i<5; i++)
   {
      double cp= (OrderType()==OP_BUY)? SymbolInfoDouble(SymbolToTrade, SYMBOL_BID)
                                      : SymbolInfoDouble(SymbolToTrade, SYMBOL_ASK);
      if(cp<=0)
      {
         RefreshRates();
         Sleep(200);
         continue;
      }
      cp= NormalizeDouble(cp, _Digits);

      bool res= OrderClose(ticket, OrderLots(), cp, SlippagePoints, clrRed);
      if(res)
      {
         Print("[CloseTrade] success ticket=", ticket, ", attempt #", i+1);
         LogCloseInfo(ticket);
         return true;
      }
      else
      {
         Print("[CloseTrade] fail #", i+1,", err=", GetLastError());
         Sleep(500);
      }
   }
   return false;
}

void LogCloseInfo(int ticket)
{
   if(OrderSelect(ticket, SELECT_BY_TICKET, MODE_HISTORY))
   {
      double pl= OrderProfit() + OrderSwap() + OrderCommission();
      PrintFormat("[Closed] #%d %s lots=%.2f open=%.2f close=%.2f P/L=%.2f",
                  ticket,
                  (OrderType()==OP_BUY?"BUY":"SELL"),
                  OrderLots(),
                  OrderOpenPrice(),
                  OrderClosePrice(),
                  pl);
   }
}

// ---------------------------------------------------------------------------
//                   MODULE 10: SWEEPS & VOLATILITY (OnTimer tasks)
// ---------------------------------------------------------------------------
void HandleSweepsAndVolatility()
{
   datetime nowT= TimeCurrent();
   if(nowT - lastSweepCheck < 60) return; // once per minute
   lastSweepCheck= nowT;

   double adjBid=GlobalVariableGet(globalVarPrefix+"_AdjBid");
   if(adjBid>0)
   {
      // 1) if session1 active and distance≥179 => close all
      if(s1.isActive)
      {
         double dist1 = MathAbs(adjBid - s1.openPrice)*IDX/Point;
         if(dist1>=179.0)
         {
            Print("[Sweep] session1 dist=",dist1," => close all trades");
            CloseAllTradesForSession(1);
         }
      }
      // 2) session2 as well
      if(s2.isActive)
      {
         double dist2 = MathAbs(adjBid - s2.openPrice)*IDX/Point;
         if(dist2>=179.0)
         {
            Print("[Sweep] session2 dist=",dist2," => close all trades");
            CloseAllTradesForSession(2);
         }
      }
   }

   // 3) if time is outside 08:00–12:30 & 14:30–17:16 => close all trades
   bool inS1= (nowT>=s1.startTime && nowT<s1.endTime);
   bool inS2= (nowT>=s2.startTime && nowT<s2.endTime);
   if(!inS1 && !inS2)
   {
      // close any open
      for(int i=0; i<ArraySize(tradeArray); i++)
      {
         if(tradeArray[i].active)
         {
            CloseTradeWithRetry(tradeArray[i].ticket);
            tradeArray[i].active=false;
         }
      }
   }

   // 4) track overnight volatility: if exactly 17:16 => record
   if(TimeHour(nowT)==17 && TimeMinute(nowT)==16)
   {
      overnightStartPrice= SymbolInfoDouble(SymbolToTrade,SYMBOL_BID);
      Print("[OvernightStart] 17:16 price=",overnightStartPrice);
   }
   // if 08:00 => compare with overnight
   if(TimeHour(nowT)==8 && TimeMinute(nowT)==0)
   {
      if(overnightStartPrice>0)
      {
         double curBid= SymbolInfoDouble(SymbolToTrade,SYMBOL_BID);
         double ovDist= MathAbs(curBid - overnightStartPrice)*IDX/Point;
         if(ovDist>=200.0)
         {
            skipTomorrowSession1=true; 
            Print("[VolatilityBlock] overnight≥200 => skip session1");
         }
      }
      overnightStartPrice=0;
   }

   // 5) midday volatility: if 12:00 => record
   if(TimeHour(nowT)==12 && TimeMinute(nowT)==0)
   {
      middayStartPrice= SymbolInfoDouble(SymbolToTrade,SYMBOL_BID);
      Print("[MiddayTrack] 12:00 => ", middayStartPrice);
   }
   // if 14:30 => compare
   if(TimeHour(nowT)==14 && TimeMinute(nowT)==30)
   {
      if(middayStartPrice>0)
      {
         double cb= SymbolInfoDouble(SymbolToTrade,SYMBOL_BID);
         double diff= MathAbs(cb - middayStartPrice)*IDX/Point;
         if(diff>=150.0)
         {
            skipTodaySession2= true;
            Print("[VolatilityBlock] midday≥150 => skip session2");
         }
      }
      middayStartPrice=0;
   }
}

void CloseAllTradesForSession(int sessNum)
{
   for(int i=0; i<ArraySize(tradeArray); i++)
   {
      if(tradeArray[i].active && tradeArray[i].sessionNumber==sessNum)
      {
         CloseTradeWithRetry(tradeArray[i].ticket);
         tradeArray[i].active=false;
      }
   }
}

string GetDateStr()
{
   MqlDateTime sdt;
   TimeToStruct(TimeCurrent(), sdt);
   return StringFormat("%04d.%02d.%02d", sdt.year, sdt.mon, sdt.day);
}
// ---------------------------------------------------------------------------
//                             END OF FILE
// ---------------------------------------------------------------------------
