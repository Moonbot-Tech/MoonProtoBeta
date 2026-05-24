//! `TStrategySerializer` reader/writer — Delphi wire-format port.
//!
//! Источник Delphi: `MoonProto/StrategySerializer.pas` (~1118 строк).
//!
//! ## Назначение
//! Парсит RTTI-driven binary snapshot стратегий из payload'а `TStratSnapshot.data`.
//! Сервер (Delphi MoonBot) использует RTTI для итерации по public-полям `TStrategy`;
//! Rust-клиент не имеет RTTI, поэтому хранит поля как `HashMap<FieldName, FieldValue>`.
//!
//! ## Wire format (после DEFLATE decompression, raw -15)
//!
//! ```text
//! NameDict:    Count:u16 + (NameLen:u8 + Name:bytes[NameLen]) * Count    // UTF-8
//! PathDict:    Count:u16 + (PathLen:u8 + Path:bytes[PathLen]) * Count    // UTF-8
//! StratCount:  u16
//! Strategies[StratCount]:
//!     StrategyID:        u64
//!     StrategyVer:       i32
//!     StrategyLastDate:  u64    // unix epoch ms
//!     Checked:           u8     // boolean
//!     Kind:              u8     // TStrategyKind ordinal
//!     PathID:            u16    // index в PathDict
//!     FieldCount:        u16
//!     Fields[FieldCount]:
//!         FieldIdx:      u16    // index в NameDict
//!         TypeID:        u8     // (с возможным флагом TID_ZERO_FLAG = 0x80)
//!         [value]               // отсутствует если ZERO_FLAG установлен; иначе зависит от типа
//! ```
//!
//! ## TypeID constants
//! - `TID_BOOL=1`:    1 byte
//! - `TID_INT32=2`:   4 bytes (signed)
//! - `TID_INT64=3`:   8 bytes (signed)
//! - `TID_DOUBLE=4`:  8 bytes (f64)
//! - `TID_STRING=5`:  u16 LE prefix + UTF-8 bytes
//! - `TID_BYTE=6`:    1 byte (unsigned)
//! - `TID_WORD=7`:    2 bytes (unsigned)
//! - `TID_UINT32=8`:  4 bytes (unsigned)
//! - `TID_UINT64=9`:  8 bytes (unsigned)
//! - `TID_SINGLE=10`: 4 bytes (f32)
//! - `TID_ZERO_FLAG = 0x80` (high bit): значение = zero для соответствующего типа, value bytes отсутствуют.
//!
//! ## Unknown TypeID
//! Reader делает fallback skip 8 байт (как Delphi `SkipFieldByTypeID`).

use std::collections::HashMap;
use std::io::{Read, Write};

use super::registry::decode_utf8_delphi;
use flate2::read::DeflateDecoder;
use flate2::write::DeflateEncoder;
use flate2::Compression;

// =============================================================================
//  TypeID constants
// =============================================================================

pub const TID_BOOL: u8 = 1;
pub const TID_INT32: u8 = 2;
pub const TID_INT64: u8 = 3;
pub const TID_DOUBLE: u8 = 4;
pub const TID_STRING: u8 = 5;
pub const TID_BYTE: u8 = 6;
pub const TID_WORD: u8 = 7;
pub const TID_UINT32: u8 = 8;
pub const TID_UINT64: u8 = 9;
pub const TID_SINGLE: u8 = 10;
pub const TID_ZERO_FLAG: u8 = 0x80;

// Delphi StrategySerializer.SaveStrategyToCompact iterates GetStrategyPropMask,
// whose base order is TRttiContext.GetType(TStrategy).GetFields filtered to
// public fields (Strategies.pas:4758-4766). RebuildFiledsList only removes
// fields from that list, so this declaration order is the stable wire order for
// any provided snapshot fields.
const DELPHI_STRATEGY_FIELD_ORDER: &[&str] = &[
    "StrategyName",
    "Comment",
    "LastEditDate",
    "SignalType",
    "ChannelName",
    "ChannelKey",
    "AcceptCommands",
    "OnlyEncryptedCommands",
    "SilentNoCharts",
    "ReportToTelegram",
    "ReportTradesToTelegram",
    "SoundAlert",
    "SoundKind",
    "KeepAlert",
    "AddToChart",
    "KeepInChart",
    "EmulatorMode",
    "DebugLog",
    "IndependentSignals",
    "DontWriteLog",
    "DontKeepOrdersOnChart",
    "UseCustomColors",
    "OrderLineKind",
    "SellOrderColor",
    "BuyOrderColor",
    "DynWL_SortBy",
    "DynWL_SortDesc",
    "DynWL_Count",
    "DynBL_SortBy",
    "DynBL_SortDesc",
    "DynBL_Count",
    "Dyn_Refresh",
    "IgnoreFilters",
    "IgnoreGatePenalty",
    "CoinsWhiteList",
    "CoinsBlackList",
    "OnlyNewListing",
    "DontTradeListing",
    "LeveragedTokens",
    "ListedType",
    "CheckAfterBuy",
    "DontCheckBeforeBuy",
    "NextDetectPenalty",
    "PreventWorkingUntil",
    "IgnoreBase",
    "BinanceTokenTags",
    "MinLeverage",
    "MaxLeverage",
    "CustomEMA",
    "MoonIntRiskLevel",
    "MoonIntStopLevel",
    "MarkPriceMin",
    "MarkPriceMax",
    "IgnoreTime",
    "WorkingTime",
    "PenaltyTime",
    "TradePenaltyTime",
    "GlobalDetectPenalty",
    "FundingBefore",
    "FundingAfter",
    "IgnorePrice",
    "MaxBalance",
    "SamePosition",
    "MaxPosition",
    "SessionProfitMin",
    "SessionProfitMax",
    "TotalLoss",
    "WorkingPriceMax",
    "WorkingPriceMin",
    "PriceStepMin",
    "PriceStepMax",
    "UseBTCPriceStep",
    "IgnorePing",
    "MaxPing",
    "MinPing",
    "MaxLatency",
    "BinancePriceBug",
    "BinancePriceBugMin",
    "IgnoreVolume",
    "MinVolume",
    "MaxVolume",
    "MinHourlyVolume",
    "MaxHourlyVolume",
    "MinHourlyVolFast",
    "MaxHourlyVolFast",
    "MinuteVolDeltaMin",
    "MinuteVolDeltaMax",
    "UseBV_SV_Filter",
    "BV_SV_FilterRatio",
    "BV_SV_FilterRatioMax",
    "IgnoreDelta",
    "Delta_3h_Min",
    "Delta_3h_Max",
    "Delta_24h_Min",
    "Delta_24h_Max",
    "Delta2_Type",
    "Delta2_Min",
    "Delta2_Max",
    "Delta3_Type",
    "Delta3_Min",
    "Delta3_Max",
    "Delta_BTC_Min",
    "Delta_BTC_Max",
    "Delta_BTC_24_Min",
    "Delta_BTC_24_Max",
    "Delta_BTC_5m_Min",
    "Delta_BTC_5m_Max",
    "Delta_BTC_1m_Min",
    "Delta_BTC_1m_Max",
    "Delta_Market_Min",
    "Delta_Market_Max",
    "Delta_Market_24_Min",
    "Delta_Market_24_Max",
    "FilterBy",
    "FilterMin",
    "FilterMax",
    "GlobalFilterPenalty",
    "DeltaSwitch",
    "TriggerKey",
    "TriggerKeyBuy",
    "TriggerKeyProfit",
    "TriggerKeyLoss",
    "ActiveTrigger",
    "ClearTriggersBelow",
    "ClearTriggersAbove",
    "ClearTriggerKeys",
    "TriggerAllMarkets",
    "TriggerByKey",
    "TriggerByAllKeys",
    "TriggerSeconds",
    "TriggerKeysBL",
    "TriggerSecondsBL",
    "SellByTriggerBL",
    "CancelByTriggerBL",
    "IgnoreSession",
    "SessionLevelsUSDT",
    "SessionStratMax",
    "SessionStratIncreaseMax",
    "SessionStratMin",
    "SessionStratReduceMin",
    "SessionResetOnMinus",
    "SessionPenaltyTime",
    "SessionPlusCount",
    "SessionMinusCount",
    "SessionIncreaseOrder",
    "SessionIncreaseOrderMax",
    "SessionReduceOrder",
    "SessionReduceOrderMin",
    "SessionResetTime",
    "AutoBuy",
    "RunDetectOnKernel",
    "BuyDelay",
    "Short",
    "HFT",
    "MaxActiveOrders",
    "MaxOrdersPerMarket",
    "MaxMarkets",
    "AutoCancelBuy",
    "AutoCancelLowerBuy",
    "CancelBuyAfterSell",
    "BuyType",
    "PendingOrderSpread",
    "OrderSize",
    "MinFreeBalance",
    "buyPrice",
    "buyPriceLastTrade",
    "buyPriceAbsolute",
    "Use30SecOldASK",
    "UseOldPrice",
    "TlgUseBuyDipWords",
    "TlgBuyDipPrice",
    "UsePostOnly",
    "BuyModifier",
    "SellModifier",
    "DetectModifier",
    "StopLossModifier",
    "MaxModifier",
    "Add24hDelta",
    "Add3hDelta",
    "AddHourlyDelta",
    "Add15minDelta",
    "Add5minDelta",
    "Add1minDelta",
    "AddMarketDelta",
    "AddMarket24Delta",
    "AddBTCDelta",
    "AddBTC5mDelta",
    "AddBTC1mDelta",
    "AddMarkDelta",
    "AddPump1h",
    "AddDump1h",
    "AddPriceBug",
    "OrdersCount",
    "CheckFreeBalance",
    "BuyPriceStep",
    "BuyStepKind",
    "OrderSizeStep",
    "OrderSizeKind",
    "CancelBuyStep",
    "JoinSellKey",
    "JoinPriceFixed",
    "IgnoreCancelBuy",
    "AutoSplitBuy",
    "AutoSell",
    "SellPrice",
    "SellDelay",
    "SplitPiece",
    "UseMarketStop",
    "MarketStopLevel",
    "SellPriceAbsolute",
    "SellFromAsset",
    "SellQuantity",
    "PriceDownTimer",
    "PriceDownDelay",
    "PriceDownPercent",
    "PriceDownRelative",
    "PriceDownAllowedDrop",
    "UseScalpingMode",
    "SellByFilters",
    "SellByCustomEMA",
    "SellEMADelay",
    "SellEMACheckEnter",
    "SellLevelDelay",
    "SellLevelDelayNext",
    "SellLevelWorkTime",
    "SellLevelTime",
    "SellLevelCount",
    "SellLevelAdjust",
    "SellLevelRelative",
    "SellLevelAllowedDrop",
    "IgnoreSellShot",
    "SellShotDelay",
    "SellShotDistance",
    "SellShotCorridor",
    "SellShotCalcInterval",
    "SellShotRaiseWait",
    "SellShotReplaceDelay",
    "SellShotPriceDown",
    "SellShotPriceDownDelay",
    "SellShotAllowedUp",
    "SellShotAllowedDown",
    "IgnoreSellSpread",
    "SellSpreadReplaceCount",
    "SellSpreadMinSpread",
    "SellSpreadDelay",
    "SellSpreadDistance",
    "SellSpreadAllowedDrop",
    "UseSignalStops",
    "UseStopLoss",
    "FastStopLoss",
    "UseMarketOrder",
    "StopLossEMA",
    "StopLossDelay",
    "StopLoss",
    "StopLossSpread",
    "StopSpreadAdd1mDelta",
    "AllowedDrop",
    "DontSellBelowLiq",
    "StopAboveLiq",
    "StopLossFixed",
    "UseSecondStop",
    "TimeToSwitch2Stop",
    "PriceToSwitch2Stop",
    "SecondStopLoss",
    "UseStopLoss3",
    "TimeToSwitchStop3",
    "PriceToSwitchStop3",
    "StopLoss3",
    "AllowedDrop3",
    "UseTrailing",
    "TrailingPercent",
    "TrailingSpread",
    "TrailingEMA",
    "UseTakeProfit",
    "TakeProfit",
    "UseBV_SV_Stop",
    "BV_SV_Kind",
    "BV_SV_TradesN",
    "BV_SV_Ratio",
    "BV_SV_Reverse",
    "BV_SV_TakeProfit",
    "PanicSellDelisted",
    "DropsMaxTime",
    "DropsPriceMA",
    "DropsLastPriceMA",
    "DropsPriceDelta",
    "DropsPriceIsLow",
    "DropsUseLastPrice",
    "WallsMaxTime",
    "WallsPriceDelta",
    "WallBuyVolDeep",
    "WallBuyVolume",
    "WallBuyVolToDailyVol",
    "WallSellVolToBuy",
    "WallSellVolDeep",
    "PumpPriceInterval",
    "PumpPriceRaise",
    "PumpBuysPerSec",
    "PumpVolPerSec",
    "PumpVolEMA",
    "PumpMoveTimer",
    "PumpMovePersent",
    "PumpUsePrevBuyPrice",
    "MShotPriceMin",
    "MShotPrice",
    "MShotMinusSatoshi",
    "MShotAdd24hDelta",
    "MShotAdd3hDelta",
    "MShotAddHourlyDelta",
    "MShotAdd15minDelta",
    "MShotAdd5minDelta",
    "MShotAdd1minDelta",
    "MShotAddMarketDelta",
    "MShotAddBTCDelta",
    "MShotAddBTC5mDelta",
    "MShotAddDistance",
    "MShotAddMarkDelta",
    "MShotAddPriceBug",
    "MShotSellAtLastPrice",
    "MShotSellPriceAdjust",
    "MShotReplaceDelay",
    "MShotRaiseWait",
    "MShotSortBy",
    "MShotSortDesc",
    "MShotUsePrice",
    "MShotRepeatAfterBuy",
    "MShotRepeatIfProfit",
    "MShotRepeatIfDrop",
    "MShotRepeatWait",
    "MShotRepeatDelay",
    "VolShortInterval",
    "VolShortPriseRaise",
    "VolLongInterval",
    "VolBvShortToLong",
    "VolBvLongToHourlyMin",
    "VolBvLongToHourlyMax",
    "VolBvLongToDailyMin",
    "VolBvLongToDailyMax",
    "VolBvToSvShort",
    "VolBvShort",
    "VolSvLong",
    "VolTakeLongMaxP",
    "VolAtMinP",
    "VolAtMaxP",
    "VolDeltaAtMaxP",
    "VolDeltaAtMinP",
    "volBidsDeep",
    "volBids",
    "volAsksDeep",
    "volBidsToAsks",
    "VLiteT0",
    "VLiteT1",
    "VLiteT2",
    "VLiteT3",
    "VLiteP1",
    "VLiteP2",
    "VLiteP3",
    "VLiteMaxP",
    "VLitePDelta1",
    "VLitePDelta2",
    "VLiteDelta0",
    "VLiteMaxSpike",
    "VLiteV1",
    "VLiteV2",
    "VLiteV3",
    "VLiteWeightedAvg",
    "VLiteReducedVolumes",
    "WavesT0",
    "WavesT1",
    "WavesT2",
    "WavesT3",
    "WavesP1",
    "WavesP2",
    "WavesP3",
    "WavesDelta0",
    "WavesMaxSpike",
    "WavesV1",
    "WavesV2",
    "WavesV3",
    "WavesWeightedAvg",
    "WavesReducedVolumes",
    "DeltaInterval",
    "DeltaShortInterval",
    "DeltaPrice",
    "DeltaVol",
    "DeltaVolRaise",
    "DeltaVolSec",
    "DeltaLastPrice",
    "ComboStart",
    "ComboEnd",
    "ComboDelayMin",
    "ComboDelayMax",
    "MStrikeDepth",
    "MStrikeVolume",
    "MStrikeLastBidEMA",
    "MStrikeAddHourlyDelta",
    "MStrikeAdd15minDelta",
    "MStrikeAddMarketDelta",
    "MStrikeAddBTCDelta",
    "MStrikeBuyDelay",
    "MStrikeBuyLevel",
    "MStrikeBuyRelative",
    "MStrikeSellLevel",
    "MStrikeSellAdjust",
    "MStrikeDirection",
    "MStrikeWaitDip",
    "TMBuyPriceLimit",
    "LiqTime",
    "LiqCount",
    "LiqVolumeMin",
    "LiqVolumeMax",
    "LiqWaitTime",
    "LiqWithinTime",
    "LiqDirection",
    "LiqSameDirection",
    "Liq_BV_SV_Time",
    "Liq_BV_SV_Filter",
    "DeltaMin",
    "TMSameDirection",
    "StrategyPenalty",
    "TimeInterval",
    "TradesDensity",
    "TradesDensityPrev",
    "TradesCountMin",
    "PriceIntervals",
    "PriceIntervalShift",
    "PriceSpread",
    "PriceSpreadMax",
    "IntervalsForBuySpread",
    "BuyPriceInSpread",
    "SellPriceInSpread",
    "BuyOrderReduce",
    "MinReducedSize",
    "SpreadRepeatIfProfit",
    "SpreadFlat",
    "Spread_BV_SV_Time",
    "Spread_BV_SV_Max",
    "Spread_BV_SV_Min",
    "SpreadPolarityMin",
    "SpreadPolarityMax",
    "WallSpread",
    "WallSize",
    "RedWall",
    "MultiTokens",
    "HookTimeFrame",
    "HookDetectDepth",
    "HookDetectDepthMax",
    "HookAntiPump",
    "HookPriceRollBack",
    "HookPriceRollBackMax",
    "HookRollBackWait",
    "HookDropMin",
    "HookDropMax",
    "HookDirection",
    "HookOppositeOrder",
    "HookInterpolate",
    "HookInitialPrice",
    "HookPriceDistance",
    "HookPartFilledDelay",
    "HookSellLevel",
    "HookSellFixed",
    "HookReplaceDelay",
    "HookRaiseWait",
    "HookRepeatAfterSell",
    "HookRepeatIfProfit",
    "HookDetectMinVolume",
    "FastShotAlgo",
    "MMTimeFrame",
    "MMOrderMin",
    "MMOrderMax",
    "MMOrderStep",
    "UseHookStrategy",
    "AlertByTrades",
    "WatchAddress",
    "WatchDirection",
    "WatchMinVolume",
    "WatchMinPosition",
];
const DELPHI_STRATEGY_FIELD_TYPES: &[u8] = &[
    TID_STRING, TID_STRING, TID_STRING, TID_STRING, TID_STRING, TID_STRING, TID_BOOL, TID_BOOL,
    TID_BOOL, TID_BOOL, TID_BOOL, TID_BOOL, TID_STRING, TID_INT32, TID_INT32, TID_INT32, TID_BOOL,
    TID_BOOL, TID_BOOL, TID_BOOL, TID_BOOL, TID_BOOL, TID_STRING, TID_STRING, TID_STRING,
    TID_STRING, TID_BOOL, TID_INT32, TID_STRING, TID_BOOL, TID_INT32, TID_INT32, TID_BOOL,
    TID_BOOL, TID_STRING, TID_STRING, TID_INT32, TID_INT32, TID_BOOL, TID_STRING, TID_BOOL,
    TID_BOOL, TID_INT32, TID_INT64, TID_BOOL, TID_STRING, TID_INT32, TID_INT32, TID_STRING,
    TID_INT32, TID_INT32, TID_DOUBLE, TID_DOUBLE, TID_BOOL, TID_STRING, TID_INT32, TID_INT32,
    TID_INT32, TID_INT32, TID_INT32, TID_BOOL, TID_DOUBLE, TID_BOOL, TID_DOUBLE, TID_INT32,
    TID_INT32, TID_DOUBLE, TID_DOUBLE, TID_DOUBLE, TID_DOUBLE, TID_DOUBLE, TID_BOOL, TID_BOOL,
    TID_INT32, TID_INT32, TID_INT32, TID_DOUBLE, TID_DOUBLE, TID_BOOL, TID_INT64, TID_INT64,
    TID_INT64, TID_INT64, TID_INT32, TID_INT64, TID_DOUBLE, TID_DOUBLE, TID_BOOL, TID_DOUBLE,
    TID_DOUBLE, TID_BOOL, TID_DOUBLE, TID_DOUBLE, TID_DOUBLE, TID_DOUBLE, TID_STRING, TID_DOUBLE,
    TID_DOUBLE, TID_STRING, TID_DOUBLE, TID_DOUBLE, TID_DOUBLE, TID_DOUBLE, TID_DOUBLE, TID_DOUBLE,
    TID_DOUBLE, TID_DOUBLE, TID_DOUBLE, TID_DOUBLE, TID_DOUBLE, TID_DOUBLE, TID_DOUBLE, TID_DOUBLE,
    TID_STRING, TID_DOUBLE, TID_DOUBLE, TID_INT32, TID_DOUBLE, TID_INT32, TID_INT32, TID_INT32,
    TID_INT32, TID_BOOL, TID_BOOL, TID_BOOL, TID_STRING, TID_BOOL, TID_STRING, TID_BOOL, TID_INT32,
    TID_STRING, TID_INT32, TID_STRING, TID_BOOL, TID_BOOL, TID_BOOL, TID_DOUBLE, TID_INT32,
    TID_DOUBLE, TID_INT32, TID_BOOL, TID_INT32, TID_INT32, TID_INT32, TID_INT32, TID_INT32,
    TID_INT32, TID_INT32, TID_INT32, TID_BOOL, TID_BOOL, TID_INT32, TID_BOOL, TID_INT32, TID_INT32,
    TID_INT32, TID_INT32, TID_DOUBLE, TID_INT32, TID_BOOL, TID_STRING, TID_DOUBLE, TID_DOUBLE,
    TID_DOUBLE, TID_DOUBLE, TID_BOOL, TID_BOOL, TID_BOOL, TID_INT32, TID_BOOL, TID_DOUBLE,
    TID_BOOL, TID_DOUBLE, TID_DOUBLE, TID_DOUBLE, TID_DOUBLE, TID_DOUBLE, TID_DOUBLE, TID_DOUBLE,
    TID_DOUBLE, TID_DOUBLE, TID_DOUBLE, TID_DOUBLE, TID_DOUBLE, TID_DOUBLE, TID_DOUBLE, TID_DOUBLE,
    TID_DOUBLE, TID_DOUBLE, TID_DOUBLE, TID_DOUBLE, TID_DOUBLE, TID_INT32, TID_BOOL, TID_DOUBLE,
    TID_STRING, TID_DOUBLE, TID_STRING, TID_INT32, TID_INT32, TID_BOOL, TID_BOOL, TID_BOOL,
    TID_BOOL, TID_DOUBLE, TID_INT32, TID_INT32, TID_BOOL, TID_DOUBLE, TID_BOOL, TID_BOOL,
    TID_DOUBLE, TID_DOUBLE, TID_DOUBLE, TID_DOUBLE, TID_BOOL, TID_DOUBLE, TID_BOOL, TID_INT32,
    TID_STRING, TID_INT32, TID_BOOL, TID_INT32, TID_INT32, TID_INT32, TID_INT32, TID_INT32,
    TID_DOUBLE, TID_BOOL, TID_DOUBLE, TID_BOOL, TID_DOUBLE, TID_DOUBLE, TID_INT32, TID_DOUBLE,
    TID_DOUBLE, TID_DOUBLE, TID_DOUBLE, TID_DOUBLE, TID_DOUBLE, TID_DOUBLE, TID_BOOL, TID_INT32,
    TID_DOUBLE, TID_DOUBLE, TID_DOUBLE, TID_DOUBLE, TID_BOOL, TID_BOOL, TID_BOOL, TID_BOOL,
    TID_INT32, TID_DOUBLE, TID_DOUBLE, TID_DOUBLE, TID_DOUBLE, TID_DOUBLE, TID_BOOL, TID_DOUBLE,
    TID_BOOL, TID_BOOL, TID_INT32, TID_DOUBLE, TID_DOUBLE, TID_BOOL, TID_INT32, TID_DOUBLE,
    TID_DOUBLE, TID_DOUBLE, TID_BOOL, TID_DOUBLE, TID_DOUBLE, TID_INT32, TID_BOOL, TID_DOUBLE,
    TID_BOOL, TID_STRING, TID_INT32, TID_DOUBLE, TID_BOOL, TID_DOUBLE, TID_BOOL, TID_INT32,
    TID_INT32, TID_INT32, TID_DOUBLE, TID_BOOL, TID_BOOL, TID_INT32, TID_DOUBLE, TID_DOUBLE,
    TID_INT32, TID_DOUBLE, TID_DOUBLE, TID_DOUBLE, TID_STRING, TID_DOUBLE, TID_INT32, TID_DOUBLE,
    TID_DOUBLE, TID_INT32, TID_DOUBLE, TID_BOOL, TID_DOUBLE, TID_DOUBLE, TID_BOOL, TID_DOUBLE,
    TID_DOUBLE, TID_DOUBLE, TID_DOUBLE, TID_DOUBLE, TID_DOUBLE, TID_DOUBLE, TID_DOUBLE, TID_DOUBLE,
    TID_DOUBLE, TID_DOUBLE, TID_DOUBLE, TID_BOOL, TID_DOUBLE, TID_DOUBLE, TID_DOUBLE, TID_STRING,
    TID_BOOL, TID_STRING, TID_BOOL, TID_DOUBLE, TID_DOUBLE, TID_INT32, TID_INT32, TID_INT32,
    TID_DOUBLE, TID_INT32, TID_DOUBLE, TID_DOUBLE, TID_DOUBLE, TID_DOUBLE, TID_DOUBLE, TID_DOUBLE,
    TID_DOUBLE, TID_DOUBLE, TID_BOOL, TID_DOUBLE, TID_DOUBLE, TID_DOUBLE, TID_DOUBLE, TID_DOUBLE,
    TID_DOUBLE, TID_DOUBLE, TID_DOUBLE, TID_INT32, TID_INT32, TID_INT32, TID_INT32, TID_DOUBLE,
    TID_DOUBLE, TID_DOUBLE, TID_DOUBLE, TID_DOUBLE, TID_DOUBLE, TID_DOUBLE, TID_DOUBLE, TID_DOUBLE,
    TID_DOUBLE, TID_DOUBLE, TID_BOOL, TID_BOOL, TID_INT32, TID_INT32, TID_INT32, TID_INT32,
    TID_DOUBLE, TID_DOUBLE, TID_DOUBLE, TID_DOUBLE, TID_DOUBLE, TID_DOUBLE, TID_DOUBLE, TID_DOUBLE,
    TID_BOOL, TID_BOOL, TID_INT32, TID_INT32, TID_DOUBLE, TID_DOUBLE, TID_DOUBLE, TID_DOUBLE,
    TID_DOUBLE, TID_STRING, TID_STRING, TID_INT32, TID_INT32, TID_DOUBLE, TID_DOUBLE, TID_INT32,
    TID_DOUBLE, TID_DOUBLE, TID_DOUBLE, TID_DOUBLE, TID_INT32, TID_DOUBLE, TID_BOOL, TID_DOUBLE,
    TID_BOOL, TID_STRING, TID_BOOL, TID_DOUBLE, TID_INT32, TID_INT32, TID_INT32, TID_INT32,
    TID_INT32, TID_INT32, TID_STRING, TID_BOOL, TID_INT32, TID_DOUBLE, TID_DOUBLE, TID_BOOL,
    TID_INT32, TID_DOUBLE, TID_INT32, TID_INT32, TID_INT32, TID_INT32, TID_INT32, TID_DOUBLE,
    TID_DOUBLE, TID_INT32, TID_INT32, TID_INT32, TID_INT32, TID_DOUBLE, TID_INT32, TID_BOOL,
    TID_INT32, TID_DOUBLE, TID_DOUBLE, TID_INT32, TID_INT32, TID_DOUBLE, TID_DOUBLE, TID_BOOL,
    TID_BOOL, TID_DOUBLE, TID_DOUBLE, TID_DOUBLE, TID_BOOL, TID_INT32, TID_INT32, TID_INT32,
    TID_INT32, TID_INT32, TID_STRING, TID_BOOL, TID_INT32, TID_INT32, TID_INT32, TID_INT32,
    TID_INT32, TID_BOOL, TID_DOUBLE, TID_DOUBLE, TID_BOOL, TID_DOUBLE, TID_DOUBLE, TID_BOOL,
    TID_INT32, TID_INT32, TID_INT32, TID_INT32, TID_STRING, TID_BOOL, TID_STRING, TID_STRING,
    TID_INT32, TID_INT32,
];
fn strategy_field_order_rank(name: &str) -> usize {
    DELPHI_STRATEGY_FIELD_ORDER
        .iter()
        .position(|field| *field == name)
        .unwrap_or(usize::MAX)
}

fn strategy_field_expected_type_id(name: &str) -> Option<u8> {
    DELPHI_STRATEGY_FIELD_TYPES
        .get(strategy_field_order_rank(name))
        .copied()
}

fn strategy_field_should_write(name: &str, value: &FieldValue) -> bool {
    let Some(expected_type) = strategy_field_expected_type_id(name) else {
        return false;
    };
    if value.type_id() != expected_type {
        return false;
    }
    !strategy_field_value_is_delphi_default(name, value)
}

fn strategy_field_value_is_delphi_default(name: &str, value: &FieldValue) -> bool {
    match name {
        // Delphi defaults for these two strings are `IntToHex(Vars.*OrderColor)`;
        // the exact value is runtime UI state, so Rust must not treat "" as default.
        "SellOrderColor" | "BuyOrderColor" => false,
        "SignalType" => matches!(value, FieldValue::String(v) if v == "DropsDetection"),
        "ReportTradesToTelegram" => matches!(value, FieldValue::Bool(v) if *v),
        "SoundKind" => matches!(value, FieldValue::String(v) if v == "TurnOn"),
        "KeepAlert" => matches!(value, FieldValue::Int32(v) if *v == 60),
        "OrderLineKind" => matches!(value, FieldValue::String(v) if v == "Solid"),
        "DynWL_SortBy" => matches!(value, FieldValue::String(v) if v == "Last2hDelta"),
        "DynWL_SortDesc" => matches!(value, FieldValue::Bool(v) if *v),
        "DynBL_SortBy" => matches!(value, FieldValue::String(v) if v == "Last2hDelta"),
        "DynBL_SortDesc" => matches!(value, FieldValue::Bool(v) if *v),
        "Dyn_Refresh" => matches!(value, FieldValue::Int32(v) if *v == 61),
        "ListedType" => matches!(value, FieldValue::String(v) if v == "Ignore"),
        "NextDetectPenalty" => matches!(value, FieldValue::Int32(v) if *v == 30),
        "MinLeverage" => matches!(value, FieldValue::Int32(v) if *v == 1),
        "MoonIntRiskLevel" => matches!(value, FieldValue::Int32(v) if *v == 2),
        "MoonIntStopLevel" => matches!(value, FieldValue::Int32(v) if *v == 4),
        "MarkPriceMax" => matches!(value, FieldValue::Double(v) if (*v - 1.0).abs() < 1e-10),
        "PenaltyTime" => matches!(value, FieldValue::Int32(v) if *v == 300),
        "PriceStepMax" => matches!(value, FieldValue::Double(v) if (*v - 0.5).abs() < 1e-10),
        "MinVolume" => matches!(value, FieldValue::Int64(v) if *v == 100),
        "MaxVolume" => matches!(value, FieldValue::Int64(v) if *v == 100000),
        "MinHourlyVolume" => matches!(value, FieldValue::Int64(v) if *v == 10),
        "MaxHourlyVolume" => matches!(value, FieldValue::Int64(v) if *v == 10000),
        "BV_SV_FilterRatio" => matches!(value, FieldValue::Double(v) if (*v - 1.0).abs() < 1e-10),
        "Delta_3h_Max" => matches!(value, FieldValue::Double(v) if (*v - 20.0).abs() < 1e-10),
        "Delta_24h_Max" => matches!(value, FieldValue::Double(v) if (*v - 100.0).abs() < 1e-10),
        "Delta2_Type" => matches!(value, FieldValue::String(v) if v == "1h"),
        "Delta2_Max" => matches!(value, FieldValue::Double(v) if (*v - 20.0).abs() < 1e-10),
        "Delta3_Type" => matches!(value, FieldValue::String(v) if v == "1m"),
        "Delta3_Max" => matches!(value, FieldValue::Double(v) if (*v - 1000.0).abs() < 1e-10),
        "Delta_BTC_Min" => matches!(value, FieldValue::Double(v) if (*v - -5.0).abs() < 1e-10),
        "Delta_BTC_Max" => matches!(value, FieldValue::Double(v) if (*v - 5.0).abs() < 1e-10),
        "Delta_BTC_24_Min" => matches!(value, FieldValue::Double(v) if (*v - -10.0).abs() < 1e-10),
        "Delta_BTC_24_Max" => matches!(value, FieldValue::Double(v) if (*v - 10.0).abs() < 1e-10),
        "Delta_BTC_5m_Max" => matches!(value, FieldValue::Double(v) if (*v - 10.0).abs() < 1e-10),
        "Delta_BTC_1m_Max" => matches!(value, FieldValue::Double(v) if (*v - 100.0).abs() < 1e-10),
        "Delta_Market_Min" => matches!(value, FieldValue::Double(v) if (*v - -5.0).abs() < 1e-10),
        "Delta_Market_Max" => matches!(value, FieldValue::Double(v) if (*v - 10.0).abs() < 1e-10),
        "Delta_Market_24_Min" => {
            matches!(value, FieldValue::Double(v) if (*v - -10.0).abs() < 1e-10)
        }
        "Delta_Market_24_Max" => {
            matches!(value, FieldValue::Double(v) if (*v - 10.0).abs() < 1e-10)
        }
        "FilterBy" => matches!(value, FieldValue::String(v) if v == "Last2hDelta"),
        "TriggerSeconds" => matches!(value, FieldValue::Int32(v) if *v == 60),
        "IgnoreSession" => matches!(value, FieldValue::Bool(v) if *v),
        "SessionLevelsUSDT" => matches!(value, FieldValue::Bool(v) if *v),
        "SessionIncreaseOrderMax" => matches!(value, FieldValue::Int32(v) if *v == 500),
        "SessionReduceOrderMin" => matches!(value, FieldValue::Int32(v) if *v == 500),
        "MaxActiveOrders" => matches!(value, FieldValue::Int32(v) if *v == 10),
        "MaxOrdersPerMarket" => matches!(value, FieldValue::Int32(v) if *v == 1),
        "AutoCancelBuy" => matches!(value, FieldValue::Double(v) if (*v - 300.0).abs() < 1e-10),
        "BuyType" => matches!(value, FieldValue::String(v) if v == "Buy"),
        "PendingOrderSpread" => matches!(value, FieldValue::Double(v) if (*v - 0.1).abs() < 1e-10),
        "DetectModifier" => matches!(value, FieldValue::Double(v) if (*v - 0.5).abs() < 1e-10),
        "AddPriceBug" => matches!(value, FieldValue::Double(v) if (*v - 0.1).abs() < 1e-10),
        "OrdersCount" => matches!(value, FieldValue::Int32(v) if *v == 1),
        "BuyPriceStep" => matches!(value, FieldValue::Double(v) if (*v - -1.5).abs() < 1e-10),
        "BuyStepKind" => matches!(value, FieldValue::String(v) if v == "Linear"),
        "OrderSizeStep" => matches!(value, FieldValue::Double(v) if (*v - 25.0).abs() < 1e-10),
        "OrderSizeKind" => matches!(value, FieldValue::String(v) if v == "Linear"),
        "AutoSell" => matches!(value, FieldValue::Bool(v) if *v),
        "SellPrice" => matches!(value, FieldValue::Double(v) if (*v - 0.5).abs() < 1e-10),
        "PriceDownDelay" => matches!(value, FieldValue::Double(v) if (*v - 10.0).abs() < 1e-10),
        "PriceDownPercent" => matches!(value, FieldValue::Double(v) if (*v - 0.2).abs() < 1e-10),
        "PriceDownAllowedDrop" => {
            matches!(value, FieldValue::Double(v) if (*v - 0.1).abs() < 1e-10)
        }
        "SellEMACheckEnter" => matches!(value, FieldValue::Bool(v) if *v),
        "SellLevelTime" => matches!(value, FieldValue::Int32(v) if *v == 3600),
        "SellLevelCount" => matches!(value, FieldValue::Int32(v) if *v == 1),
        "SellLevelAdjust" => matches!(value, FieldValue::Double(v) if (*v - 0.2).abs() < 1e-10),
        "SellLevelAllowedDrop" => {
            matches!(value, FieldValue::Double(v) if (*v - 0.1).abs() < 1e-10)
        }
        "IgnoreSellShot" => matches!(value, FieldValue::Bool(v) if *v),
        "SellShotDistance" => matches!(value, FieldValue::Double(v) if (*v - 0.1).abs() < 1e-10),
        "SellShotCorridor" => matches!(value, FieldValue::Int32(v) if *v == 50),
        "SellShotCalcInterval" => {
            matches!(value, FieldValue::Double(v) if (*v - 0.6).abs() < 1e-10)
        }
        "SellShotRaiseWait" => matches!(value, FieldValue::Double(v) if (*v - 0.2).abs() < 1e-10),
        "SellShotReplaceDelay" => {
            matches!(value, FieldValue::Double(v) if (*v - 0.1).abs() < 1e-10)
        }
        "SellShotAllowedUp" => matches!(value, FieldValue::Double(v) if (*v - 10.0).abs() < 1e-10),
        "SellShotAllowedDown" => {
            matches!(value, FieldValue::Double(v) if (*v - -0.1).abs() < 1e-10)
        }
        "IgnoreSellSpread" => matches!(value, FieldValue::Bool(v) if *v),
        "SellSpreadReplaceCount" => matches!(value, FieldValue::Int32(v) if *v == 10),
        "SellSpreadMinSpread" => matches!(value, FieldValue::Double(v) if (*v - 0.5).abs() < 1e-10),
        "SellSpreadDistance" => {
            matches!(value, FieldValue::Double(v) if (*v - -10.0).abs() < 1e-10)
        }
        "SellSpreadAllowedDrop" => {
            matches!(value, FieldValue::Double(v) if (*v - 0.3).abs() < 1e-10)
        }
        "UseStopLoss" => matches!(value, FieldValue::Bool(v) if *v),
        "StopLoss" => matches!(value, FieldValue::Double(v) if (*v - -5.0).abs() < 1e-10),
        "StopLossSpread" => matches!(value, FieldValue::Double(v) if (*v - 1.0).abs() < 1e-10),
        "StopSpreadAdd1mDelta" => {
            matches!(value, FieldValue::Double(v) if (*v - 0.01).abs() < 1e-10)
        }
        "AllowedDrop" => matches!(value, FieldValue::Double(v) if (*v - -99.0).abs() < 1e-10),
        "TimeToSwitch2Stop" => matches!(value, FieldValue::Int32(v) if *v == 60),
        "TimeToSwitchStop3" => matches!(value, FieldValue::Int32(v) if *v == 60),
        "PriceToSwitchStop3" => {
            matches!(value, FieldValue::Double(v) if (*v - -10.0).abs() < 1e-10)
        }
        "AllowedDrop3" => matches!(value, FieldValue::Double(v) if (*v - -99.0).abs() < 1e-10),
        "UseTrailing" => matches!(value, FieldValue::Bool(v) if *v),
        "TrailingPercent" => matches!(value, FieldValue::Double(v) if (*v - -3.0).abs() < 1e-10),
        "TrailingSpread" => matches!(value, FieldValue::Double(v) if (*v - 0.5).abs() < 1e-10),
        "UseTakeProfit" => matches!(value, FieldValue::Bool(v) if *v),
        "TakeProfit" => matches!(value, FieldValue::Double(v) if (*v - 1.0).abs() < 1e-10),
        "BV_SV_Kind" => matches!(value, FieldValue::String(v) if v == "TradesCount"),
        "BV_SV_TradesN" => matches!(value, FieldValue::Int32(v) if *v == 100),
        "BV_SV_Ratio" => matches!(value, FieldValue::Double(v) if (*v - 0.75).abs() < 1e-10),
        "BV_SV_TakeProfit" => matches!(value, FieldValue::Double(v) if (*v - -1.0).abs() < 1e-10),
        "PumpPriceInterval" => matches!(value, FieldValue::String(v) if v == "30s"),
        "PumpPriceRaise" => matches!(value, FieldValue::Double(v) if (*v - 7.0).abs() < 1e-10),
        "PumpBuysPerSec" => matches!(value, FieldValue::Int32(v) if *v == 15),
        "PumpVolPerSec" => matches!(value, FieldValue::Double(v) if (*v - 0.8).abs() < 1e-10),
        "PumpVolEMA" => matches!(value, FieldValue::Double(v) if (*v - 2.0).abs() < 1e-10),
        "PumpMovePersent" => matches!(value, FieldValue::Double(v) if (*v - 0.5).abs() < 1e-10),
        "PumpUsePrevBuyPrice" => matches!(value, FieldValue::Bool(v) if *v),
        "MShotPriceMin" => matches!(value, FieldValue::Double(v) if (*v - 2.0).abs() < 1e-10),
        "MShotPrice" => matches!(value, FieldValue::Double(v) if (*v - 7.0).abs() < 1e-10),
        "MShotMinusSatoshi" => matches!(value, FieldValue::Bool(v) if *v),
        "MShotAddHourlyDelta" => matches!(value, FieldValue::Double(v) if (*v - 0.1).abs() < 1e-10),
        "MShotAddPriceBug" => matches!(value, FieldValue::Double(v) if (*v - 0.1).abs() < 1e-10),
        "MShotSellAtLastPrice" => matches!(value, FieldValue::Bool(v) if *v),
        "MShotRaiseWait" => matches!(value, FieldValue::Double(v) if (*v - 30.0).abs() < 1e-10),
        "MShotSortBy" => matches!(value, FieldValue::String(v) if v == "Last2hDelta"),
        "MShotSortDesc" => matches!(value, FieldValue::Bool(v) if *v),
        "MShotUsePrice" => matches!(value, FieldValue::String(v) if v == "Trade"),
        "MShotRepeatWait" => matches!(value, FieldValue::Int32(v) if *v == 5),
        "VolShortInterval" => matches!(value, FieldValue::Int32(v) if *v == 45),
        "VolShortPriseRaise" => matches!(value, FieldValue::Double(v) if (*v - 1.0).abs() < 1e-10),
        "VolLongInterval" => matches!(value, FieldValue::Int32(v) if *v == 300),
        "VolBvShortToLong" => matches!(value, FieldValue::Double(v) if (*v - 0.2).abs() < 1e-10),
        "VolBvLongToHourlyMin" => {
            matches!(value, FieldValue::Double(v) if (*v - 0.2).abs() < 1e-10)
        }
        "VolBvLongToHourlyMax" => {
            matches!(value, FieldValue::Double(v) if (*v - 2.0).abs() < 1e-10)
        }
        "VolBvLongToDailyMin" => matches!(value, FieldValue::Double(v) if (*v - 0.2).abs() < 1e-10),
        "VolBvLongToDailyMax" => matches!(value, FieldValue::Double(v) if (*v - 2.0).abs() < 1e-10),
        "VolBvToSvShort" => matches!(value, FieldValue::Double(v) if (*v - 2.0).abs() < 1e-10),
        "VolBvShort" => matches!(value, FieldValue::Double(v) if (*v - 0.5).abs() < 1e-10),
        "VolSvLong" => matches!(value, FieldValue::Double(v) if (*v - 4.0).abs() < 1e-10),
        "VolAtMinP" => matches!(value, FieldValue::Double(v) if (*v - 0.5).abs() < 1e-10),
        "VolAtMaxP" => matches!(value, FieldValue::Double(v) if (*v - 0.1).abs() < 1e-10),
        "VolDeltaAtMaxP" => matches!(value, FieldValue::Double(v) if (*v - 0.1).abs() < 1e-10),
        "VolDeltaAtMinP" => matches!(value, FieldValue::Double(v) if (*v - 0.2).abs() < 1e-10),
        "volBidsDeep" => matches!(value, FieldValue::Double(v) if (*v - 3.0).abs() < 1e-10),
        "volBids" => matches!(value, FieldValue::Double(v) if (*v - 0.5).abs() < 1e-10),
        "volAsksDeep" => matches!(value, FieldValue::Double(v) if (*v - 4.0).abs() < 1e-10),
        "volBidsToAsks" => matches!(value, FieldValue::Double(v) if (*v - 1.5).abs() < 1e-10),
        "VLiteT0" => matches!(value, FieldValue::Int32(v) if *v == 300),
        "VLiteT1" => matches!(value, FieldValue::Int32(v) if *v == 180),
        "VLiteT2" => matches!(value, FieldValue::Int32(v) if *v == 180),
        "VLiteT3" => matches!(value, FieldValue::Int32(v) if *v == 180),
        "VLiteP1" => matches!(value, FieldValue::Double(v) if (*v - 1.0).abs() < 1e-10),
        "VLiteP2" => matches!(value, FieldValue::Double(v) if (*v - 1.0).abs() < 1e-10),
        "VLiteP3" => matches!(value, FieldValue::Double(v) if (*v - 1.0).abs() < 1e-10),
        "VLiteMaxP" => matches!(value, FieldValue::Double(v) if (*v - 5.0).abs() < 1e-10),
        "VLiteDelta0" => matches!(value, FieldValue::Double(v) if (*v - 2.0).abs() < 1e-10),
        "VLiteMaxSpike" => matches!(value, FieldValue::Double(v) if (*v - 7.0).abs() < 1e-10),
        "VLiteV1" => matches!(value, FieldValue::Double(v) if (*v - 1.0).abs() < 1e-10),
        "VLiteV2" => matches!(value, FieldValue::Double(v) if (*v - 1.0).abs() < 1e-10),
        "VLiteV3" => matches!(value, FieldValue::Double(v) if (*v - 1.0).abs() < 1e-10),
        "VLiteWeightedAvg" => matches!(value, FieldValue::Bool(v) if *v),
        "WavesT0" => matches!(value, FieldValue::Int32(v) if *v == 300),
        "WavesT1" => matches!(value, FieldValue::Int32(v) if *v == 180),
        "WavesT2" => matches!(value, FieldValue::Int32(v) if *v == 180),
        "WavesT3" => matches!(value, FieldValue::Int32(v) if *v == 180),
        "WavesP1" => matches!(value, FieldValue::Double(v) if (*v - 1.0).abs() < 1e-10),
        "WavesP2" => matches!(value, FieldValue::Double(v) if (*v - -1.0).abs() < 1e-10),
        "WavesP3" => matches!(value, FieldValue::Double(v) if (*v - 1.0).abs() < 1e-10),
        "WavesDelta0" => matches!(value, FieldValue::Double(v) if (*v - 2.0).abs() < 1e-10),
        "WavesMaxSpike" => matches!(value, FieldValue::Double(v) if (*v - 7.0).abs() < 1e-10),
        "WavesV1" => matches!(value, FieldValue::Double(v) if (*v - 1.0).abs() < 1e-10),
        "WavesV3" => matches!(value, FieldValue::Double(v) if (*v - 1.0).abs() < 1e-10),
        "WavesWeightedAvg" => matches!(value, FieldValue::Bool(v) if *v),
        "DeltaInterval" => matches!(value, FieldValue::Int32(v) if *v == 600),
        "DeltaShortInterval" => matches!(value, FieldValue::Int32(v) if *v == 5),
        "DeltaPrice" => matches!(value, FieldValue::Double(v) if (*v - 2.0).abs() < 1e-10),
        "DeltaVol" => matches!(value, FieldValue::Double(v) if (*v - 5.0).abs() < 1e-10),
        "DeltaVolRaise" => matches!(value, FieldValue::Double(v) if (*v - 100.0).abs() < 1e-10),
        "DeltaVolSec" => matches!(value, FieldValue::Double(v) if (*v - 0.1).abs() < 1e-10),
        "DeltaLastPrice" => matches!(value, FieldValue::Double(v) if (*v - 0.3).abs() < 1e-10),
        "ComboDelayMax" => matches!(value, FieldValue::Int32(v) if *v == 600),
        "MStrikeDepth" => matches!(value, FieldValue::Double(v) if (*v - 10.0).abs() < 1e-10),
        "MStrikeVolume" => matches!(value, FieldValue::Double(v) if (*v - 0.2).abs() < 1e-10),
        "MStrikeLastBidEMA" => matches!(value, FieldValue::Int32(v) if *v == 10),
        "MStrikeAdd15minDelta" => {
            matches!(value, FieldValue::Double(v) if (*v - 0.1).abs() < 1e-10)
        }
        "MStrikeAddMarketDelta" => {
            matches!(value, FieldValue::Double(v) if (*v - 1.0).abs() < 1e-10)
        }
        "MStrikeAddBTCDelta" => matches!(value, FieldValue::Double(v) if (*v - 0.5).abs() < 1e-10),
        "MStrikeBuyRelative" => matches!(value, FieldValue::Bool(v) if *v),
        "MStrikeSellLevel" => matches!(value, FieldValue::Double(v) if (*v - 80.0).abs() < 1e-10),
        "MStrikeDirection" => matches!(value, FieldValue::String(v) if v == "OnlyLong"),
        "TMBuyPriceLimit" => matches!(value, FieldValue::Double(v) if (*v - 500.0).abs() < 1e-10),
        "LiqTime" => matches!(value, FieldValue::Int32(v) if *v == 15),
        "LiqCount" => matches!(value, FieldValue::Int32(v) if *v == 5),
        "LiqVolumeMin" => matches!(value, FieldValue::Int32(v) if *v == 10000),
        "LiqWithinTime" => matches!(value, FieldValue::Int32(v) if *v == 5000),
        "LiqDirection" => matches!(value, FieldValue::String(v) if v == "Both"),
        "LiqSameDirection" => matches!(value, FieldValue::Bool(v) if *v),
        "Liq_BV_SV_Time" => matches!(value, FieldValue::Int32(v) if *v == 1500),
        "Liq_BV_SV_Filter" => matches!(value, FieldValue::Double(v) if (*v - 0.5).abs() < 1e-10),
        "TMSameDirection" => matches!(value, FieldValue::Bool(v) if *v),
        "StrategyPenalty" => matches!(value, FieldValue::Int32(v) if *v == 2),
        "TimeInterval" => matches!(value, FieldValue::Double(v) if (*v - 5.0).abs() < 1e-10),
        "TradesDensity" => matches!(value, FieldValue::Int32(v) if *v == 80),
        "TradesDensityPrev" => matches!(value, FieldValue::Int32(v) if *v == 20),
        "PriceIntervals" => matches!(value, FieldValue::Int32(v) if *v == 10),
        "PriceSpread" => matches!(value, FieldValue::Double(v) if (*v - 0.3).abs() < 1e-10),
        "IntervalsForBuySpread" => matches!(value, FieldValue::Int32(v) if *v == 3),
        "BuyPriceInSpread" => matches!(value, FieldValue::Int32(v) if *v == 20),
        "SellPriceInSpread" => matches!(value, FieldValue::Int32(v) if *v == 80),
        "BuyOrderReduce" => matches!(value, FieldValue::Int32(v) if *v == 100),
        "SpreadFlat" => matches!(value, FieldValue::Bool(v) if *v),
        "Spread_BV_SV_Time" => matches!(value, FieldValue::Int32(v) if *v == 3000),
        "SpreadPolarityMin" => matches!(value, FieldValue::Int32(v) if *v == -100),
        "SpreadPolarityMax" => matches!(value, FieldValue::Int32(v) if *v == 100),
        "HookTimeFrame" => matches!(value, FieldValue::Double(v) if (*v - 2.0).abs() < 1e-10),
        "HookDetectDepth" => matches!(value, FieldValue::Double(v) if (*v - 1.0).abs() < 1e-10),
        "HookPriceRollBack" => matches!(value, FieldValue::Int32(v) if *v == 75),
        "HookDirection" => matches!(value, FieldValue::String(v) if v == "OnlyLong"),
        "HookInitialPrice" => matches!(value, FieldValue::Int32(v) if *v == 10),
        "HookPriceDistance" => matches!(value, FieldValue::Int32(v) if *v == 25),
        "HookSellLevel" => matches!(value, FieldValue::Int32(v) if *v == 50),
        "HookRaiseWait" => matches!(value, FieldValue::Double(v) if (*v - 30.0).abs() < 1e-10),
        "FastShotAlgo" => matches!(value, FieldValue::Bool(v) if *v),
        "MMTimeFrame" => matches!(value, FieldValue::Int32(v) if *v == 45),
        "MMOrderMin" => matches!(value, FieldValue::Int32(v) if *v == 100),
        "MMOrderMax" => matches!(value, FieldValue::Int32(v) if *v == 1000),
        "MMOrderStep" => matches!(value, FieldValue::Int32(v) if *v == 1),
        "WatchDirection" => matches!(value, FieldValue::String(v) if v == "Both"),
        _ => value.is_zero(),
    }
}

// =============================================================================
//  FieldValue
// =============================================================================

/// Decoded поле стратегии. Соответствует Delphi `TValue` после RTTI-десериализации.
#[derive(Debug, Clone, PartialEq)]
pub enum FieldValue {
    Bool(bool),
    Int32(i32),
    Int64(i64),
    Double(f64),
    String(String),
    Byte(u8),
    Word(u16),
    UInt32(u32),
    UInt64(u64),
    Single(f32),
}

impl FieldValue {
    /// Zero значение для указанного TypeID. Используется когда установлен `TID_ZERO_FLAG`.
    pub fn zero(type_id: u8) -> Option<Self> {
        Some(match type_id & 0x7F {
            TID_BOOL => FieldValue::Bool(false),
            TID_INT32 => FieldValue::Int32(0),
            TID_INT64 => FieldValue::Int64(0),
            TID_DOUBLE => FieldValue::Double(0.0),
            TID_STRING => FieldValue::String(String::new()),
            TID_BYTE => FieldValue::Byte(0),
            TID_WORD => FieldValue::Word(0),
            TID_UINT32 => FieldValue::UInt32(0),
            TID_UINT64 => FieldValue::UInt64(0),
            TID_SINGLE => FieldValue::Single(0.0),
            _ => return None,
        })
    }

    pub fn type_id(&self) -> u8 {
        match self {
            FieldValue::Bool(_) => TID_BOOL,
            FieldValue::Int32(_) => TID_INT32,
            FieldValue::Int64(_) => TID_INT64,
            FieldValue::Double(_) => TID_DOUBLE,
            FieldValue::String(_) => TID_STRING,
            FieldValue::Byte(_) => TID_BYTE,
            FieldValue::Word(_) => TID_WORD,
            FieldValue::UInt32(_) => TID_UINT32,
            FieldValue::UInt64(_) => TID_UINT64,
            FieldValue::Single(_) => TID_SINGLE,
        }
    }

    /// True если значение эквивалентно zero для своего типа.
    /// Соответствует `IsZeroValue` (StrategySerializer.pas:337-355).
    pub fn is_zero(&self) -> bool {
        match self {
            FieldValue::Bool(b) => !*b,
            FieldValue::Int32(v) => *v == 0,
            FieldValue::Int64(v) => *v == 0,
            FieldValue::Double(v) => v.abs() < 1e-10,
            FieldValue::String(s) => s.is_empty(),
            FieldValue::Byte(v) => *v == 0,
            FieldValue::Word(v) => *v == 0,
            FieldValue::UInt32(v) => *v == 0,
            FieldValue::UInt64(v) => *v == 0,
            FieldValue::Single(v) => v.abs() < 1e-10,
        }
    }
}

// =============================================================================
//  StrategySnapshot
// =============================================================================

/// Распакованный snapshot одной стратегии. Поля хранятся в HashMap по имени —
/// потребитель использует `FieldValue::*` extractors для строгой типизации.
#[derive(Debug, Clone)]
pub struct StrategySnapshot {
    pub strategy_id: u64,
    pub strategy_ver: i32,
    /// Unix epoch ms (TDateTime → UnixTimeToDelphi на стороне сервера, см. pas:671).
    pub last_date: u64,
    pub checked: bool,
    pub kind: u8,
    /// Folder path (из PathDict по PathID; пустая строка если PathID out-of-range).
    pub path: String,
    pub fields: HashMap<String, FieldValue>,
}

/// Raw Delphi `TStrategyKind` ordinal (`Strategies.pas`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct StrategyKind(pub u8);

impl StrategyKind {
    pub const UNKNOWN: Self = Self(0);
    pub const TELEGRAM: Self = Self(1);
    pub const DROPS: Self = Self(2);
    pub const WALLS: Self = Self(3);
    pub const VOLUMES: Self = Self(4);
    pub const PUMP_DETECTION: Self = Self(5);
    pub const MOON_SHOT: Self = Self(6);
    pub const V_LITE: Self = Self(7);
    pub const DELTA: Self = Self(8);
    pub const WAVES: Self = Self(9);
    pub const COMBO: Self = Self(10);
    pub const UDP: Self = Self(11);
    pub const MANUAL: Self = Self(12);
    pub const MOON_STRIKE: Self = Self(13);
    pub const NEW_LISTING: Self = Self(14);
    pub const LIQUIDATIONS: Self = Self(15);
    pub const TOP_MARKET: Self = Self(16);
    pub const EMA: Self = Self(17);
    pub const SPREAD: Self = Self(18);
    pub const CHART_WALL: Self = Self(19);
    pub const MOON_HOOK: Self = Self(20);
    pub const ACTIVITY: Self = Self(21);
    pub const ALERTS: Self = Self(22);
    pub const WATCHER: Self = Self(23);
}

/// Delphi strategy active-state mode from `TStratForm.CheckActive`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum StrategyActiveMode {
    /// `cfg.MoonProtoConfig.ActiveClient = true`.
    ActiveClient,
    /// `UsingMoonProto = true` and not `ActiveClient`.
    UsingMoonProto,
    /// Standalone MoonBot path, without MoonProto split.
    Standalone,
}

impl StrategySnapshot {
    pub fn kind_like_delphi(&self) -> StrategyKind {
        StrategyKind(self.kind)
    }

    pub fn field_bool_or_false(&self, name: &str) -> bool {
        matches!(self.fields.get(name), Some(FieldValue::Bool(true)))
    }

    pub fn auto_buy_like_delphi(&self) -> bool {
        self.field_bool_or_false("AutoBuy")
    }

    pub fn run_detect_on_kernel_like_delphi(&self) -> bool {
        self.field_bool_or_false("RunDetectOnKernel")
    }

    pub fn short_like_delphi(&self) -> bool {
        self.field_bool_or_false("Short")
    }

    pub fn sell_from_asset_like_delphi(&self) -> bool {
        self.field_bool_or_false("SellFromAsset")
    }

    /// Delphi `TStrategy.CanAutoBuy`.
    pub fn can_auto_buy_like_delphi(&self) -> bool {
        (self.auto_buy_like_delphi() || self.kind_like_delphi() == StrategyKind::MOON_SHOT)
            && self.kind_like_delphi() != StrategyKind::MANUAL
    }

    /// Delphi `TStratForm.CheckActive` / `bStartCheckedClick` active assignment.
    pub fn active_like_delphi(&self, mode: StrategyActiveMode) -> bool {
        match mode {
            StrategyActiveMode::ActiveClient => {
                self.checked
                    && !self.can_auto_buy_like_delphi()
                    && !self.run_detect_on_kernel_like_delphi()
            }
            StrategyActiveMode::UsingMoonProto => {
                self.checked
                    && (self.can_auto_buy_like_delphi() || self.run_detect_on_kernel_like_delphi())
            }
            StrategyActiveMode::Standalone => self.checked,
        }
    }
}

#[derive(Debug, Clone, Default)]
pub struct StrategyBatch {
    pub names: Vec<String>,
    pub paths: Vec<String>,
    pub strategies: Vec<StrategySnapshot>,
}

// =============================================================================
//  Парсер
// =============================================================================

/// Парсинг с DEFLATE-сжатого payload'а (как приходит в `TStratSnapshot.data`).
pub fn parse_strategy_batch(deflate_bytes: &[u8]) -> Option<StrategyBatch> {
    let mut decoder = DeflateDecoder::new(deflate_bytes);
    let mut decompressed = Vec::new();
    decoder.read_to_end(&mut decompressed).ok()?;
    parse_strategy_batch_plain(&decompressed)
}

/// Парсинг уже распакованного плоского payload'а (для случая если decompression сделан снаружи).
pub fn parse_strategy_batch_plain(data: &[u8]) -> Option<StrategyBatch> {
    let mut pos = 0usize;
    let names = read_dict(data, &mut pos)?;
    let paths = read_dict(data, &mut pos)?;
    let strat_count = read_u16(data, &mut pos)? as usize;
    let mut strategies = Vec::with_capacity(strat_count);
    for _ in 0..strat_count {
        strategies.push(read_strategy(data, &mut pos, &names, &paths)?);
    }
    Some(StrategyBatch {
        names,
        paths,
        strategies,
    })
}

fn read_dict(data: &[u8], pos: &mut usize) -> Option<Vec<String>> {
    let count = read_u16(data, pos)? as usize;
    let mut out = Vec::with_capacity(count);
    for _ in 0..count {
        let len = read_u8(data, pos)? as usize;
        if *pos + len > data.len() {
            return None;
        }
        let s = decode_utf8_delphi(&data[*pos..*pos + len]);
        *pos += len;
        out.push(s);
    }
    Some(out)
}

fn read_strategy(
    data: &[u8],
    pos: &mut usize,
    names: &[String],
    paths: &[String],
) -> Option<StrategySnapshot> {
    let strategy_id = read_u64(data, pos)?;
    let strategy_ver = read_i32(data, pos)?;
    let last_date = read_u64(data, pos)?;
    let checked = read_u8(data, pos)? != 0;
    let kind = read_u8(data, pos)?;
    let path_id = read_u16(data, pos)? as usize;
    let path = paths.get(path_id).cloned().unwrap_or_default();

    let field_count = read_u16(data, pos)? as usize;
    let mut fields = HashMap::with_capacity(field_count);

    for _ in 0..field_count {
        let field_idx = read_u16(data, pos)? as usize;
        let type_id = read_u8(data, pos)?;
        let is_zero = (type_id & TID_ZERO_FLAG) != 0;
        let real_type = type_id & 0x7F;
        let name = names.get(field_idx);

        if let Some(name) = name {
            if let Some(expected_type) = strategy_field_expected_type_id(name) {
                if real_type != expected_type {
                    skip_field_by_type_id(data, pos, type_id)?;
                    continue;
                }
            }
        }

        let value: Option<FieldValue> = if is_zero {
            // Value bytes отсутствуют (Delphi: `If (TypeID and TID_ZERO_FLAG) <> 0 then exit`).
            FieldValue::zero(real_type)
        } else {
            try_read_field_value(data, pos, real_type)
        };

        if let Some(v) = value {
            if let Some(name) = name {
                fields.insert(name.clone(), v);
            }
            // Иначе — поле известного типа, но имя не в словаре. Поведение Delphi:
            // ReaderProps[idx] = nil → SkipField; в данной точке мы УЖЕ прочитали значение,
            // так что просто игнорируем (позиция корректна).
        }
        // Если value=None и !is_zero — это случай unknown TypeID: `try_read_field_value`
        // выполнил fallback skip 8 байт (как Delphi `SkipFieldByTypeID` else branch pas:373).
    }

    Some(StrategySnapshot {
        strategy_id,
        strategy_ver,
        last_date,
        checked,
        kind,
        path,
        fields,
    })
}

/// Читает значение по `type_id`. Если type_id неизвестный — fallback skip 8 байт
/// (как `SkipFieldByTypeID` pas:373: `Stream.Position := Stream.Position + 8`).
fn try_read_field_value(data: &[u8], pos: &mut usize, type_id: u8) -> Option<FieldValue> {
    match type_id {
        TID_BOOL => Some(FieldValue::Bool(read_u8(data, pos)? != 0)),
        TID_BYTE => Some(FieldValue::Byte(read_u8(data, pos)?)),
        TID_WORD => Some(FieldValue::Word(read_u16(data, pos)?)),
        TID_INT32 => Some(FieldValue::Int32(read_i32(data, pos)?)),
        TID_UINT32 => Some(FieldValue::UInt32(read_u32(data, pos)?)),
        TID_INT64 => Some(FieldValue::Int64(read_i64(data, pos)?)),
        TID_UINT64 => Some(FieldValue::UInt64(read_u64(data, pos)?)),
        TID_SINGLE => Some(FieldValue::Single(read_f32(data, pos)?)),
        TID_DOUBLE => Some(FieldValue::Double(read_f64(data, pos)?)),
        TID_STRING => {
            let len = read_u16(data, pos)? as usize;
            if *pos + len > data.len() {
                return None;
            }
            let s = decode_utf8_delphi(&data[*pos..*pos + len]);
            *pos += len;
            Some(FieldValue::String(s))
        }
        _ => {
            // Unknown — fallback skip 8 байт. Позиция сдвигается, но значение не возвращается.
            *pos = (*pos + 8).min(data.len());
            None
        }
    }
}

fn skip_field_by_type_id(data: &[u8], pos: &mut usize, type_id: u8) -> Option<()> {
    if (type_id & TID_ZERO_FLAG) != 0 {
        return Some(());
    }

    let size = match type_id & 0x7F {
        TID_BOOL | TID_BYTE => Some(1),
        TID_WORD => Some(2),
        TID_INT32 | TID_UINT32 | TID_SINGLE => Some(4),
        TID_INT64 | TID_UINT64 | TID_DOUBLE => Some(8),
        TID_STRING => {
            let len = read_u16(data, pos)? as usize;
            if *pos + len > data.len() {
                return None;
            }
            *pos += len;
            return Some(());
        }
        _ => Some(8),
    }?;

    if *pos + size > data.len() {
        return None;
    }
    *pos += size;
    Some(())
}

// --- Primitive readers ---
fn read_u8(d: &[u8], p: &mut usize) -> Option<u8> {
    if *p + 1 > d.len() {
        return None;
    }
    let v = d[*p];
    *p += 1;
    Some(v)
}
fn read_u16(d: &[u8], p: &mut usize) -> Option<u16> {
    if *p + 2 > d.len() {
        return None;
    }
    let v = u16::from_le_bytes(d[*p..*p + 2].try_into().unwrap());
    *p += 2;
    Some(v)
}
fn read_i32(d: &[u8], p: &mut usize) -> Option<i32> {
    if *p + 4 > d.len() {
        return None;
    }
    let v = i32::from_le_bytes(d[*p..*p + 4].try_into().unwrap());
    *p += 4;
    Some(v)
}
fn read_u32(d: &[u8], p: &mut usize) -> Option<u32> {
    if *p + 4 > d.len() {
        return None;
    }
    let v = u32::from_le_bytes(d[*p..*p + 4].try_into().unwrap());
    *p += 4;
    Some(v)
}
fn read_i64(d: &[u8], p: &mut usize) -> Option<i64> {
    if *p + 8 > d.len() {
        return None;
    }
    let v = i64::from_le_bytes(d[*p..*p + 8].try_into().unwrap());
    *p += 8;
    Some(v)
}
fn read_u64(d: &[u8], p: &mut usize) -> Option<u64> {
    if *p + 8 > d.len() {
        return None;
    }
    let v = u64::from_le_bytes(d[*p..*p + 8].try_into().unwrap());
    *p += 8;
    Some(v)
}
fn read_f32(d: &[u8], p: &mut usize) -> Option<f32> {
    if *p + 4 > d.len() {
        return None;
    }
    let v = f32::from_le_bytes(d[*p..*p + 4].try_into().unwrap());
    *p += 4;
    Some(v)
}
fn read_f64(d: &[u8], p: &mut usize) -> Option<f64> {
    if *p + 8 > d.len() {
        return None;
    }
    let v = f64::from_le_bytes(d[*p..*p + 8].try_into().unwrap());
    *p += 8;
    Some(v)
}

// =============================================================================
//  Writer (для тестов и опционального клиентского `WriteStrategy`)
// =============================================================================

/// Builder для создания DEFLATE-compressed snapshot'а. Wire-format зеркало
/// `BeginWrite/WriteStrategy/FinalizeWrite`: dicts, headers, type IDs, zero flag,
/// raw-deflate и length truncation совпадают с Delphi.
///
/// `StrategySnapshot::fields` may contain a full user-side map, but the writer
/// serializes only the same wire-visible subset Delphi can write: known public
/// `TStrategy` fields with the expected TypeID and value different from
/// `TStrategy.Create` defaults. `SellOrderColor`/`BuyOrderColor` defaults are
/// runtime `Vars` state in Delphi; omit them unless they are explicit overrides.
#[derive(Debug, Default)]
pub struct StrategyBatchBuilder {
    name_dict: Vec<String>,
    name_idx: HashMap<String, u16>,
    path_dict: Vec<String>,
    path_idx: HashMap<String, u16>,
    body: Vec<u8>,
    count: u16,
}

impl StrategyBatchBuilder {
    pub fn new() -> Self {
        Self::default()
    }

    fn name_index(&mut self, name: &str) -> u16 {
        if let Some(&i) = self.name_idx.get(name) {
            return i;
        }
        let i = self.name_dict.len() as u16;
        self.name_dict.push(name.to_string());
        self.name_idx.insert(name.to_string(), i);
        i
    }

    fn path_index(&mut self, path: &str) -> u16 {
        if let Some(&i) = self.path_idx.get(path) {
            return i;
        }
        let i = self.path_dict.len() as u16;
        self.path_dict.push(path.to_string());
        self.path_idx.insert(path.to_string(), i);
        i
    }

    /// Добавить одну стратегию.
    pub fn write_strategy(&mut self, s: &StrategySnapshot) {
        let path_id = self.path_index(&s.path);
        // Header
        self.body.extend_from_slice(&s.strategy_id.to_le_bytes());
        self.body.extend_from_slice(&s.strategy_ver.to_le_bytes());
        self.body.extend_from_slice(&s.last_date.to_le_bytes());
        self.body.push(s.checked as u8);
        self.body.push(s.kind);
        self.body.extend_from_slice(&path_id.to_le_bytes());

        // Сериализуем поля. Записываем количество (placeholder), потом обновим.
        let count_offset = self.body.len();
        self.body.extend_from_slice(&[0u8, 0u8]);
        let mut field_count = 0u16;

        // Delphi iterates public TStrategy fields in RTTI declaration order, then
        // RebuildFiledsList removes fields that are not in the kind/config PropMask.
        // Rust snapshots are maps, so we filter back to the exact Delphi field set
        // before writing: unknown/mismatched/default fields are not wire-visible.
        let mut entries: Vec<_> = s.fields.iter().collect();
        entries.sort_by(|a, b| {
            let ar = strategy_field_order_rank(a.0);
            let br = strategy_field_order_rank(b.0);
            ar.cmp(&br).then_with(|| a.0.cmp(b.0))
        });

        for (name, value) in entries {
            if !strategy_field_should_write(name, value) {
                continue;
            }
            let idx = self.name_index(name);
            self.body.extend_from_slice(&idx.to_le_bytes());
            write_field(&mut self.body, value);
            field_count = field_count.wrapping_add(1);
        }
        // Backfill count
        self.body[count_offset..count_offset + 2].copy_from_slice(&field_count.to_le_bytes());
        self.count = self.count.wrapping_add(1);
    }

    /// Финализировать в DEFLATE-compressed payload (формат TStratSnapshot.data).
    pub fn finalize(self) -> Vec<u8> {
        let mut plain = Vec::with_capacity(self.body.len() + 64);

        // NameDict
        plain.extend_from_slice(&(self.name_dict.len() as u16).to_le_bytes());
        for n in &self.name_dict {
            let b = n.as_bytes();
            // PathLen/NameLen — byte (max 255). Для стратегий имена полей < 255 байт.
            write_u8_len_bytes(&mut plain, b);
        }
        // PathDict
        plain.extend_from_slice(&(self.path_dict.len() as u16).to_le_bytes());
        for p in &self.path_dict {
            let b = p.as_bytes();
            write_u8_len_bytes(&mut plain, b);
        }
        // StratCount + body
        plain.extend_from_slice(&self.count.to_le_bytes());
        plain.extend_from_slice(&self.body);

        // DEFLATE compress (raw, без zlib header — Delphi -15)
        let mut encoder = DeflateEncoder::new(Vec::new(), Compression::default());
        encoder.write_all(&plain).unwrap();
        encoder.finish().unwrap()
    }
}

fn write_field(out: &mut Vec<u8>, v: &FieldValue) {
    let type_id = v.type_id();
    if v.is_zero() {
        // Записываем только TypeID с флагом ZERO; value bytes отсутствуют.
        out.push(type_id | TID_ZERO_FLAG);
        return;
    }
    out.push(type_id);
    match v {
        FieldValue::Bool(b) => out.push(*b as u8),
        FieldValue::Byte(b) => out.push(*b),
        FieldValue::Word(w) => out.extend_from_slice(&w.to_le_bytes()),
        FieldValue::Int32(i) => out.extend_from_slice(&i.to_le_bytes()),
        FieldValue::UInt32(u) => out.extend_from_slice(&u.to_le_bytes()),
        FieldValue::Int64(i) => out.extend_from_slice(&i.to_le_bytes()),
        FieldValue::UInt64(u) => out.extend_from_slice(&u.to_le_bytes()),
        FieldValue::Single(f) => out.extend_from_slice(&f.to_le_bytes()),
        FieldValue::Double(d) => out.extend_from_slice(&d.to_le_bytes()),
        FieldValue::String(s) => {
            let b = s.as_bytes();
            write_u16_len_bytes(out, b);
        }
    }
}

fn write_u8_len_bytes(out: &mut Vec<u8>, bytes: &[u8]) {
    let len = bytes.len() as u8;
    let len_usize = usize::from(len);
    out.push(len);
    out.extend_from_slice(&bytes[..len_usize]);
}

fn write_u16_len_bytes(out: &mut Vec<u8>, bytes: &[u8]) {
    let len = bytes.len() as u16;
    let len_usize = usize::from(len);
    out.extend_from_slice(&len.to_le_bytes());
    out.extend_from_slice(&bytes[..len_usize]);
}

// =============================================================================
//  Tests
// =============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_strategy(id: u64, name: &str, path: &str) -> StrategySnapshot {
        let mut fields = HashMap::new();
        fields.insert(
            "StrategyName".to_string(),
            FieldValue::String(name.to_string()),
        );
        fields.insert("OrderSize".to_string(), FieldValue::Double(123.45));
        fields.insert("KeepAlert".to_string(), FieldValue::Int32(61));
        fields.insert("AcceptCommands".to_string(), FieldValue::Bool(true));
        fields.insert(
            "Comment".to_string(),
            FieldValue::String("Test strategy".to_string()),
        );
        StrategySnapshot {
            strategy_id: id,
            strategy_ver: 1,
            last_date: 1737000000000, // 2026-01-16 UTC ms
            checked: true,
            kind: 5,
            path: path.to_string(),
            fields,
        }
    }

    fn strategy_with_fields(
        kind: StrategyKind,
        checked: bool,
        fields: &[(&str, FieldValue)],
    ) -> StrategySnapshot {
        StrategySnapshot {
            strategy_id: 1,
            strategy_ver: 1,
            last_date: 1,
            checked,
            kind: kind.0,
            path: String::new(),
            fields: fields
                .iter()
                .map(|(name, value)| ((*name).to_string(), value.clone()))
                .collect(),
        }
    }

    #[test]
    fn strategy_active_helpers_match_delphi_check_active_modes() {
        let listing = strategy_with_fields(StrategyKind::NEW_LISTING, true, &[]);
        assert!(listing.active_like_delphi(StrategyActiveMode::ActiveClient));
        assert!(!listing.active_like_delphi(StrategyActiveMode::UsingMoonProto));
        assert!(listing.active_like_delphi(StrategyActiveMode::Standalone));

        let moonshot = strategy_with_fields(StrategyKind::MOON_SHOT, true, &[]);
        assert!(
            moonshot.can_auto_buy_like_delphi(),
            "Delphi CanAutoBuy is true for MoonShot even when AutoBuy=false"
        );
        assert!(!moonshot.active_like_delphi(StrategyActiveMode::ActiveClient));
        assert!(moonshot.active_like_delphi(StrategyActiveMode::UsingMoonProto));

        let remote_kernel = strategy_with_fields(
            StrategyKind::NEW_LISTING,
            true,
            &[("RunDetectOnKernel", FieldValue::Bool(true))],
        );
        assert!(!remote_kernel.active_like_delphi(StrategyActiveMode::ActiveClient));
        assert!(remote_kernel.active_like_delphi(StrategyActiveMode::UsingMoonProto));
    }

    #[test]
    fn empty_batch_roundtrip() {
        let builder = StrategyBatchBuilder::new();
        let compressed = builder.finalize();
        let parsed = parse_strategy_batch(&compressed).unwrap();
        assert!(parsed.names.is_empty());
        assert!(parsed.paths.is_empty());
        assert!(parsed.strategies.is_empty());
    }

    #[test]
    fn single_strategy_roundtrip() {
        let mut b = StrategyBatchBuilder::new();
        let s = sample_strategy(100, "Strat-1", "Folder/A");
        b.write_strategy(&s);
        let compressed = b.finalize();

        let parsed = parse_strategy_batch(&compressed).unwrap();
        assert_eq!(parsed.strategies.len(), 1);
        let ps = &parsed.strategies[0];
        assert_eq!(ps.strategy_id, 100);
        assert_eq!(ps.strategy_ver, 1);
        assert!(ps.checked);
        assert_eq!(ps.kind, 5);
        assert_eq!(ps.path, "Folder/A");
        assert_eq!(
            ps.fields.get("StrategyName"),
            Some(&FieldValue::String("Strat-1".to_string()))
        );
        assert_eq!(
            ps.fields.get("OrderSize"),
            Some(&FieldValue::Double(123.45))
        );
        assert_eq!(ps.fields.get("KeepAlert"), Some(&FieldValue::Int32(61)));
        assert_eq!(
            ps.fields.get("AcceptCommands"),
            Some(&FieldValue::Bool(true))
        );
    }

    #[test]
    fn writer_uses_delphi_public_field_order_for_name_dict() {
        let mut fields = HashMap::new();
        fields.insert("OrderSize".to_string(), FieldValue::Double(1.0));
        fields.insert(
            "StrategyName".to_string(),
            FieldValue::String("A".to_string()),
        );
        fields.insert("UnknownZ".to_string(), FieldValue::Byte(1));
        fields.insert("AcceptCommands".to_string(), FieldValue::Bool(true));
        fields.insert("UnknownA".to_string(), FieldValue::Byte(2));
        fields.insert("Comment".to_string(), FieldValue::String("C".to_string()));

        let mut b = StrategyBatchBuilder::new();
        b.write_strategy(&StrategySnapshot {
            strategy_id: 1,
            strategy_ver: 1,
            last_date: 0,
            checked: true,
            kind: 1,
            path: String::new(),
            fields,
        });

        let parsed = parse_strategy_batch(&b.finalize()).unwrap();
        assert_eq!(
            parsed.names,
            vec![
                "StrategyName".to_string(),
                "Comment".to_string(),
                "AcceptCommands".to_string(),
                "OrderSize".to_string(),
            ]
        );
    }

    #[test]
    fn writer_skips_delphi_defaults_unknown_fields_and_type_mismatches() {
        let mut fields = HashMap::new();
        fields.insert(
            "StrategyName".to_string(),
            FieldValue::String("Local".to_string()),
        );
        fields.insert("KeepAlert".to_string(), FieldValue::Int32(60));
        fields.insert("UseStopLoss".to_string(), FieldValue::Bool(true));
        fields.insert("StopLoss".to_string(), FieldValue::Double(-5.0));
        fields.insert("PendingOrderSpread".to_string(), FieldValue::Double(0.1));
        fields.insert("DebugLog".to_string(), FieldValue::Bool(false));
        fields.insert("UnknownA".to_string(), FieldValue::Byte(7));
        fields.insert(
            "OrderSize".to_string(),
            FieldValue::String("wrong type".to_string()),
        );
        fields.insert(
            "SellOrderColor".to_string(),
            FieldValue::String(String::new()),
        );

        let mut b = StrategyBatchBuilder::new();
        b.write_strategy(&StrategySnapshot {
            strategy_id: 1,
            strategy_ver: 1,
            last_date: 0,
            checked: true,
            kind: 1,
            path: String::new(),
            fields,
        });

        let parsed = parse_strategy_batch(&b.finalize()).unwrap();
        assert_eq!(
            parsed.names,
            vec!["StrategyName".to_string(), "SellOrderColor".to_string()]
        );
        let ps = &parsed.strategies[0];
        assert_eq!(
            ps.fields.get("StrategyName"),
            Some(&FieldValue::String("Local".to_string()))
        );
        assert_eq!(
            ps.fields.get("SellOrderColor"),
            Some(&FieldValue::String(String::new()))
        );
        assert!(!ps.fields.contains_key("KeepAlert"));
        assert!(!ps.fields.contains_key("UseStopLoss"));
        assert!(!ps.fields.contains_key("StopLoss"));
        assert!(!ps.fields.contains_key("PendingOrderSpread"));
        assert!(!ps.fields.contains_key("DebugLog"));
        assert!(!ps.fields.contains_key("UnknownA"));
        assert!(!ps.fields.contains_key("OrderSize"));
    }

    #[test]
    fn multiple_strategies_share_name_dict() {
        let mut b = StrategyBatchBuilder::new();
        b.write_strategy(&sample_strategy(1, "A", "Folder/X"));
        b.write_strategy(&sample_strategy(2, "B", "Folder/X")); // same path
        b.write_strategy(&sample_strategy(3, "C", "Folder/Y")); // new path
        let compressed = b.finalize();

        let parsed = parse_strategy_batch(&compressed).unwrap();
        assert_eq!(parsed.strategies.len(), 3);
        // Имена уникальны: StrategyName, OrderSize, KeepAlert, AcceptCommands, Comment — 5 имён.
        assert_eq!(parsed.names.len(), 5);
        // Пути уникальны: 2 штуки.
        assert_eq!(parsed.paths.len(), 2);
    }

    #[test]
    fn zero_flag_encoded_for_zero_values() {
        let mut fields = HashMap::new();
        fields.insert("KeepAlert".to_string(), FieldValue::Int32(0));
        fields.insert("UseStopLoss".to_string(), FieldValue::Bool(false));
        fields.insert("SignalType".to_string(), FieldValue::String(String::new()));
        fields.insert("DebugLog".to_string(), FieldValue::Bool(false));

        let s = StrategySnapshot {
            strategy_id: 1,
            strategy_ver: 1,
            last_date: 0,
            checked: false,
            kind: 0,
            path: String::new(),
            fields,
        };

        let mut b = StrategyBatchBuilder::new();
        b.write_strategy(&s);
        let compressed = b.finalize();

        let parsed = parse_strategy_batch(&compressed).unwrap();
        let ps = &parsed.strategies[0];
        assert_eq!(ps.fields.get("KeepAlert"), Some(&FieldValue::Int32(0)));
        assert_eq!(ps.fields.get("UseStopLoss"), Some(&FieldValue::Bool(false)));
        assert_eq!(
            ps.fields.get("SignalType"),
            Some(&FieldValue::String(String::new()))
        );
        assert!(!ps.fields.contains_key("DebugLog"));
    }

    #[test]
    fn all_primitive_types_roundtrip() {
        let values = [
            FieldValue::Bool(true),
            FieldValue::Byte(200),
            FieldValue::Word(60000),
            FieldValue::Int32(-12345),
            FieldValue::UInt32(3_000_000_000),
            FieldValue::Int64(-9_876_543_210),
            FieldValue::UInt64(12_345_678_901_234),
            FieldValue::Single(3.125),
            FieldValue::Double(2.75),
            FieldValue::String("Hello 世界 🚀".to_string()),
        ];

        for value in values {
            let mut bytes = Vec::new();
            write_field(&mut bytes, &value);
            let mut pos = 0usize;
            let type_id = read_u8(&bytes, &mut pos).unwrap();
            assert_eq!(type_id & 0x7F, value.type_id());
            let parsed = if (type_id & TID_ZERO_FLAG) != 0 {
                FieldValue::zero(type_id).unwrap()
            } else {
                try_read_field_value(&bytes, &mut pos, type_id).unwrap()
            };
            assert_eq!(parsed, value);
            assert_eq!(pos, bytes.len());
        }
    }

    #[test]
    fn writer_wraps_name_path_and_string_lengths_like_delphi() {
        let long_name = "N".repeat(257);
        let long_path = "P".repeat(257);
        let long_value = "V".repeat(65_537);

        let mut name_bytes = Vec::new();
        write_u8_len_bytes(&mut name_bytes, long_name.as_bytes());
        assert_eq!(name_bytes, vec![1, b'N']);

        let mut fields = HashMap::new();
        fields.insert("Comment".to_string(), FieldValue::String(long_value));

        let s = StrategySnapshot {
            strategy_id: 1000,
            strategy_ver: 1,
            last_date: 1737000000000,
            checked: true,
            kind: 1,
            path: long_path,
            fields,
        };

        let mut b = StrategyBatchBuilder::new();
        b.write_strategy(&s);
        let compressed = b.finalize();
        let parsed = parse_strategy_batch(&compressed).unwrap();
        let ps = &parsed.strategies[0];

        assert_eq!(ps.path, "P");
        assert_eq!(
            ps.fields.get("Comment"),
            Some(&FieldValue::String("V".to_string()))
        );
    }

    #[test]
    fn missing_path_id_yields_empty() {
        // Конструируем raw plain payload где PathID=99 при пустом PathDict.
        let mut plain = Vec::new();
        // NameDict: 1 name "X"
        plain.extend_from_slice(&1u16.to_le_bytes());
        plain.push(1);
        plain.push(b'X');
        // PathDict: empty
        plain.extend_from_slice(&0u16.to_le_bytes());
        // StratCount: 1
        plain.extend_from_slice(&1u16.to_le_bytes());
        // Strategy
        plain.extend_from_slice(&42u64.to_le_bytes()); // id
        plain.extend_from_slice(&1i32.to_le_bytes()); // ver
        plain.extend_from_slice(&0u64.to_le_bytes()); // last_date
        plain.push(0); // checked
        plain.push(0); // kind
        plain.extend_from_slice(&99u16.to_le_bytes()); // path_id (OOR)
        plain.extend_from_slice(&0u16.to_le_bytes()); // field count

        let parsed = parse_strategy_batch_plain(&plain).unwrap();
        assert_eq!(parsed.strategies.len(), 1);
        assert_eq!(parsed.strategies[0].path, ""); // PathID out of range → empty
    }

    #[test]
    fn unknown_type_id_skipped_8_bytes() {
        // FieldIdx=0, TypeID=99 (неизвестный) → reader должен пропустить 8 байт.
        // После этого должен корректно прочитать следующее поле.
        let mut plain = Vec::new();
        // NameDict: 2 names
        plain.extend_from_slice(&2u16.to_le_bytes());
        plain.push(1);
        plain.push(b'A');
        plain.push(1);
        plain.push(b'B');
        // PathDict
        plain.extend_from_slice(&0u16.to_le_bytes());
        // StratCount
        plain.extend_from_slice(&1u16.to_le_bytes());
        // Strategy header
        plain.extend_from_slice(&1u64.to_le_bytes());
        plain.extend_from_slice(&1i32.to_le_bytes());
        plain.extend_from_slice(&0u64.to_le_bytes());
        plain.push(0);
        plain.push(0);
        plain.extend_from_slice(&0u16.to_le_bytes());
        // FieldCount=2
        plain.extend_from_slice(&2u16.to_le_bytes());
        // Field 0: idx=0, typeID=99 (unknown), 8 bytes value (всё нули)
        plain.extend_from_slice(&0u16.to_le_bytes());
        plain.push(99);
        plain.extend_from_slice(&[0u8; 8]);
        // Field 1: idx=1, typeID=TID_INT32, value=42
        plain.extend_from_slice(&1u16.to_le_bytes());
        plain.push(TID_INT32);
        plain.extend_from_slice(&42i32.to_le_bytes());

        let parsed = parse_strategy_batch_plain(&plain).unwrap();
        let ps = &parsed.strategies[0];
        // Field A не разобран (unknown TypeID).
        assert_eq!(ps.fields.get("A"), None);
        // Field B разобран как Int32=42.
        assert_eq!(ps.fields.get("B"), Some(&FieldValue::Int32(42)));
    }

    #[test]
    fn known_field_type_mismatch_is_skipped_like_delphi_read_field() {
        let mut plain = Vec::new();
        // NameDict: OrderSize expects TID_DOUBLE, Comment expects TID_STRING.
        plain.extend_from_slice(&2u16.to_le_bytes());
        plain.push(9);
        plain.extend_from_slice(b"OrderSize");
        plain.push(7);
        plain.extend_from_slice(b"Comment");
        // PathDict
        plain.extend_from_slice(&0u16.to_le_bytes());
        // StratCount
        plain.extend_from_slice(&1u16.to_le_bytes());
        // Strategy header
        plain.extend_from_slice(&1u64.to_le_bytes());
        plain.extend_from_slice(&1i32.to_le_bytes());
        plain.extend_from_slice(&0u64.to_le_bytes());
        plain.push(0);
        plain.push(0);
        plain.extend_from_slice(&0u16.to_le_bytes());
        // FieldCount=2
        plain.extend_from_slice(&2u16.to_le_bytes());
        // Field 0: OrderSize but wire type is String; Delphi skips it.
        plain.extend_from_slice(&0u16.to_le_bytes());
        plain.push(TID_STRING);
        plain.extend_from_slice(&3u16.to_le_bytes());
        plain.extend_from_slice(b"bad");
        // Field 1: Comment, correct string, proves skip consumed exact bytes.
        plain.extend_from_slice(&1u16.to_le_bytes());
        plain.push(TID_STRING);
        plain.extend_from_slice(&2u16.to_le_bytes());
        plain.extend_from_slice(b"ok");

        let parsed = parse_strategy_batch_plain(&plain).unwrap();
        let ps = &parsed.strategies[0];
        assert!(!ps.fields.contains_key("OrderSize"));
        assert_eq!(
            ps.fields.get("Comment"),
            Some(&FieldValue::String("ok".to_string()))
        );
    }

    #[test]
    fn invalid_utf8_dicts_and_string_fields_use_delphi_question_mark_fallback() {
        let mut plain = Vec::new();
        // NameDict: one invalid UTF-8 field name "N?me".
        plain.extend_from_slice(&1u16.to_le_bytes());
        plain.push(4);
        plain.extend_from_slice(&[b'N', 0xFF, b'm', b'e']);
        // PathDict: one invalid UTF-8 path "P?".
        plain.extend_from_slice(&1u16.to_le_bytes());
        plain.push(2);
        plain.extend_from_slice(&[b'P', 0x80]);
        // StratCount
        plain.extend_from_slice(&1u16.to_le_bytes());
        // Strategy header
        plain.extend_from_slice(&1u64.to_le_bytes());
        plain.extend_from_slice(&1i32.to_le_bytes());
        plain.extend_from_slice(&0u64.to_le_bytes());
        plain.push(0);
        plain.push(0);
        plain.extend_from_slice(&0u16.to_le_bytes());
        // FieldCount=1, field value "V?"
        plain.extend_from_slice(&1u16.to_le_bytes());
        plain.extend_from_slice(&0u16.to_le_bytes());
        plain.push(TID_STRING);
        plain.extend_from_slice(&2u16.to_le_bytes());
        plain.extend_from_slice(&[b'V', 0xFF]);

        let parsed = parse_strategy_batch_plain(&plain).unwrap();
        assert_eq!(parsed.names, vec!["N?me".to_string()]);
        assert_eq!(parsed.paths, vec!["P?".to_string()]);
        let ps = &parsed.strategies[0];
        assert_eq!(ps.path, "P?");
        assert_eq!(
            ps.fields.get("N?me"),
            Some(&FieldValue::String("V?".to_string()))
        );
    }

    #[test]
    fn truncated_payload_returns_none() {
        let mut plain = Vec::new();
        // Только частичный NameDict header (нет данных)
        plain.extend_from_slice(&100u16.to_le_bytes()); // обещано 100 имён
                                                        // Но больше нет данных → должен вернуть None
        let parsed = parse_strategy_batch_plain(&plain);
        assert!(parsed.is_none());
    }

    #[test]
    fn field_value_type_id_match() {
        assert_eq!(FieldValue::Bool(true).type_id(), TID_BOOL);
        assert_eq!(FieldValue::Byte(0).type_id(), TID_BYTE);
        assert_eq!(FieldValue::Word(0).type_id(), TID_WORD);
        assert_eq!(FieldValue::Int32(0).type_id(), TID_INT32);
        assert_eq!(FieldValue::UInt32(0).type_id(), TID_UINT32);
        assert_eq!(FieldValue::Int64(0).type_id(), TID_INT64);
        assert_eq!(FieldValue::UInt64(0).type_id(), TID_UINT64);
        assert_eq!(FieldValue::Single(0.0).type_id(), TID_SINGLE);
        assert_eq!(FieldValue::Double(0.0).type_id(), TID_DOUBLE);
        assert_eq!(FieldValue::String(String::new()).type_id(), TID_STRING);
    }

    #[test]
    fn field_value_zero_for_each_type() {
        assert_eq!(FieldValue::zero(TID_BOOL), Some(FieldValue::Bool(false)));
        assert_eq!(FieldValue::zero(TID_INT32), Some(FieldValue::Int32(0)));
        assert_eq!(
            FieldValue::zero(TID_STRING),
            Some(FieldValue::String(String::new()))
        );
        assert_eq!(FieldValue::zero(TID_DOUBLE), Some(FieldValue::Double(0.0)));
        assert_eq!(FieldValue::zero(99), None);
    }

    #[test]
    fn is_zero_for_each_type() {
        assert!(FieldValue::Bool(false).is_zero());
        assert!(!FieldValue::Bool(true).is_zero());
        assert!(FieldValue::Int32(0).is_zero());
        assert!(!FieldValue::Int32(1).is_zero());
        assert!(FieldValue::String(String::new()).is_zero());
        assert!(!FieldValue::String("x".to_string()).is_zero());
        assert!(FieldValue::Double(0.0).is_zero());
        assert!(FieldValue::Double(1e-15).is_zero()); // < 1e-10
        assert!(!FieldValue::Double(1e-5).is_zero());
    }
}
