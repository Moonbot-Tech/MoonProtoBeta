//! Engine API method ordinals.

/// Engine RPC method identifiers.
///
/// Each method has a corresponding builder in [`super::super::engine_request`]
/// and a `Client::api_*` wrapper. Most wrappers return an `mpsc::Receiver` for
/// asynchronous handling through the pending-response registry.
///
/// Method-specific response payloads are parsed by helpers near the related
/// protocol module, for example `commands::market`, `commands::candles`, or
/// `commands::engine_api` for small scalar responses.
#[derive(Clone, Copy, PartialEq, Eq, Hash)]
pub struct EngineMethod(u8);

#[allow(non_upper_case_globals)]
impl EngineMethod {
    /// Empty method (`emk_None`).
    pub const None: Self = Self(0);
    /// `BaseCheck`: engine health and server-identity check.
    pub const BaseCheck: Self = Self(1);
    /// `AuthCheck`: exchange API-key authorization check.
    pub const AuthCheck: Self = Self(2);
    /// `GetMarketsList`: full list of tradable markets.
    ///
    /// The response contains market records parsed by
    /// [`crate::commands::market::parse_markets_list_response`].
    pub const GetMarketsList: Self = Self(3);
    /// `UpdateMarketsList`: refresh market prices, funding, mark price, and
    /// correlation prices.
    pub const UpdateMarketsList: Self = Self(4);
    /// `GetMarketsIndexes`: compact server `mIndex -> market name` mapping used
    /// by indexed streams.
    pub const GetMarketsIndexes: Self = Self(5);
    /// `GetBalance`: current quantity for one currency. Parse with
    /// [`super::parse_get_balance_response`].
    pub const GetBalance: Self = Self(6);
    /// `GetMarketsBalanceFull`: server-side full balance refresh.
    ///
    /// Current Delphi `MoonProtoEngineServer.pas -> ProcessRequest` calls
    /// `Engine.GetMarketsBalanceFull`, but `WriteBalancesToStream` is not
    /// implemented in the reference server, so a successful response has an
    /// empty `data` payload.
    pub const GetMarketsBalanceFull: Self = Self(7);
    /// `GetOrder` — enum value exists in `TEngineMethodKind`.
    ///
    /// The current Delphi reference server has no `ProcessRequest` branch for this
    /// method, so it returns `Unknown method` (error 400). Raw wrapper is kept for
    /// protocol completeness / future server versions.
    pub const GetOrder: Self = Self(8);
    /// `GetOpenOrders` — enum value exists in `TEngineMethodKind`.
    ///
    /// The current Delphi reference server has no request-handler branch for this
    /// method and returns `Unknown method` (error 400).
    pub const GetOpenOrders: Self = Self(9);
    /// `GetActiveOrders` — enum value exists in `TEngineMethodKind`.
    ///
    /// The current Delphi reference server has no request-handler branch for this
    /// method and returns `Unknown method` (error 400).
    pub const GetActiveOrders: Self = Self(10);
    /// `CancelAllOrders`: cancel all open orders.
    pub const CancelAllOrders: Self = Self(11);
    /// `SetLeverage`: set leverage for one market.
    pub const SetLeverage: Self = Self(12);
    /// `SetHedgeMode`: enable or disable hedge mode.
    pub const SetHedgeMode: Self = Self(13);
    /// `QueryHedgeMode`: current hedge-mode flag. Parse with
    /// [`super::parse_query_hedge_mode_response`].
    pub const QueryHedgeMode: Self = Self(14);
    /// `CheckAPIExpirationTime`: exchange API-key expiration as a Delphi
    /// `TDateTime`, parsed by [`super::parse_api_expiration_time_response`].
    pub const CheckAPIExpirationTime: Self = Self(15);
    /// `CheckBinanceTags`: Binance token permission tags.
    pub const CheckBinanceTags: Self = Self(16);
    /// `TradesResend`: request resend for missing TradesStream packet numbers.
    pub const TradesResend: Self = Self(17);
    /// `SubscribeAllTrades`: subscribe to the all-trades stream.
    pub const SubscribeAllTrades: Self = Self(18);
    /// `UnsubscribeAllTrades`: unsubscribe from the all-trades stream.
    pub const UnsubscribeAllTrades: Self = Self(19);
    /// `SubscribeOrderBook`: subscribe to orderbooks for market names.
    pub const SubscribeOrderBook: Self = Self(20);
    /// `UnsubscribeOrderBook`: unsubscribe from orderbooks for market names.
    pub const UnsubscribeOrderBook: Self = Self(21);
    /// `RequestOrderBookFull`: request a full snapshot for one indexed orderbook.
    pub const RequestOrderBookFull: Self = Self(22);
    /// `ReloadOrderBook`: force reload of subscribed orderbooks.
    pub const ReloadOrderBook: Self = Self(23);
    /// `RequestCandlesData`: request full historical candle data.
    ///
    /// The response is chunked: multiple `EngineResponse` packets share one UID.
    /// Prefer `Client::request_candles_data` or `Client::api_request_candles_data_async`.
    pub const RequestCandlesData: Self = Self(24);
    /// `ChangePositionType`: change isolated/cross position type for a market.
    pub const ChangePositionType: Self = Self(25);
    /// `ConvertDustBNB`: convert dust balances to BNB.
    pub const ConvertDustBNB: Self = Self(26);
    /// `ConfirmRiskLimit`: confirm risk limit for a market.
    pub const ConfirmRiskLimit: Self = Self(27);
    /// `SetMAMode`: enable or disable Binance Multi-Assets mode.
    pub const SetMAMode: Self = Self(28);
    /// `DoTransferAsset`: transfer one asset between exchange wallet kinds.
    pub const DoTransferAsset: Self = Self(29);
    /// `UpdateTransferAssets`: refresh the transferable asset list for one
    /// exchange wallet kind. Parse with [`super::parse_update_transfer_assets_response`].
    pub const UpdateTransferAssets: Self = Self(30);
    /// `GetCoinCardCandles`: short candle history for a coin-card UI component.
    pub const GetCoinCardCandles: Self = Self(31);

    /// Keep the raw Delphi ordinal byte. Delphi reads/writes
    /// `TEngineMethodKind` via `ms.Read/Stream.Write` and does not turn an
    /// unknown ordinal into `emk_None`.
    pub const fn from_byte(b: u8) -> Self {
        Self(b)
    }

    pub const fn to_byte(self) -> u8 {
        self.0
    }

    pub const fn is_known(self) -> bool {
        self.0 <= Self::GetCoinCardCandles.0
    }

    pub const fn name(self) -> &'static str {
        match self {
            Self::None => "None",
            Self::BaseCheck => "BaseCheck",
            Self::AuthCheck => "AuthCheck",
            Self::GetMarketsList => "GetMarketsList",
            Self::UpdateMarketsList => "UpdateMarketsList",
            Self::GetMarketsIndexes => "GetMarketsIndexes",
            Self::GetBalance => "GetBalance",
            Self::GetMarketsBalanceFull => "GetMarketsBalanceFull",
            Self::GetOrder => "GetOrder",
            Self::GetOpenOrders => "GetOpenOrders",
            Self::GetActiveOrders => "GetActiveOrders",
            Self::CancelAllOrders => "CancelAllOrders",
            Self::SetLeverage => "SetLeverage",
            Self::SetHedgeMode => "SetHedgeMode",
            Self::QueryHedgeMode => "QueryHedgeMode",
            Self::CheckAPIExpirationTime => "CheckAPIExpirationTime",
            Self::CheckBinanceTags => "CheckBinanceTags",
            Self::TradesResend => "TradesResend",
            Self::SubscribeAllTrades => "SubscribeAllTrades",
            Self::UnsubscribeAllTrades => "UnsubscribeAllTrades",
            Self::SubscribeOrderBook => "SubscribeOrderBook",
            Self::UnsubscribeOrderBook => "UnsubscribeOrderBook",
            Self::RequestOrderBookFull => "RequestOrderBookFull",
            Self::ReloadOrderBook => "ReloadOrderBook",
            Self::RequestCandlesData => "RequestCandlesData",
            Self::ChangePositionType => "ChangePositionType",
            Self::ConvertDustBNB => "ConvertDustBNB",
            Self::ConfirmRiskLimit => "ConfirmRiskLimit",
            Self::SetMAMode => "SetMAMode",
            Self::DoTransferAsset => "DoTransferAsset",
            Self::UpdateTransferAssets => "UpdateTransferAssets",
            Self::GetCoinCardCandles => "GetCoinCardCandles",
            _ => "Unknown",
        }
    }
}

impl std::fmt::Debug for EngineMethod {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if self.is_known() {
            f.write_str(self.name())
        } else {
            write!(f, "Unknown({})", self.0)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn engine_method_known_bytes() {
        assert_eq!(EngineMethod::from_byte(1), EngineMethod::BaseCheck);
        assert_eq!(
            EngineMethod::from_byte(31),
            EngineMethod::GetCoinCardCandles
        );
        assert_eq!(EngineMethod::from_byte(0), EngineMethod::None);
    }

    #[test]
    // parity: MoonBot MoonProtoEngineStruct.pas:TEngineResponse.CreateFromStream
    fn engine_method_unknown_preserves_raw_ordinal() {
        // Delphi `ms.Read(Method, SizeOf(Method))` keeps the raw enum byte.
        let method = EngineMethod::from_byte(99);
        assert_eq!(method.to_byte(), 99);
        assert_eq!(method.name(), "Unknown");
        assert!(!method.is_known());
        assert_eq!(EngineMethod::from_byte(255).to_byte(), 255);
    }
}
