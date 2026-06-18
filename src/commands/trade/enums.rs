//! Stable order-state ordinal wrappers used by the order channel.
//!
//! These wrappers preserve unknown raw bytes so newer server-side states remain
//! round-trippable instead of being collapsed into a lossy enum.

/// Order side/type: sell, buy, stop-buy, or limit-buy.
#[derive(Clone, Copy, PartialEq, Eq, Hash)]
pub struct OrderType(u8);

#[allow(non_upper_case_globals)]
impl OrderType {
    pub const Sell: Self = Self(0);
    pub const Buy: Self = Self(1);
    pub const BuyStop: Self = Self(2);
    pub const BuyLimit: Self = Self(3);

    /// Preserve a raw ordinal byte.
    #[cfg(any(test, feature = "diagnostics"))]
    #[doc(hidden)]
    #[allow(dead_code)]
    pub const fn from_byte(b: u8) -> Self {
        Self(b)
    }

    #[cfg(not(any(test, feature = "diagnostics")))]
    pub(crate) const fn from_byte(b: u8) -> Self {
        Self(b)
    }

    #[cfg(any(test, feature = "diagnostics"))]
    #[doc(hidden)]
    pub const fn to_byte(self) -> u8 {
        self.0
    }

    #[cfg(not(any(test, feature = "diagnostics")))]
    pub(crate) const fn to_byte(self) -> u8 {
        self.0
    }

    pub const fn is_known(self) -> bool {
        self.0 <= Self::BuyLimit.0
    }

    pub const fn name(self) -> &'static str {
        match self {
            Self::Sell => "Sell",
            Self::Buy => "Buy",
            Self::BuyStop => "BuyStop",
            Self::BuyLimit => "BuyLimit",
            _ => "Unknown",
        }
    }
}

impl Default for OrderType {
    fn default() -> Self {
        Self::Sell
    }
}

impl std::fmt::Debug for OrderType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if self.is_known() {
            f.write_str(self.name())
        } else {
            write!(f, "Unknown({})", self.0)
        }
    }
}

/// Order execution subtype retained in the compact order snapshot.
#[derive(Clone, Copy, PartialEq, Eq, Hash)]
pub struct OrderSubType(u8);

#[allow(non_upper_case_globals)]
impl OrderSubType {
    pub const Limit: Self = Self(0);
    pub const Trailing: Self = Self(1);
    pub const Stop: Self = Self(2);
    pub const StopMarket: Self = Self(3);
    pub const ReduceOnly: Self = Self(4);
    pub const Market: Self = Self(5);

    #[cfg(any(test, feature = "diagnostics"))]
    #[doc(hidden)]
    #[allow(dead_code)]
    pub const fn from_byte(b: u8) -> Self {
        Self(b)
    }

    #[cfg(not(any(test, feature = "diagnostics")))]
    pub(crate) const fn from_byte(b: u8) -> Self {
        Self(b)
    }

    #[cfg(any(test, feature = "diagnostics"))]
    #[doc(hidden)]
    pub const fn to_byte(self) -> u8 {
        self.0
    }

    #[cfg(not(any(test, feature = "diagnostics")))]
    pub(crate) const fn to_byte(self) -> u8 {
        self.0
    }

    pub const fn is_known(self) -> bool {
        self.0 <= Self::Market.0
    }

    pub const fn name(self) -> &'static str {
        match self {
            Self::Limit => "Limit",
            Self::Trailing => "Trailing",
            Self::Stop => "Stop",
            Self::StopMarket => "StopMarket",
            Self::ReduceOnly => "ReduceOnly",
            Self::Market => "Market",
            _ => "Unknown",
        }
    }
}

impl Default for OrderSubType {
    fn default() -> Self {
        Self::Limit
    }
}

impl std::fmt::Debug for OrderSubType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if self.is_known() {
            f.write_str(self.name())
        } else {
            write!(f, "Unknown({})", self.0)
        }
    }
}

/// Trading order state-machine status.
///
/// Standard flow for a long position:
/// ```text
///   None ──► BuySet ──► BuyDone ──► SellSet ──► SellDone
///             │           │           │            │
///             ▼           ▼           ▼            ▼
///          BuyFail    BuyCancel   SellFail    SellCancel
/// ```
///
/// **Terminal states** (the order is closed and no further transition is expected):
/// `SellDone`, `SellAlmostDone`, `BuyFail`, `BuyCancel`, `SellFail`, `SellCancel`.
///
/// **Phase semantics** for UI grouping:
/// - **Buy phase** (`BuySet`/`BuyDone`/`BuyFail`/`BuyCancel`) waits for or
///   completes position entry.
/// - **Sell phase** (`SellSet`/`SellAlmostDone`/`SellDone`/`SellFail`/`SellCancel`) —
///   exits the position (take-profit, stop-loss, or manual close).
/// - `SellAlmostDone` means the sell completed through a replace/market-stop
///   path; it leaves the worker loop like the final sell statuses.
///
/// **Server constraints**:
/// - Phase rollback is rejected: a sell phase must not return to buy phase.
/// - Transitions inside the same phase are valid, e.g. `BuySet -> BuyDone`.
/// - Terminal state does not change.
#[derive(Clone, Copy, PartialEq, Eq, Hash)]
pub struct OrderWorkerStatus(u8);

#[allow(non_upper_case_globals)]
impl OrderWorkerStatus {
    /// Initial state: the order has not been sent to the exchange yet.
    pub const None: Self = Self(0);
    /// Buy order failed (exchange rejection, insufficient balance, etc.).
    pub const BuyFail: Self = Self(1);
    /// Buy order is placed on the exchange and waits for fill.
    pub const BuySet: Self = Self(2);
    /// Buy order was cancelled by the user or system.
    pub const BuyCancel: Self = Self(3);
    /// Buy order filled, position is open.
    pub const BuyDone: Self = Self(4);
    /// Sell order failed.
    pub const SellFail: Self = Self(5);
    /// Sell order (close/take-profit) is placed and waits for fill.
    pub const SellSet: Self = Self(6);
    /// Sell order was cancelled.
    pub const SellCancel: Self = Self(7);
    /// Sell order fully filled, position is closed.
    pub const SellDone: Self = Self(8);
    /// Delphi wire/source spelling kept for protocol parity tests.
    #[cfg(any(test, feature = "diagnostics"))]
    #[doc(hidden)]
    pub const SelLDone: Self = Self::SellDone;
    /// Sell completed through an intermediate path; terminal for worker/state.
    pub const SellAlmostDone: Self = Self(9);
    /// Delphi wire/source spelling kept for protocol parity tests.
    #[cfg(any(test, feature = "diagnostics"))]
    #[doc(hidden)]
    pub const SelLAlmostDone: Self = Self::SellAlmostDone;

    /// Preserve a raw Delphi ordinal byte.
    #[cfg(any(test, feature = "diagnostics"))]
    #[doc(hidden)]
    #[allow(dead_code)]
    pub const fn from_byte(b: u8) -> Self {
        Self(b)
    }

    #[cfg(not(any(test, feature = "diagnostics")))]
    pub(crate) const fn from_byte(b: u8) -> Self {
        Self(b)
    }

    #[cfg(any(test, feature = "diagnostics"))]
    #[doc(hidden)]
    pub const fn to_byte(self) -> u8 {
        self.0
    }

    #[cfg(not(any(test, feature = "diagnostics")))]
    pub(crate) const fn to_byte(self) -> u8 {
        self.0
    }

    pub const fn is_known(self) -> bool {
        self.0 <= Self::SellAlmostDone.0
    }

    pub const fn name(self) -> &'static str {
        match self {
            Self::None => "None",
            Self::BuyFail => "BuyFail",
            Self::BuySet => "BuySet",
            Self::BuyCancel => "BuyCancel",
            Self::BuyDone => "BuyDone",
            Self::SellFail => "SellFail",
            Self::SellSet => "SellSet",
            Self::SellCancel => "SellCancel",
            Self::SellDone => "SellDone",
            Self::SellAlmostDone => "SellAlmostDone",
            _ => "Unknown",
        }
    }

    /// Terminal status: the order is closed and the worker can be removed.
    pub const fn is_terminal(self) -> bool {
        matches!(
            self,
            Self::SellDone
                | Self::SellAlmostDone
                | Self::BuyCancel
                | Self::BuyFail
                | Self::SellFail
                | Self::SellCancel
        )
    }
}

impl std::fmt::Debug for OrderWorkerStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if self.is_known() {
            f.write_str(self.name())
        } else {
            write!(f, "Unknown({})", self.0)
        }
    }
}

/// TFixedPosition (Vars.pas:52): FP_Both=0, FP_Long=1, FP_Short=2.
#[derive(Clone, Copy, PartialEq, Eq, Hash)]
pub struct FixedPosition(u8);

#[allow(non_upper_case_globals)]
impl FixedPosition {
    pub const Both: Self = Self(0);
    pub const Long: Self = Self(1);
    pub const Short: Self = Self(2);

    #[cfg(any(test, feature = "diagnostics"))]
    #[doc(hidden)]
    #[allow(dead_code)]
    pub const fn from_byte(b: u8) -> Self {
        Self(b)
    }

    #[cfg(not(any(test, feature = "diagnostics")))]
    pub(crate) const fn from_byte(b: u8) -> Self {
        Self(b)
    }

    #[cfg(any(test, feature = "diagnostics"))]
    #[doc(hidden)]
    pub const fn to_byte(self) -> u8 {
        self.0
    }

    #[cfg(not(any(test, feature = "diagnostics")))]
    pub(crate) const fn to_byte(self) -> u8 {
        self.0
    }

    pub const fn is_known(self) -> bool {
        self.0 <= Self::Short.0
    }

    pub const fn name(self) -> &'static str {
        match self {
            Self::Both => "Both",
            Self::Long => "Long",
            Self::Short => "Short",
            _ => "Unknown",
        }
    }
}

impl std::fmt::Debug for FixedPosition {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if self.is_known() {
            f.write_str(self.name())
        } else {
            write!(f, "Unknown({})", self.0)
        }
    }
}

/// Sell-side bulk-move mode.
///
/// Describes how a move-all-sells action interprets its target price and
/// optional price zone.
#[derive(Clone, Copy, PartialEq, Eq, Hash)]
pub struct MoveAllCmdType(u8);

#[allow(non_upper_case_globals)]
impl MoveAllCmdType {
    /// Move all matching orders by `ReplaceMultiKind`.
    pub const MoveKind: Self = Self(0);
    /// Move orders whose price is inside `[price_zone.min_p, price_zone.max_p]`.
    pub const PriceZone: Self = Self(1);
    /// Percent/personal mode.
    pub const Pers: Self = Self(2);

    #[cfg(any(test, feature = "diagnostics"))]
    #[doc(hidden)]
    #[allow(dead_code)]
    pub const fn from_byte(b: u8) -> Self {
        Self(b)
    }

    #[cfg(not(any(test, feature = "diagnostics")))]
    pub(crate) const fn from_byte(b: u8) -> Self {
        Self(b)
    }

    #[cfg(any(test, feature = "diagnostics"))]
    #[doc(hidden)]
    pub const fn to_byte(self) -> u8 {
        self.0
    }

    #[cfg(not(any(test, feature = "diagnostics")))]
    pub(crate) const fn to_byte(self) -> u8 {
        self.0
    }

    pub const fn is_known(self) -> bool {
        self.0 <= Self::Pers.0
    }

    pub const fn name(self) -> &'static str {
        match self {
            Self::MoveKind => "MoveKind",
            Self::PriceZone => "PriceZone",
            Self::Pers => "Pers",
            _ => "Unknown",
        }
    }
}

impl std::fmt::Debug for MoveAllCmdType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if self.is_known() {
            f.write_str(self.name())
        } else {
            write!(f, "Unknown({})", self.0)
        }
    }
}

/// Buy-side bulk-move mode.
///
/// Buy-side moves support regular move-kind and percent/personal modes. There
/// is no buy-side price-zone mode.
#[derive(Clone, Copy, PartialEq, Eq, Hash)]
pub struct MoveAllBuysCmdType(u8);

#[allow(non_upper_case_globals)]
impl MoveAllBuysCmdType {
    pub const MoveKind: Self = Self(0);
    pub const Pers: Self = Self(2);

    #[cfg(any(test, feature = "diagnostics"))]
    #[doc(hidden)]
    #[allow(dead_code)]
    pub const fn from_byte(b: u8) -> Self {
        Self(b)
    }

    #[cfg(not(any(test, feature = "diagnostics")))]
    pub(crate) const fn from_byte(b: u8) -> Self {
        Self(b)
    }

    #[cfg(any(test, feature = "diagnostics"))]
    #[doc(hidden)]
    pub const fn to_byte(self) -> u8 {
        self.0
    }

    #[cfg(not(any(test, feature = "diagnostics")))]
    pub(crate) const fn to_byte(self) -> u8 {
        self.0
    }

    pub const fn is_known(self) -> bool {
        matches!(self, Self::MoveKind | Self::Pers)
    }

    pub const fn name(self) -> &'static str {
        match self {
            Self::MoveKind => "MoveKind",
            Self::Pers => "Pers",
            _ => "Unknown",
        }
    }
}

impl std::fmt::Debug for MoveAllBuysCmdType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if self.is_known() {
            f.write_str(self.name())
        } else {
            write!(f, "Unknown({})", self.0)
        }
    }
}

/// TReplaceMultiKind (Vars.pas:37).
#[derive(Clone, Copy, PartialEq, Eq, Hash)]
pub struct ReplaceMultiKind(u8);

#[allow(non_upper_case_globals)]
impl ReplaceMultiKind {
    pub const None: Self = Self(0);
    pub const Shift: Self = Self(1);
    pub const TopVol: Self = Self(2);
    pub const LowVol: Self = Self(3);
    pub const TopProfit: Self = Self(4);
    pub const All: Self = Self(5);
    pub const LastSet: Self = Self(6);
    pub const LastMoved: Self = Self(7);

    #[cfg(any(test, feature = "diagnostics"))]
    #[doc(hidden)]
    #[allow(dead_code)]
    pub const fn from_byte(b: u8) -> Self {
        Self(b)
    }

    #[cfg(not(any(test, feature = "diagnostics")))]
    pub(crate) const fn from_byte(b: u8) -> Self {
        Self(b)
    }

    #[cfg(any(test, feature = "diagnostics"))]
    #[doc(hidden)]
    pub const fn to_byte(self) -> u8 {
        self.0
    }

    #[cfg(not(any(test, feature = "diagnostics")))]
    pub(crate) const fn to_byte(self) -> u8 {
        self.0
    }

    pub const fn is_known(self) -> bool {
        self.0 <= Self::LastMoved.0
    }

    pub const fn name(self) -> &'static str {
        match self {
            Self::None => "None",
            Self::Shift => "Shift",
            Self::TopVol => "TopVol",
            Self::LowVol => "LowVol",
            Self::TopProfit => "TopProfit",
            Self::All => "All",
            Self::LastSet => "LastSet",
            Self::LastMoved => "LastMoved",
            _ => "Unknown",
        }
    }
}

impl std::fmt::Debug for ReplaceMultiKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if self.is_known() {
            f.write_str(self.name())
        } else {
            write!(f, "Unknown({})", self.0)
        }
    }
}
