//! Delphi ordinal types used by the `MPC_Order` channel.
//!
//! These wrappers preserve unknown raw bytes because Delphi reads packed enum
//! fields with `ms.Read(..., SizeOf(...))` and does not reject future ordinals.

/// TOrderType (Vars.pas:57): O_SELL=0, O_BUY=1, O_BuyStop=2, O_BuyLimit=3.
#[derive(Clone, Copy, PartialEq, Eq, Hash)]
pub struct OrderType(pub u8);

#[allow(non_upper_case_globals)]
impl OrderType {
    pub const Sell: Self = Self(0);
    pub const Buy: Self = Self(1);
    pub const BuyStop: Self = Self(2);
    pub const BuyLimit: Self = Self(3);

    /// Сохранить raw Delphi ordinal byte.
    pub const fn from_byte(b: u8) -> Self {
        Self(b)
    }

    pub const fn to_byte(self) -> u8 {
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

impl std::fmt::Debug for OrderType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if self.is_known() {
            f.write_str(self.name())
        } else {
            write!(f, "Unknown({})", self.0)
        }
    }
}

/// TOrderWorkerStatus (MarketsU.pas:39) — состояние торгового ордера в state machine.
///
/// Standard flow для long-позиции:
/// ```text
///   None ──► BuySet ──► BuyDone ──► SellSet ──► SelLDone
///             │           │           │            │
///             ▼           ▼           ▼            ▼
///          BuyFail    BuyCancel   SellFail    SellCancel
/// ```
///
/// **Terminal states** (ордер закрыт, дальнейших переходов не будет):
/// `SelLDone`, `SelLAlmostDone`, `BuyFail`, `BuyCancel`, `SellFail`, `SellCancel`.
///
/// **Phase semantics** (для UI группировки):
/// - **Buy phase** (`BuySet`/`BuyDone`/`BuyFail`/`BuyCancel`) — ожидание/исполнение
///   входа в позицию.
/// - **Sell phase** (`SellSet`/`SelLAlmostDone`/`SelLDone`/`SellFail`/`SellCancel`) —
///   выход из позиции (take-profit / stop-loss / manual close).
/// - `SelLAlmostDone` — sell уже завершился во время replace/market-stop path,
///   в Delphi worker выходит из цикла так же как при финальных sell-statuses.
///
/// **Server constraints** (см. ARCHITECTURE.md §17 sync state):
/// - Откат фазы запрещён сервером (нельзя из SellSet вернуться в BuySet).
/// - Внутри фазы переходы по статусам валидны (BuySet → BuyDone).
/// - Terminal состояние не меняется.
#[derive(Clone, Copy, PartialEq, Eq, Hash)]
pub struct OrderWorkerStatus(pub u8);

#[allow(non_upper_case_globals)]
impl OrderWorkerStatus {
    /// Initial state — ордер ещё не отправлен на биржу.
    pub const None: Self = Self(0);
    /// Buy-ордер не удался (отказ биржи, недостаточно баланса, т.п.). Terminal.
    pub const BuyFail: Self = Self(1);
    /// Buy-ордер размещён на бирже, ждём fill.
    pub const BuySet: Self = Self(2);
    /// Buy-ордер отменён (пользователем или системой). Terminal.
    pub const BuyCancel: Self = Self(3);
    /// Buy-ордер исполнен — позиция открыта.
    pub const BuyDone: Self = Self(4);
    /// Sell-ордер не удался. Terminal.
    pub const SellFail: Self = Self(5);
    /// Sell-ордер (закрытие/take-profit) размещён, ждём fill.
    pub const SellSet: Self = Self(6);
    /// Sell-ордер отменён. Terminal.
    pub const SellCancel: Self = Self(7);
    /// Sell-ордер полностью исполнен — позиция закрыта.
    pub const SelLDone: Self = Self(8);
    /// Sell завершился через intermediate path; terminal для worker/state.
    pub const SelLAlmostDone: Self = Self(9);

    /// Сохранить raw Delphi ordinal byte.
    pub const fn from_byte(b: u8) -> Self {
        Self(b)
    }

    pub const fn to_byte(self) -> u8 {
        self.0
    }

    pub const fn is_known(self) -> bool {
        self.0 <= Self::SelLAlmostDone.0
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
            Self::SelLDone => "SellDone",
            Self::SelLAlmostDone => "SellAlmostDone",
            _ => "Unknown",
        }
    }

    /// Terminal status — ордер закрыт, воркер удалить.
    pub const fn is_terminal(self) -> bool {
        matches!(
            self,
            Self::SelLDone
                | Self::SelLAlmostDone
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
pub struct FixedPosition(pub u8);

#[allow(non_upper_case_globals)]
impl FixedPosition {
    pub const Both: Self = Self(0);
    pub const Long: Self = Self(1);
    pub const Short: Self = Self(2);

    pub const fn from_byte(b: u8) -> Self {
        Self(b)
    }

    pub const fn to_byte(self) -> u8 {
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

/// Sell-side `TMoveAllCmdType` (MoonProtoTradeStruct.pas:148 inline comment).
/// Описывает интерпретацию параметра `Price`/`PriceZone` в `TMoveAllSellsCommand`.
#[derive(Clone, Copy, PartialEq, Eq, Hash)]
pub struct MoveAllCmdType(pub u8);

#[allow(non_upper_case_globals)]
impl MoveAllCmdType {
    /// `MoveKind` — двигать всех по правилу из `ReplaceMultiKind`.
    pub const MoveKind: Self = Self(0);
    /// `PriceZone` — двигать тех чья цена в зоне `[price_zone.min_p, price_zone.max_p]`.
    pub const PriceZone: Self = Self(1);
    /// `Pers` — персональный режим (см. Delphi server logic).
    pub const Pers: Self = Self(2);

    pub const fn from_byte(b: u8) -> Self {
        Self(b)
    }

    pub const fn to_byte(self) -> u8 {
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

/// Buy-side `TMoveAllBuysCommand.CmdType`.
///
/// Delphi `TMoveAllBuysCommand` supports only `0: MoveKind` and `2: Pers`;
/// there is no buy-side `PriceZone` mode and the server buy branch ignores
/// `CmdType=1`.
#[derive(Clone, Copy, PartialEq, Eq, Hash)]
pub struct MoveAllBuysCmdType(pub u8);

#[allow(non_upper_case_globals)]
impl MoveAllBuysCmdType {
    pub const MoveKind: Self = Self(0);
    pub const Pers: Self = Self(2);

    pub const fn from_byte(b: u8) -> Self {
        Self(b)
    }

    pub const fn to_byte(self) -> u8 {
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
pub struct ReplaceMultiKind(pub u8);

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

    pub const fn from_byte(b: u8) -> Self {
        Self(b)
    }

    pub const fn to_byte(self) -> u8 {
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
