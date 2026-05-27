//! Transferable asset read model maintained by Active Lib.
//!
//! This is a separate entity from per-market balances. Delphi stores it in
//! `Markets.FAssets[TExchangeKind]` and refreshes Spot/Futures/Quarterly
//! wallets independently through `emk_UpdateTransferAssets`.

use crate::commands::engine_api::TransferAsset;

/// Delphi `TExchangeKind = (EX_Spot, EX_Futures, EX_QFutures)`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(u8)]
pub enum ExchangeKind {
    Spot = 0,
    Futures = 1,
    Quarterly = 2,
}

impl ExchangeKind {
    pub const ALL: [Self; 3] = [Self::Spot, Self::Futures, Self::Quarterly];

    #[inline]
    pub const fn to_byte(self) -> u8 {
        self as u8
    }

    #[inline]
    pub const fn as_index(self) -> usize {
        self as usize
    }

    pub const fn name(self) -> &'static str {
        match self {
            Self::Spot => "Spot",
            Self::Futures => "Futures",
            Self::Quarterly => "Quarterly",
        }
    }

    pub const fn from_byte(value: u8) -> Option<Self> {
        match value {
            0 => Some(Self::Spot),
            1 => Some(Self::Futures),
            2 => Some(Self::Quarterly),
            _ => None,
        }
    }
}

impl From<ExchangeKind> for u8 {
    fn from(value: ExchangeKind) -> Self {
        value.to_byte()
    }
}

/// Event emitted after an async transfer-assets refresh request completes.
#[derive(Debug, Clone, PartialEq)]
pub enum TransferAssetsEvent {
    Updated {
        kind: ExchangeKind,
        request_uid: u64,
        count: usize,
        nonzero_count: usize,
        revision: u64,
    },
    RefreshCompleted {
        request_id: u64,
        requested: usize,
        updated: usize,
        failed: usize,
        revision: u64,
    },
    UpdateFailed {
        kind: ExchangeKind,
        request_uid: Option<u64>,
        error: String,
    },
    TransferApplied {
        asset: String,
        qty: f64,
        from: ExchangeKind,
        to: ExchangeKind,
        request_uid: u64,
        revision: u64,
    },
}

/// Current transferable asset lists by exchange wallet kind.
#[derive(Debug, Clone, Default)]
pub struct TransferAssetsState {
    by_kind: [Vec<TransferAsset>; 3],
    revision: u64,
    revision_by_kind: [u64; 3],
}

impl TransferAssetsState {
    pub fn new() -> Self {
        Self::default()
    }

    /// Read the current asset list for one wallet kind.
    pub fn get(&self, kind: ExchangeKind) -> &[TransferAsset] {
        &self.by_kind[kind.as_index()]
    }

    /// Iterate all wallet lists in Delphi enum order.
    pub fn iter(&self) -> impl Iterator<Item = (ExchangeKind, &[TransferAsset])> {
        ExchangeKind::ALL
            .into_iter()
            .map(|kind| (kind, self.get(kind)))
    }

    /// Global monotonically increasing state revision.
    pub fn revision(&self) -> u64 {
        self.revision
    }

    /// Last revision that updated one wallet kind. Zero means never updated.
    pub fn kind_revision(&self, kind: ExchangeKind) -> u64 {
        self.revision_by_kind[kind.as_index()]
    }

    pub(crate) fn apply_update(
        &mut self,
        kind: ExchangeKind,
        request_uid: u64,
        assets: Vec<TransferAsset>,
    ) -> TransferAssetsEvent {
        self.revision = self.revision.wrapping_add(1).max(1);
        self.revision_by_kind[kind.as_index()] = self.revision;
        let nonzero_count = assets
            .iter()
            .filter(|asset| asset.amount != 0.0 || asset.total != 0.0)
            .count();
        let count = assets.len();
        self.by_kind[kind.as_index()] = assets;
        TransferAssetsEvent::Updated {
            kind,
            request_uid,
            count,
            nonzero_count,
            revision: self.revision,
        }
    }

    pub(crate) fn apply_transfer_like_delphi(
        &mut self,
        asset: &str,
        qty: f64,
        from: ExchangeKind,
        to: ExchangeKind,
        request_uid: u64,
    ) -> TransferAssetsEvent {
        self.revision = self.revision.wrapping_add(1).max(1);
        self.revision_by_kind[from.as_index()] = self.revision;
        self.revision_by_kind[to.as_index()] = self.revision;

        let to_assets = &mut self.by_kind[to.as_index()];
        if let Some(row) = to_assets
            .iter_mut()
            .find(|row| row.currency.eq_ignore_ascii_case(asset))
        {
            row.amount += qty;
            row.total += qty;
        } else {
            to_assets.push(TransferAsset {
                currency: asset.to_string(),
                amount: qty,
                total: qty,
            });
        }

        if let Some(row) = self.by_kind[from.as_index()]
            .iter_mut()
            .find(|row| row.currency.eq_ignore_ascii_case(asset))
        {
            row.amount = (row.amount - qty).max(0.0);
            row.total = (row.total - qty).max(0.0);
        }

        TransferAssetsEvent::TransferApplied {
            asset: asset.to_string(),
            qty,
            from,
            to,
            request_uid,
            revision: self.revision,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn asset(currency: &str, amount: f64, total: f64) -> TransferAsset {
        TransferAsset {
            currency: currency.to_string(),
            amount,
            total,
        }
    }

    #[test]
    fn exchange_kind_matches_delphi_ordinals() {
        assert_eq!(ExchangeKind::from_byte(0), Some(ExchangeKind::Spot));
        assert_eq!(ExchangeKind::from_byte(1), Some(ExchangeKind::Futures));
        assert_eq!(ExchangeKind::from_byte(2), Some(ExchangeKind::Quarterly));
        assert_eq!(ExchangeKind::from_byte(3), None);
        assert_eq!(ExchangeKind::ALL.map(ExchangeKind::to_byte), [0, 1, 2]);
    }

    #[test]
    fn apply_update_replaces_only_one_kind() {
        let mut state = TransferAssetsState::new();
        state.apply_update(ExchangeKind::Spot, 10, vec![asset("USDT", 1.0, 2.0)]);
        state.apply_update(ExchangeKind::Futures, 11, vec![asset("BTC", 0.0, 0.5)]);

        assert_eq!(state.get(ExchangeKind::Spot)[0].currency, "USDT");
        assert_eq!(state.get(ExchangeKind::Futures)[0].currency, "BTC");
        assert!(state.get(ExchangeKind::Quarterly).is_empty());
        assert_eq!(state.kind_revision(ExchangeKind::Spot), 1);
        assert_eq!(state.kind_revision(ExchangeKind::Futures), 2);
        assert_eq!(state.revision(), 2);
    }

    #[test]
    fn apply_transfer_moves_amounts_like_delphi_after_success() {
        let mut state = TransferAssetsState::new();
        state.apply_update(ExchangeKind::Spot, 10, vec![asset("USDT", 10.0, 12.0)]);
        state.apply_update(ExchangeKind::Futures, 11, vec![asset("USDT", 1.0, 2.0)]);

        let ev = state.apply_transfer_like_delphi(
            "usdt",
            3.0,
            ExchangeKind::Spot,
            ExchangeKind::Futures,
            99,
        );

        assert!(matches!(
            ev,
            TransferAssetsEvent::TransferApplied {
                from: ExchangeKind::Spot,
                to: ExchangeKind::Futures,
                request_uid: 99,
                ..
            }
        ));
        assert_eq!(state.get(ExchangeKind::Spot)[0].amount, 7.0);
        assert_eq!(state.get(ExchangeKind::Spot)[0].total, 9.0);
        assert_eq!(state.get(ExchangeKind::Futures)[0].amount, 4.0);
        assert_eq!(state.get(ExchangeKind::Futures)[0].total, 5.0);
    }
}
