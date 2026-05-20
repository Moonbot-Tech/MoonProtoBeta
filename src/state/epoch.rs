//! Wrap-safe epoch comparison matching Delphi `EpochIsOK`.
//!
//! Используется для определения "новее ли" пришедшее значение `new` чем последнее
//! известное `last` в условиях:
//! - u16 epoch обёртывается каждые ~64K событий;
//! - reorder из-за UDP / WiFi/cellular handoff может прислать legitimate update
//!   "из прошлого" на несколько штук;
//! - сервер может ребутнуть и начать счёт заново.
//!
//! Алгоритм соответствует Delphi `MoonProtoFunc.pas:188-203 EpochIsOK`:
//! - `last == new` → дубликат, reject;
//! - `(last - new) mod 2^16 <= 100` → stale (старее `last`), reject;
//! - иначе → новее, accept.

const STALE_WINDOW: u16 = 100;

/// Wrap-safe epoch comparison. Возвращает `true` если `new` — действительно
/// новое значение (не дубликат, не stale).
///
/// Шаблон использования:
/// ```ignore
/// let last = self.last_epoch;
/// if !epoch_is_ok(last, incoming.epoch) {
///     return;  // duplicate или stale — игнорируем пакет
/// }
/// self.last_epoch = incoming.epoch;
/// // ... apply update
/// ```
///
/// См. `MoonProtoFunc.pas:188-203`: Delphi использует именно окно `100`, а не
/// RFC 1982 half-cycle.
pub fn epoch_is_ok(last: u16, new: u16) -> bool {
    if last == new {
        return false; // duplicate
    }
    last.wrapping_sub(new) > STALE_WINDOW
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn duplicate_is_rejected() {
        assert!(!epoch_is_ok(42, 42));
        assert!(!epoch_is_ok(0, 0));
        assert!(!epoch_is_ok(u16::MAX, u16::MAX));
    }

    #[test]
    fn normal_forward_accepted() {
        assert!(epoch_is_ok(0, 1));
        assert!(epoch_is_ok(100, 200));
        assert!(epoch_is_ok(1000, 30000));
    }

    #[test]
    fn small_backward_rejected_as_stale() {
        // Legitimate reorder в пределах малой дистанции — stale.
        assert!(!epoch_is_ok(100, 99));
        assert!(!epoch_is_ok(100, 50));
        assert!(!epoch_is_ok(30_000, 29_900));
    }

    #[test]
    fn backward_more_than_100_is_accepted_like_delphi() {
        assert!(epoch_is_ok(30_000, 29_899));
        assert!(epoch_is_ok(200, 65500));
    }

    #[test]
    fn wrap_around_forward_accepted() {
        // last близко к u16::MAX, new близко к 0 — это wrap forward, accept.
        assert!(epoch_is_ok(u16::MAX - 5, 0));
        assert!(epoch_is_ok(u16::MAX - 5, 100));
        assert!(epoch_is_ok(60_000, 100));
    }

    #[test]
    fn delphi_stale_window_boundary() {
        // Delphi rejects only `backDist <= 100`; 101 is already accepted.
        assert!(!epoch_is_ok(1000, 900));
        assert!(epoch_is_ok(1000, 899));
    }

    #[test]
    fn stale_window_matches_delphi_constant() {
        let last: u16 = 1000;
        for backward in 1..=100 {
            let new = last.wrapping_sub(backward);
            assert!(
                !epoch_is_ok(last, new),
                "backward by {backward} from {last} → new={new} должно быть stale"
            );
        }
        assert!(epoch_is_ok(last, last.wrapping_sub(101)));
    }
}
