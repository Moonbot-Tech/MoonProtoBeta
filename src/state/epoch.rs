//! Wrap-safe epoch comparison — RFC 1982 serial number arithmetic.
//!
//! Используется для определения "новее ли" пришедшее значение `new` чем последнее
//! известное `last` в условиях:
//! - u16 epoch обёртывается каждые ~64K событий;
//! - reorder из-за UDP / WiFi/cellular handoff может прислать legitimate update
//!   "из прошлого" на несколько штук;
//! - сервер может ребутнуть и начать счёт заново (тогда фильтр backDist
//!   защищает от ошибочного принятия "старых" значений после рестарта).
//!
//! Алгоритм соответствует Delphi `MoonProtoFunc.pas:188-203 EpochIsOK`:
//! - `last == new` → дубликат, reject;
//! - `(last - new) mod 2^16 <= STALE_WINDOW` → stale (старее `last`), reject;
//! - иначе → новее, accept.
//!
//! Окно `STALE_WINDOW = u16::MAX/2 = 32767` — RFC 1982 half-cycle. Это
//! максимально широкое окно которое позволяет различать direction (вперёд/назад)
//! у обёрнутого counter'а. Любое меньшее окно (например, 100 из старой реализации)
//! на high-frequency reorder теряет legitimate updates тихо.

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
/// См. `audit_rust_quality #1` (унификация: до этого `balances.rs` имела
/// устаревшее окно 100 → high-freq legitimate updates тихо терялись).
pub fn epoch_is_ok(last: u16, new: u16) -> bool {
    if last == new {
        return false; // duplicate
    }
    // RFC 1982 serial number comparison: `new` is "ahead" of `last` iff backward
    // distance > halfway round the u16 cycle.
    last.wrapping_sub(new) > u16::MAX / 2
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
    fn wrap_around_forward_accepted() {
        // last близко к u16::MAX, new близко к 0 — это wrap forward, accept.
        assert!(epoch_is_ok(u16::MAX - 5, 0));
        assert!(epoch_is_ok(u16::MAX - 5, 100));
        assert!(epoch_is_ok(60_000, 100));
    }

    #[test]
    fn half_cycle_boundary() {
        // Окно ровно u16::MAX/2 = 32767. (last - new) > 32767 — accept.
        // 0 - new > 32767 для new < 0 (wrapping) или new > 32768 (wrapping).
        // last=0, new=32768 → 0 - 32768 = -32768 = 0x8000 = 32768 → > 32767 → accept (forward через wrap).
        assert!(epoch_is_ok(0, 32768));
        // last=0, new=32767 → 0 - 32767 = -32767 = 0x8001 = 32769 → > 32767 → accept (через wrap).
        assert!(epoch_is_ok(0, 32767));
    }

    #[test]
    fn high_freq_reorder_window_is_wide_enough() {
        // На 10 events/sec * 32767 = ~55 минут можем переживать reorder без
        // потери legitimate updates. Это значит что в практике для торговых
        // приложений (события идут пачками по 10-100/sec) окно безопасно широкое.
        let last: u16 = 1000;
        // Все эти "из прошлого" значения должны быть rejected как stale (нет
        // ошибочного wrap-acceptance).
        for backward in 1..=1000 {
            let new = last.wrapping_sub(backward);
            assert!(!epoch_is_ok(last, new),
                "backward by {backward} from {last} → new={new} должно быть stale");
        }
    }
}
