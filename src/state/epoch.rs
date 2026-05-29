//! Wrap-safe epoch comparison matching Delphi `EpochIsOK`.
//!
//! Used to decide whether an incoming value `new` is "newer than" the last
//! known `last`, under these conditions:
//! - the u16 epoch wraps around every ~64K events;
//! - reorder caused by UDP / WiFi/cellular handoff may deliver a legitimate
//!   update "from the past" by a few units;
//! - the server may reboot and restart the counter from scratch.
//!
//! The algorithm matches Delphi `MoonProtoFunc.pas:188-203 EpochIsOK`:
//! - `last == new` → duplicate, reject;
//! - `(last - new) mod 2^16 <= 100` → stale (older than `last`), reject;
//! - otherwise → newer, accept.

const STALE_WINDOW: u16 = 100;

/// Wrap-safe epoch comparison. Returns `true` if `new` is genuinely a new
/// value (not a duplicate, not stale).
///
/// Usage pattern:
/// ```ignore
/// let last = self.last_epoch;
/// if !epoch_is_ok(last, incoming.epoch) {
///     return;  // duplicate or stale — drop the packet
/// }
/// self.last_epoch = incoming.epoch;
/// // ... apply update
/// ```
///
/// See `MoonProtoFunc.pas:188-203`: Delphi uses exactly a window of `100`, not
/// the RFC 1982 half-cycle.
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
        // Legitimate reorder within a small distance — stale.
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
        // last close to u16::MAX, new close to 0 — this is a wrap forward, accept.
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
                "backward by {backward} from {last} -> new={new} must be stale"
            );
        }
        assert!(epoch_is_ok(last, last.wrapping_sub(101)));
    }
}
