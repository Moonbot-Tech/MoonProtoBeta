//! Wrap-safe comparison for server-owned `u16` epochs.
//!
//! Equal values are duplicates. A value up to 1000 steps behind the current
//! watermark is stale. Every other value is forward progress, including a
//! legitimate wrap through zero.

const STALE_WINDOW: u16 = 1000;

/// Returns `true` when `new` is forward progress rather than a duplicate or a
/// stale reordered value.
///
/// This deliberately uses the production protocol's 1000-step stale window,
/// not the RFC 1982 half-cycle.
pub(crate) fn epoch_is_ok(last: u16, new: u16) -> bool {
    if last == new {
        return false;
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
        assert!(epoch_is_ok(1000, 30_000));
    }

    #[test]
    fn small_backward_rejected_as_stale() {
        assert!(!epoch_is_ok(2000, 1999));
        assert!(!epoch_is_ok(2000, 1500));
        assert!(!epoch_is_ok(30_000, 29_000));
    }

    #[test]
    fn backward_more_than_1000_is_accepted() {
        assert!(epoch_is_ok(30_000, 28_999));
        assert!(epoch_is_ok(200, 64_735));
    }

    #[test]
    fn wrap_around_forward_accepted() {
        assert!(epoch_is_ok(u16::MAX - 5, 0));
        assert!(epoch_is_ok(u16::MAX - 5, 100));
        assert!(epoch_is_ok(60_000, 100));
    }

    #[test]
    fn stale_window_boundary_matches_protocol() {
        assert!(!epoch_is_ok(2000, 1000));
        assert!(epoch_is_ok(2000, 999));
    }

    #[test]
    fn complete_stale_window_is_rejected() {
        let last = 2000u16;
        for backward in 1..=STALE_WINDOW {
            let new = last.wrapping_sub(backward);
            assert!(
                !epoch_is_ok(last, new),
                "backward by {backward} from {last} -> new={new} must be stale"
            );
        }
        assert!(epoch_is_ok(last, last.wrapping_sub(STALE_WINDOW + 1)));
    }
}
