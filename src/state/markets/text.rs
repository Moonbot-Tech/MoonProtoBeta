//! Delphi `SameText`-style ASCII helpers for market/currency identifiers.

pub(super) fn same_text_ascii(left: &str, right: &str) -> bool {
    left.eq_ignore_ascii_case(right)
}

pub(super) fn replace_text_ascii_case_insensitive(input: &str, from: &str, to: &str) -> String {
    if from.is_empty() {
        return input.to_string();
    }
    let bytes = input.as_bytes();
    let needle = from.as_bytes();
    let mut out = String::with_capacity(input.len());
    let mut last = 0usize;
    let mut i = 0usize;
    while i + needle.len() <= bytes.len() {
        let matched = bytes[i..i + needle.len()]
            .iter()
            .zip(needle.iter())
            .all(|(a, b)| a.eq_ignore_ascii_case(b));
        if matched && input.is_char_boundary(i) && input.is_char_boundary(i + needle.len()) {
            out.push_str(&input[last..i]);
            out.push_str(to);
            i += needle.len();
            last = i;
        } else {
            i += 1;
        }
    }
    out.push_str(&input[last..]);
    out
}
