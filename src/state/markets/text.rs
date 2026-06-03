//! Delphi `SameText`-style ASCII helpers for market/currency identifiers.

pub(super) fn same_text_ascii(left: &str, right: &str) -> bool {
    left.eq_ignore_ascii_case(right)
}

pub(super) fn starts_text_ascii(text: &str, prefix: &str) -> bool {
    text.as_bytes()
        .get(..prefix.len())
        .is_some_and(|head| head.eq_ignore_ascii_case(prefix.as_bytes()))
}

pub(super) fn contains_text_ascii(text: &str, needle: &str) -> bool {
    if needle.is_empty() {
        return true;
    }
    let text = text.as_bytes();
    let needle = needle.as_bytes();
    if needle.len() > text.len() {
        return false;
    }
    text.windows(needle.len())
        .any(|window| window.eq_ignore_ascii_case(needle))
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
