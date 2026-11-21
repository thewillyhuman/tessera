// SPDX-License-Identifier: Apache-2.0

//! Text with units, as `LogQL` reads it: durations as seconds, byte sizes
//! as bytes, and lines without ANSI colour codes.

/// `1.5s`, `250ms`, `1h30m`, `2d` as seconds; `None` for anything else.
#[must_use]
pub fn parse_duration(text: &str) -> Option<f64> {
    let text = text.trim();
    if text.is_empty() {
        return None;
    }
    let mut total = 0.0;
    let mut rest = text;
    while !rest.is_empty() {
        let digits = rest
            .find(|c: char| !(c.is_ascii_digit() || c == '.' || c == '-'))
            .unwrap_or(rest.len());
        let number: f64 = rest[..digits].parse().ok()?;
        rest = &rest[digits..];
        let unit_end = rest
            .find(|c: char| !c.is_ascii_alphabetic() && c != 'µ')
            .unwrap_or(rest.len());
        let unit = &rest[..unit_end];
        rest = &rest[unit_end..];
        let seconds = match unit {
            "ns" => 1e-9,
            "us" | "µs" => 1e-6,
            "ms" => 1e-3,
            "s" => 1.0,
            "m" => 60.0,
            "h" => 3600.0,
            "d" => 86_400.0,
            "w" => 604_800.0,
            _ => return None,
        };
        total += number * seconds;
    }
    Some(total)
}

/// `512`, `2KB`, `1.5MiB`, `3GB` as bytes; decimal units are powers of
/// a thousand and binary units powers of 1024.
#[must_use]
pub fn parse_bytes(text: &str) -> Option<f64> {
    let text = text.trim();
    let digits = text
        .find(|c: char| !(c.is_ascii_digit() || c == '.'))
        .unwrap_or(text.len());
    let number: f64 = text[..digits].parse().ok()?;
    let factor = match text[digits..].trim().to_ascii_lowercase().as_str() {
        "" | "b" => 1.0,
        "kb" => 1e3,
        "mb" => 1e6,
        "gb" => 1e9,
        "tb" => 1e12,
        "kib" => 1024.0,
        "mib" => 1024.0 * 1024.0,
        "gib" => 1024.0 * 1024.0 * 1024.0,
        "tib" => 1024.0 * 1024.0 * 1024.0 * 1024.0,
        _ => return None,
    };
    Some(number * factor)
}

/// `text` without ANSI escape sequences (`ESC [ ... m` and friends).
#[must_use]
pub fn decolorize(text: &str) -> String {
    let mut out = String::with_capacity(text.len());
    let mut chars = text.chars().peekable();
    while let Some(c) = chars.next() {
        if c == '\u{1b}' && chars.peek() == Some(&'[') {
            chars.next();
            for inner in chars.by_ref() {
                if inner.is_ascii_alphabetic() {
                    break;
                }
            }
            continue;
        }
        out.push(c);
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn durations_and_sizes_parse_with_units() {
        assert_eq!(parse_duration("1.5s"), Some(1.5));
        assert_eq!(parse_duration("250ms"), Some(0.25));
        assert_eq!(parse_duration("1h30m"), Some(5400.0));
        assert_eq!(parse_duration("fast"), None);
        assert_eq!(parse_duration(""), None);
        assert_eq!(parse_bytes("512"), Some(512.0));
        assert_eq!(parse_bytes("2KB"), Some(2000.0));
        assert_eq!(parse_bytes("1.5MiB"), Some(1.5 * 1024.0 * 1024.0));
        assert_eq!(parse_bytes("big"), None);
    }

    #[test]
    fn colour_codes_are_stripped() {
        assert_eq!(decolorize("\u{1b}[31merror\u{1b}[0m done"), "error done");
        assert_eq!(decolorize("plain"), "plain");
    }
}
