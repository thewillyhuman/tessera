// SPDX-License-Identifier: Apache-2.0

//! Line and label templates (`LogQL` `line_format`, `label_format`): the
//! subset of Go templates dashboards use, `{{.label}}` and `{{__line__}}`.
//! An unknown label renders as empty text.

/// Renders `template` with `label` giving each label's value and `line`
/// the current line.
pub fn render(template: &str, label: impl Fn(&str) -> Option<String>, line: &str) -> String {
    let mut out = String::with_capacity(template.len());
    let mut rest = template;
    while let Some(start) = rest.find("{{") {
        out.push_str(&rest[..start]);
        let after = &rest[start + 2..];
        let Some(end) = after.find("}}") else {
            out.push_str(&rest[start..]);
            return out;
        };
        let inner = after[..end].trim();
        match inner.strip_prefix('.') {
            Some(name) => out.push_str(&label(name).unwrap_or_default()),
            None if inner == "__line__" => out.push_str(line),
            None => {}
        }
        rest = &after[end + 2..];
    }
    out.push_str(rest);
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn renders_labels_and_the_line() {
        let label = |name: &str| match name {
            "method" => Some("GET".to_owned()),
            "status" => Some("200".to_owned()),
            _ => None,
        };
        assert_eq!(
            render(
                "{{.method}} -> {{ .status }} [{{__line__}}] {{.missing}}!",
                label,
                "body"
            ),
            "GET -> 200 [body] !"
        );
        assert_eq!(render("plain {{unclosed", |_| None, ""), "plain {{unclosed");
    }
}
