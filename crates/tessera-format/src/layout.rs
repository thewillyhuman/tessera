// SPDX-License-Identifier: LicenseRef-Tessera-Pending

//! How a file is laid out (spec section 8): its sort key, its row-group
//! size and the indexes built into its sidecar. Every layout decision is
//! recorded per file (P3), so files of different layouts coexist.

use tessera_types::Kind;
use tessera_types::event::{BODY, NAME, SERIES_ID, SEVERITY_NUMBER, TIMESTAMP, TRACE_ID};

use crate::sidecar::IndexSpec;

/// Rows per row group of a compacted file: small enough for row-group
/// pruning to matter, large enough for the encoders to work well.
pub const DEFAULT_ROW_GROUP_ROWS: usize = 32_768;

/// The physical layout of one file.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct Layout {
    /// The columns rows are sorted by, in order. `timestamp` is appended
    /// when it is not already the last key, so that time pruning works
    /// inside every sort-key group.
    pub sort_key: Vec<String>,
    /// Rows per row group; one row group when larger than the file.
    pub row_group_rows: usize,
    /// The indexes of the sidecar; none means no sidecar.
    pub indexes: IndexSpec,
}

impl Layout {
    /// The layout of an L1 file written by ingest flush: sorted by time,
    /// one row group, statistics only (section 6.9).
    #[must_use]
    pub fn l1() -> Self {
        Layout {
            sort_key: vec![TIMESTAMP.to_owned()],
            row_group_rows: usize::MAX,
            indexes: IndexSpec::default(),
        }
    }

    /// The default layout of a kind before any workload exists (section
    /// 8.5), restricted to the columns files have today: attribute sort
    /// keys (`service`, `name`) wait for column promotion (M8).
    #[must_use]
    pub fn default_for(kind: Kind) -> Self {
        let bloom_on_trace = IndexSpec {
            bloom: vec![TRACE_ID.to_owned()],
            ..IndexSpec::default()
        };
        match kind {
            Kind::Log => Layout {
                sort_key: vec![SEVERITY_NUMBER.to_owned(), TIMESTAMP.to_owned()],
                row_group_rows: DEFAULT_ROW_GROUP_ROWS,
                indexes: IndexSpec {
                    term: vec![BODY.to_owned()],
                    trigram: vec![BODY.to_owned()],
                    bloom: vec![TRACE_ID.to_owned()],
                },
            },
            // Spans and events sort by name, then time (section 8.5);
            // the service comes first once attribute columns exist (M8).
            Kind::Span | Kind::Event => Layout {
                sort_key: vec![NAME.to_owned(), TIMESTAMP.to_owned()],
                row_group_rows: DEFAULT_ROW_GROUP_ROWS,
                indexes: bloom_on_trace,
            },
            // Metrics sort by name, series and time (section 8.5), so that
            // a series is contiguous and selectors on the name prune.
            Kind::Metric => Layout {
                sort_key: vec![NAME.to_owned(), SERIES_ID.to_owned(), TIMESTAMP.to_owned()],
                row_group_rows: DEFAULT_ROW_GROUP_ROWS,
                indexes: IndexSpec::default(),
            },
            Kind::Alert => Layout {
                sort_key: vec![NAME.to_owned(), TIMESTAMP.to_owned()],
                row_group_rows: DEFAULT_ROW_GROUP_ROWS,
                indexes: IndexSpec::default(),
            },
        }
    }

    /// The sort key with `timestamp` as its last column.
    #[must_use]
    pub fn full_sort_key(&self) -> Vec<String> {
        let mut key: Vec<String> = self
            .sort_key
            .iter()
            .filter(|c| c.as_str() != TIMESTAMP)
            .cloned()
            .collect();
        key.push(TIMESTAMP.to_owned());
        key
    }
}

impl Default for Layout {
    fn default() -> Self {
        Layout::l1()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn timestamp_is_always_the_last_sort_key() {
        let layout = Layout {
            sort_key: vec!["timestamp".into(), "severity_number".into()],
            ..Layout::l1()
        };
        assert_eq!(layout.full_sort_key(), vec!["severity_number", "timestamp"]);
        assert_eq!(Layout::l1().full_sort_key(), vec!["timestamp"]);
    }

    #[test]
    fn logs_get_text_indexes_by_default() {
        let logs = Layout::default_for(Kind::Log);
        assert_eq!(logs.indexes.term, vec!["body"]);
        assert_eq!(logs.indexes.trigram, vec!["body"]);
        assert_eq!(logs.indexes.bloom, vec!["trace_id"]);
        assert!(Layout::default_for(Kind::Metric).indexes.bloom.is_empty());
        assert_eq!(
            Layout::default_for(Kind::Span).indexes.bloom,
            vec!["trace_id"]
        );
        assert_eq!(
            Layout::default_for(Kind::Span).sort_key,
            vec!["name", "timestamp"]
        );
        assert_eq!(
            Layout::default_for(Kind::Event).sort_key,
            vec!["name", "timestamp"]
        );
        assert_eq!(
            Layout::default_for(Kind::Metric).sort_key,
            vec!["name", "series_id", "timestamp"]
        );
    }
}
