// SPDX-License-Identifier: LicenseRef-Tessera-Pending

//! Pruning from manifest data only (section 10.4, stages 1 to 4; QE-7).
//!
//! 1. Partitions, by the query's time range: the caller lists only the
//!    partitions whose window overlaps it.
//! 2. File time ranges, from the manifest.
//! 3. Path presence, against each file's attribute-path Bloom filter:
//!    files carry none yet, so every file passes.
//! 4. Column statistics, min and max per file from the manifest, against
//!    the bounds the scan's predicates imply.
//!
//! Each stage sees only what survived the previous one, and the report
//! says how many files each eliminated (QE-8).

use std::collections::BTreeMap;

use tessera_format::Bound;
use tessera_ir::trace::NEST_LEFT;
use tessera_ir::{BinaryOp, Expr, Literal, Scan};
use tessera_types::event::{
    CLASS_BOOL, CLASS_FLOAT, CLASS_INT, CLASS_TEXT, Scalar, TIMESTAMP, sub_column,
};
use tessera_types::{FileMeta, TimeRange, Timestamp};

/// How many files each stage let through.
#[derive(Clone, Copy, PartialEq, Eq, Debug, Default)]
pub struct PruneReport {
    /// Files in the partitions considered.
    pub files: usize,
    /// Files whose time range overlaps the query's.
    pub after_time_range: usize,
    /// Files that may hold a referenced path.
    pub after_path_presence: usize,
    /// Files whose column statistics admit the predicates.
    pub after_statistics: usize,
}

/// The files of `files` the scan must read, and what pruned the rest.
pub fn prune_files<'a>(
    scan: &Scan,
    files: impl IntoIterator<Item = &'a FileMeta>,
) -> (Vec<&'a FileMeta>, PruneReport) {
    let mut report = PruneReport::default();
    let stored = bounds(&scan.stored_predicates, TimeRange::ALL);
    let bounds = bounds(&scan.predicates, scan.time_range);
    let mut kept = Vec::new();
    for file in files {
        report.files += 1;
        if !scan
            .time_range
            .overlaps_closed(file.min_timestamp, file.max_timestamp)
        {
            continue;
        }
        report.after_time_range += 1;
        report.after_path_presence += 1;
        if !admits(file, &bounds) {
            continue;
        }
        // The stored predicates bound a file only when every trace in it
        // stores its shape (QL-14): a null there is a trace to number.
        if stores_every_trace(file) && !admits(file, &stored) {
            continue;
        }
        report.after_statistics += 1;
        kept.push(file);
    }
    (kept, report)
}

/// The inclusive per-column bounds implied by `predicates` and
/// `time_range`, one per column. Only comparisons of a dedicated column,
/// or of a path a file may hold as one (AD-4), with a constant
/// contribute; anything else is left to row filtering.
#[must_use]
pub fn bounds(predicates: &[Expr], time_range: TimeRange) -> Vec<Bound> {
    let mut by_column: BTreeMap<String, Bound> = BTreeMap::new();
    let mut narrow = |column: &str, min: Option<Scalar>, max: Option<Scalar>| {
        let bound = by_column.entry(column.to_owned()).or_insert_with(|| Bound {
            column: column.to_owned(),
            min: None,
            max: None,
        });
        if let Some(min) = min {
            bound.min = Some(match bound.min.take() {
                Some(current) if !less(&current, &min) => current,
                _ => min,
            });
        }
        if let Some(max) = max {
            bound.max = Some(match bound.max.take() {
                Some(current) if !less(&max, &current) => current,
                _ => max,
            });
        }
    };
    if time_range.start != Timestamp::MIN {
        narrow(
            TIMESTAMP,
            Some(Scalar::Int(time_range.start.as_nanos())),
            None,
        );
    }
    if time_range.end != Timestamp::MAX {
        narrow(
            TIMESTAMP,
            None,
            Some(Scalar::Int(time_range.end.as_nanos().saturating_sub(1))),
        );
    }
    for predicate in predicates {
        let Expr::Binary { op, left, right } = predicate else {
            continue;
        };
        let (op, column, value) = match (left.as_ref(), right.as_ref()) {
            (Expr::Column(c) | Expr::Path { path: c, .. }, Expr::Literal(v)) => (*op, c, v),
            (Expr::Literal(v), Expr::Column(c) | Expr::Path { path: c, .. }) => {
                (op.flipped(), c, v)
            }
            _ => continue,
        };
        let Some(value) = scalar(value) else {
            continue;
        };
        match op {
            BinaryOp::Eq => narrow(column, Some(value.clone()), Some(value)),
            BinaryOp::Lt | BinaryOp::LtEq => narrow(column, None, Some(value)),
            BinaryOp::Gt | BinaryOp::GtEq => narrow(column, Some(value), None),
            _ => {}
        }
    }
    by_column.into_values().collect()
}

fn scalar(literal: &Literal) -> Option<Scalar> {
    match literal {
        Literal::Bool(b) => Some(Scalar::Bool(*b)),
        Literal::Int(i) => Some(Scalar::Int(*i)),
        Literal::Float(f) => Some(Scalar::Float(*f)),
        Literal::Text(s) => Some(Scalar::Text(s.clone())),
        Literal::Timestamp(t) => Some(Scalar::Int(t.as_nanos())),
        Literal::Null | Literal::Duration(_) | Literal::Bytes(_) => None,
    }
}

/// Whether `a < b` for scalars of a comparable kind; false otherwise, so
/// an incomparable pair never narrows a bound.
#[allow(clippy::cast_precision_loss)]
fn less(a: &Scalar, b: &Scalar) -> bool {
    match (a, b) {
        (Scalar::Int(a), Scalar::Int(b)) => a < b,
        (Scalar::Float(a), Scalar::Float(b)) => a < b,
        (Scalar::Int(a), Scalar::Float(b)) => (*a as f64) < *b,
        (Scalar::Float(a), Scalar::Int(b)) => *a < *b as f64,
        (Scalar::Text(a), Scalar::Text(b)) => a < b,
        _ => false,
    }
}

/// The type classes of sub-columns whose values `bound` can match
/// (DM-5): a number bounds the integer and float classes, text the
/// text class, a boolean its own.
fn classes_of(bound: &Bound) -> &'static [&'static str] {
    match bound.min.as_ref().or(bound.max.as_ref()) {
        Some(Scalar::Int(_) | Scalar::Float(_)) => &[CLASS_INT, CLASS_FLOAT],
        Some(Scalar::Text(_)) => &[CLASS_TEXT],
        Some(Scalar::Bool(_)) => &[CLASS_BOOL],
        None => &[],
    }
}

/// Whether every span of the file carries its trace's stored shape
/// (QL-14): the file has the nested-set columns and none is null.
#[must_use]
pub fn stores_every_trace(file: &FileMeta) -> bool {
    file.columns
        .iter()
        .find(|c| c.name == NEST_LEFT)
        .is_some_and(|c| c.null_count == 0)
}

/// Whether the file's statistics admit a row within every bound.
fn admits(file: &FileMeta, bounds: &[Bound]) -> bool {
    bounds.iter().all(|bound| {
        let stats = file
            .columns
            .iter()
            .find(|c| c.name == bound.column)
            .or_else(|| {
                // A path stored per type class: the statistics of the
                // sub-column holding the class the bound can match. Read
                // as text a path gives every class in its text form, so
                // text statistics bound it only when text is all it holds.
                let of_class = |class: &str| {
                    let name = sub_column(&bound.column, class);
                    file.columns.iter().find(|c| c.name == name)
                };
                let classes = classes_of(bound);
                if classes == [CLASS_TEXT]
                    && [CLASS_INT, CLASS_FLOAT, CLASS_BOOL]
                        .iter()
                        .any(|class| of_class(class).is_some())
                {
                    return None;
                }
                classes.iter().find_map(|class| of_class(class))
            });
        let Some(stats) = stats else {
            return true;
        };
        let below = match (&bound.min, &stats.max) {
            (Some(wanted), Some(max)) => less(max, wanted),
            _ => false,
        };
        let above = match (&bound.max, &stats.min) {
            (Some(wanted), Some(min)) => less(wanted, min),
            _ => false,
        };
        !(below || above)
    })
}

#[cfg(test)]
mod tests {
    use tessera_types::event::SEVERITY_NUMBER;
    use tessera_types::{
        ColumnStats, DatasetName, FileId, FileSource, Kind, Level, PartitionId, PoolId, Seq,
        TenantId, Window, WriterId,
    };

    use super::*;

    fn file(id: u128, min: i64, max: i64, severity: (i64, i64)) -> FileMeta {
        FileMeta {
            file_id: FileId::from_u128(id),
            partition: PartitionId {
                tenant: TenantId::DEFAULT,
                dataset: DatasetName::new("prod").unwrap(),
                kind: Kind::Log,
                window: Window::containing(Timestamp::UNIX_EPOCH, Window::DEFAULT_LENGTH).unwrap(),
            },
            pool: PoolId::new("root").unwrap(),
            level: Level::L1,
            source: FileSource::Writer {
                writer: WriterId::from_u128(1),
                after: Seq::ZERO,
                through: Seq::new(1),
            },
            min_timestamp: Timestamp::from_nanos(min),
            max_timestamp: Timestamp::from_nanos(max),
            row_count: 1,
            size_bytes: 1,
            layout_epoch: 0,
            sort_key: vec![],
            columns: vec![ColumnStats {
                name: SEVERITY_NUMBER.into(),
                min: Some(Scalar::Int(severity.0)),
                max: Some(Scalar::Int(severity.1)),
                null_count: 0,
                distinct: None,
                quantiles: Vec::new(),
            }],
            indexes: Vec::new(),
        }
    }

    #[test]
    fn stages_prune_in_order_and_report_counts() {
        let files = vec![
            file(1, 0, 10, (1, 5)),
            file(2, 20, 30, (1, 5)),
            file(3, 25, 40, (17, 20)),
        ];
        let mut scan = Scan::all(
            TenantId::DEFAULT,
            DatasetName::new("prod").unwrap(),
            Kind::Log,
        );
        scan.time_range = TimeRange::new(Timestamp::from_nanos(20), Timestamp::from_nanos(50));
        scan.predicates =
            vec![Expr::column(SEVERITY_NUMBER).binary(BinaryOp::GtEq, Expr::literal(17))];
        let (kept, report) = prune_files(&scan, &files);
        assert_eq!(kept.len(), 1);
        assert_eq!(kept[0].file_id, FileId::from_u128(3));
        assert_eq!(
            report,
            PruneReport {
                files: 3,
                after_time_range: 2,
                after_path_presence: 2,
                after_statistics: 1
            }
        );
    }

    #[test]
    fn bounds_combine_predicates_and_the_time_range() {
        let predicates = vec![
            Expr::column(SEVERITY_NUMBER).binary(BinaryOp::GtEq, Expr::literal(9)),
            Expr::column(SEVERITY_NUMBER).binary(BinaryOp::Gt, Expr::literal(12)),
            Expr::literal(20).binary(BinaryOp::Gt, Expr::column(SEVERITY_NUMBER)),
            Expr::column("body").binary(BinaryOp::Eq, Expr::literal("x")),
            Expr::column("body").binary(BinaryOp::Eq, Expr::literal(1)),
            Expr::column("trace_id").binary(BinaryOp::Eq, Expr::Literal(Literal::Null)),
        ];
        let range = TimeRange::new(Timestamp::from_nanos(5), Timestamp::from_nanos(9));
        let bounds = bounds(&predicates, range);
        let by_name: BTreeMap<&str, &Bound> =
            bounds.iter().map(|b| (b.column.as_str(), b)).collect();
        assert_eq!(by_name["severity_number"].min, Some(Scalar::Int(12)));
        assert_eq!(by_name["severity_number"].max, Some(Scalar::Int(20)));
        assert_eq!(by_name["body"].min, Some(Scalar::Text("x".into())));
        assert_eq!(by_name["timestamp"].min, Some(Scalar::Int(5)));
        assert_eq!(by_name["timestamp"].max, Some(Scalar::Int(8)));
        assert!(!by_name.contains_key("trace_id"));
    }

    #[test]
    fn files_without_statistics_are_kept() {
        let mut bare = file(1, 0, 10, (0, 0));
        bare.columns.clear();
        let mut scan = Scan::all(
            TenantId::DEFAULT,
            DatasetName::new("prod").unwrap(),
            Kind::Log,
        );
        scan.predicates =
            vec![Expr::column(SEVERITY_NUMBER).binary(BinaryOp::Eq, Expr::literal(99))];
        let (kept, _) = prune_files(&scan, [&bare]);
        assert_eq!(kept.len(), 1);
    }

    /// DM-5: a file storing `attr.code` per type class has statistics
    /// under `attr.code#i64`; a numeric bound on the path reads them.
    #[test]
    fn sub_column_statistics_prune_a_path_of_several_types() {
        use arrow::datatypes::DataType;

        let mut typed = file(1, 0, 10, (1, 5));
        typed.columns.push(ColumnStats {
            name: "attr.code#i64".into(),
            min: Some(Scalar::Int(100)),
            max: Some(Scalar::Int(200)),
            null_count: 0,
            distinct: None,
            quantiles: Vec::new(),
        });
        let mut files = vec![typed];
        let path = |data_type: DataType| Expr::Path {
            path: "attr.code".into(),
            data_type,
        };
        let mut scan = Scan::all(
            TenantId::DEFAULT,
            DatasetName::new("prod").unwrap(),
            Kind::Log,
        );
        scan.predicates = vec![path(DataType::Int64).binary(BinaryOp::GtEq, Expr::literal(500))];
        let (kept, _) = prune_files(&scan, &files);
        assert!(kept.is_empty(), "no number of the path reaches 500");
        scan.predicates = vec![path(DataType::Utf8).binary(BinaryOp::Eq, Expr::literal("x"))];
        let (kept, _) = prune_files(&scan, &files);
        assert_eq!(kept.len(), 1, "no statistics of the text class: kept");
        files[0].columns.push(ColumnStats {
            name: "attr.code#str".into(),
            min: Some(Scalar::Text("a".into())),
            max: Some(Scalar::Text("b".into())),
            null_count: 0,
            distinct: None,
            quantiles: Vec::new(),
        });
        let (kept, _) = prune_files(&scan, &files);
        assert_eq!(
            kept.len(),
            1,
            "read as text the numbers count too: text statistics alone do not bound the path"
        );
        scan.predicates = vec![path(DataType::Utf8).binary(BinaryOp::Eq, Expr::literal("x"))];
        files[0].columns.retain(|c| c.name != "attr.code#i64");
        let (kept, _) = prune_files(&scan, &files);
        assert!(
            kept.is_empty(),
            "text is all the path holds: pruned by its statistics"
        );
    }

    /// QL-14: a stored predicate prunes by the statistics of the files
    /// whose every trace stores its shape, and leaves the others alone.
    #[test]
    fn stored_predicates_prune_the_files_that_store_every_trace() {
        let stats = |name: &str, min: &str, max: &str, nulls: u64| ColumnStats {
            name: name.into(),
            min: Some(Scalar::Text(min.into())),
            max: Some(Scalar::Text(max.into())),
            null_count: nulls,
            distinct: None,
            quantiles: Vec::new(),
        };
        let numbered = |nulls: u64| ColumnStats {
            name: NEST_LEFT.into(),
            min: Some(Scalar::Int(1)),
            max: Some(Scalar::Int(9)),
            null_count: nulls,
            distinct: None,
            quantiles: Vec::new(),
        };
        let mut complete = file(1, 0, 10, (1, 5));
        complete
            .columns
            .push(stats("root_name", "GET /", "GET /", 0));
        complete.columns.push(numbered(0));
        let mut partial = file(2, 0, 10, (1, 5));
        partial
            .columns
            .push(stats("root_name", "GET /", "GET /", 2));
        partial.columns.push(numbered(2));
        let plain = file(3, 0, 10, (1, 5));
        let files = vec![complete, partial, plain];
        let mut scan = Scan::all(
            TenantId::DEFAULT,
            DatasetName::new("prod").unwrap(),
            Kind::Span,
        );
        scan.stored_predicates =
            vec![Expr::column("root_name").binary(BinaryOp::Eq, Expr::literal("POST /"))];
        let (kept, _) = prune_files(&scan, &files);
        let ids: Vec<FileId> = kept.iter().map(|f| f.file_id).collect();
        assert_eq!(
            ids,
            [FileId::from_u128(2), FileId::from_u128(3)],
            "the complete file has no such root"
        );
        assert!(stores_every_trace(&files[0]));
        assert!(!stores_every_trace(&files[1]));
        assert!(!stores_every_trace(&files[2]));
    }
}
