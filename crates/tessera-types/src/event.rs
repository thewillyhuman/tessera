// SPDX-License-Identifier: LicenseRef-Tessera-Pending

//! The event model (spec section 4.2) as Arrow schemas, and batches of
//! events in flight.
//!
//! Every record of every signal is an event; a `kind` column says which
//! signal produced it. Files are physically segregated by kind (ST-11), so
//! each kind has its own schema holding the well-known columns that apply
//! to it. Attributes without a dedicated column (resource, scope and record
//! attributes, flattened to dotted paths) travel in `_overflow` as a JSON
//! object of path to scalar: the fallback encoding of ST-13, used until
//! Variant support in the Parquet implementation is verified.

use std::collections::BTreeMap;
use std::sync::{Arc, OnceLock};

use arrow::array::{
    Array, ArrayRef, FixedSizeBinaryArray, Float64Array, Int32Array, RecordBatch, StringArray,
    TimestampNanosecondArray,
};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
use serde::{Deserialize, Serialize};

use crate::{DatasetName, Kind, TenantId, Timestamp};

/// Event time, nanoseconds; required.
pub const TIMESTAMP: &str = "timestamp";
/// Ingest time, nanoseconds.
pub const OBSERVED_TIMESTAMP: &str = "observed_timestamp";
/// The signal, as its lowercase name.
pub const KIND: &str = "kind";
/// Identity for deduplication (section 7.5), 16 bytes.
pub const EVENT_ID: &str = "event_id";
/// Correlation with a trace, 16 bytes.
pub const TRACE_ID: &str = "trace_id";
/// The span this event belongs to, 8 bytes.
pub const SPAN_ID: &str = "span_id";
/// Fraction of similar rows kept; 1.0 unless sampled (section 13.3).
pub const SAMPLE_RATE: &str = "sample_rate";
/// Log severity as a number.
pub const SEVERITY_NUMBER: &str = "severity_number";
/// Log severity as text.
pub const SEVERITY_TEXT: &str = "severity_text";
/// The log body.
pub const BODY: &str = "body";
/// Span name, event name or metric name.
pub const NAME: &str = "name";
/// Attributes without a dedicated column, as a JSON object.
pub const OVERFLOW: &str = "_overflow";
/// Labels derived at query time by a parser stage (`LogQL` `json`,
/// `logfmt`, ...), as a JSON object of label name to text. Never stored;
/// only plans add it.
pub const EXTRACTED: &str = "_extracted";

/// The schema of events of one kind. Column order is fixed, so that
/// batches of one kind always line up.
pub fn schema(kind: Kind) -> SchemaRef {
    static SCHEMAS: OnceLock<[SchemaRef; 5]> = OnceLock::new();
    let schemas = SCHEMAS.get_or_init(|| Kind::ALL.map(|kind| Arc::new(build_schema(kind))));
    let index = Kind::ALL.iter().position(|k| *k == kind).unwrap_or(0);
    Arc::clone(&schemas[index])
}

fn build_schema(kind: Kind) -> Schema {
    let ns = DataType::Timestamp(TimeUnit::Nanosecond, None);
    let mut fields = vec![
        Field::new(TIMESTAMP, ns.clone(), false),
        Field::new(OBSERVED_TIMESTAMP, ns, false),
        Field::new(KIND, DataType::Utf8, false),
        Field::new(EVENT_ID, DataType::FixedSizeBinary(16), false),
        Field::new(TRACE_ID, DataType::FixedSizeBinary(16), true),
        Field::new(SPAN_ID, DataType::FixedSizeBinary(8), true),
        Field::new(SAMPLE_RATE, DataType::Float64, false),
    ];
    match kind {
        Kind::Log => fields.extend([
            Field::new(SEVERITY_NUMBER, DataType::Int32, true),
            Field::new(SEVERITY_TEXT, DataType::Utf8, true),
            Field::new(BODY, DataType::Utf8, true),
        ]),
        Kind::Span | Kind::Metric | Kind::Event | Kind::Alert => {
            fields.push(Field::new(NAME, DataType::Utf8, true));
        }
    }
    fields.push(Field::new(OVERFLOW, DataType::Utf8, true));
    Schema::new(fields)
}

/// The Prometheus and Loki label name of an attribute path (QL-19): the
/// path without its scope prefix, with every character outside
/// `[a-zA-Z0-9_]` replaced by `_`. `resource.service.name` is
/// `service_name`; `attr.http.request.method` is `http_request_method`.
#[must_use]
pub fn label_name(path: &str) -> String {
    let unscoped = SCOPES
        .iter()
        .find_map(|scope| path.strip_prefix(scope))
        .unwrap_or(path);
    unscoped
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || c == '_' {
                c
            } else {
                '_'
            }
        })
        .collect()
}

/// The attribute scopes of section 4.2, in the order a label name
/// resolves against them (QL-20): the more specific scope wins.
pub const SCOPES: [&str; 3] = ["attr.", "resource.", "scope."];

/// A scalar attribute value (section 4.3). Lists and nested maps are
/// flattened or kept as JSON text by normalization (IN-5).
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum Scalar {
    /// A boolean.
    Bool(bool),
    /// A 64-bit integer.
    Int(i64),
    /// A 64-bit float.
    Float(f64),
    /// Text.
    Text(String),
}

/// Flattened attributes of one event: dotted path to scalar, under the
/// `resource.`, `scope.` and `attr.` prefixes of section 4.2.
pub type Attributes = BTreeMap<String, Scalar>;

/// Why a record batch is not a valid batch of events.
#[derive(Debug, thiserror::Error)]
pub enum InvalidBatch {
    /// The batch has no rows.
    #[error("an event batch must have at least one row")]
    Empty,
    /// The batch's schema is not the schema of its kind.
    #[error("event batch schema does not match the schema of kind {kind}")]
    Schema {
        /// The kind the batch claims.
        kind: Kind,
    },
    /// A column could not be built.
    #[error("cannot build event batch: {0}")]
    Arrow(#[from] arrow::error::ArrowError),
}

/// A batch of events of one tenant, dataset and kind: the unit that flows
/// through ingest, writer logs and files.
#[derive(Clone, Debug, PartialEq)]
pub struct EventBatch {
    tenant: TenantId,
    dataset: DatasetName,
    kind: Kind,
    rows: RecordBatch,
}

impl EventBatch {
    /// Wraps `rows`, which must have the schema of `kind` and at least one
    /// row.
    ///
    /// # Errors
    ///
    /// `Empty` or `Schema` as described.
    pub fn new(
        tenant: TenantId,
        dataset: DatasetName,
        kind: Kind,
        rows: RecordBatch,
    ) -> Result<Self, InvalidBatch> {
        if rows.num_rows() == 0 {
            return Err(InvalidBatch::Empty);
        }
        if rows.schema() != schema(kind) {
            return Err(InvalidBatch::Schema { kind });
        }
        Ok(EventBatch {
            tenant,
            dataset,
            kind,
            rows,
        })
    }

    /// A batch of log events.
    ///
    /// # Errors
    ///
    /// `Empty` if there are no rows; `Arrow` if a column cannot be built.
    pub fn from_logs(
        tenant: TenantId,
        dataset: DatasetName,
        logs: &[LogRow],
    ) -> Result<Self, InvalidBatch> {
        if logs.is_empty() {
            return Err(InvalidBatch::Empty);
        }
        let columns: Vec<ArrayRef> = vec![
            timestamps(logs.iter().map(|l| l.timestamp)),
            timestamps(logs.iter().map(|l| l.observed_timestamp)),
            Arc::new(StringArray::from(vec![Kind::Log.as_str(); logs.len()])),
            fixed(logs.iter().map(|l| Some(&l.event_id[..])), 16)?,
            fixed(
                logs.iter().map(|l| l.trace_id.as_ref().map(|id| &id[..])),
                16,
            )?,
            fixed(logs.iter().map(|l| l.span_id.as_ref().map(|id| &id[..])), 8)?,
            Arc::new(Float64Array::from(vec![1.0; logs.len()])),
            Arc::new(Int32Array::from(
                logs.iter().map(|l| l.severity_number).collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from(
                logs.iter()
                    .map(|l| l.severity_text.as_deref())
                    .collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from(
                logs.iter().map(|l| l.body.as_deref()).collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from(
                logs.iter()
                    .map(|l| overflow(&l.attributes))
                    .collect::<Vec<_>>(),
            )),
        ];
        let rows = RecordBatch::try_new(schema(Kind::Log), columns)?;
        Ok(EventBatch {
            tenant,
            dataset,
            kind: Kind::Log,
            rows,
        })
    }

    /// The tenant the events belong to.
    #[must_use]
    pub fn tenant(&self) -> &TenantId {
        &self.tenant
    }

    /// The dataset the events belong to.
    #[must_use]
    pub fn dataset(&self) -> &DatasetName {
        &self.dataset
    }

    /// The signal.
    #[must_use]
    pub fn kind(&self) -> Kind {
        self.kind
    }

    /// The rows.
    #[must_use]
    pub fn rows(&self) -> &RecordBatch {
        &self.rows
    }

    /// The number of events.
    #[must_use]
    pub fn len(&self) -> usize {
        self.rows.num_rows()
    }

    /// Always false: a batch has at least one row.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        false
    }

    /// The earliest and latest event time.
    #[must_use]
    pub fn time_bounds(&self) -> (Timestamp, Timestamp) {
        bounds(&self.rows, 0)
    }

    /// The earliest and latest observed time.
    #[must_use]
    pub fn observed_bounds(&self) -> (Timestamp, Timestamp) {
        bounds(&self.rows, 1)
    }
}

/// One log event, before it is turned into a column of a batch.
#[derive(Clone, Debug, PartialEq, Default)]
pub struct LogRow {
    /// Event time.
    pub timestamp: Timestamp,
    /// Ingest time.
    pub observed_timestamp: Timestamp,
    /// Identity for deduplication.
    pub event_id: [u8; 16],
    /// The trace this log belongs to, if any.
    pub trace_id: Option<[u8; 16]>,
    /// The span this log belongs to, if any.
    pub span_id: Option<[u8; 8]>,
    /// Severity as a number.
    pub severity_number: Option<i32>,
    /// Severity as text.
    pub severity_text: Option<String>,
    /// The body.
    pub body: Option<String>,
    /// Flattened attributes.
    pub attributes: Attributes,
}

fn timestamps(values: impl Iterator<Item = Timestamp>) -> ArrayRef {
    Arc::new(TimestampNanosecondArray::from(
        values.map(Timestamp::as_nanos).collect::<Vec<_>>(),
    ))
}

fn fixed<'a>(
    values: impl Iterator<Item = Option<&'a [u8]>>,
    size: i32,
) -> Result<ArrayRef, InvalidBatch> {
    let array = FixedSizeBinaryArray::try_from_sparse_iter_with_size(values, size)?;
    Ok(Arc::new(array))
}

/// The `_overflow` value for a set of attributes: a JSON object, or null
/// when there are none.
fn overflow(attributes: &Attributes) -> Option<String> {
    if attributes.is_empty() {
        return None;
    }
    serde_json::to_string(attributes).ok()
}

/// Min and max of a non-null nanosecond timestamp column.
fn bounds(rows: &RecordBatch, column: usize) -> (Timestamp, Timestamp) {
    let array = rows
        .column(column)
        .as_any()
        .downcast_ref::<TimestampNanosecondArray>();
    let values = array
        .map(|a| a.values().iter().copied())
        .into_iter()
        .flatten();
    let (min, max) = values.fold((i64::MAX, i64::MIN), |(lo, hi), v| (lo.min(v), hi.max(v)));
    if min > max {
        return (Timestamp::UNIX_EPOCH, Timestamp::UNIX_EPOCH);
    }
    (Timestamp::from_nanos(min), Timestamp::from_nanos(max))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tenant() -> TenantId {
        TenantId::DEFAULT
    }

    fn dataset() -> DatasetName {
        DatasetName::new("prod").unwrap()
    }

    fn log(ts: i64, body: &str) -> LogRow {
        LogRow {
            timestamp: Timestamp::from_nanos(ts),
            observed_timestamp: Timestamp::from_nanos(ts + 5),
            event_id: [1; 16],
            body: Some(body.to_owned()),
            severity_number: Some(9),
            attributes: BTreeMap::from([
                (
                    "resource.service.name".to_owned(),
                    Scalar::Text("checkout".into()),
                ),
                ("attr.http.status_code".to_owned(), Scalar::Int(500)),
            ]),
            ..LogRow::default()
        }
    }

    #[test]
    fn each_kind_has_the_shared_columns_first() {
        for kind in Kind::ALL {
            let schema = schema(kind);
            let names: Vec<&str> = schema
                .fields()
                .iter()
                .map(|f| f.name().as_str())
                .take(7)
                .collect();
            assert_eq!(
                names,
                [
                    TIMESTAMP,
                    OBSERVED_TIMESTAMP,
                    KIND,
                    EVENT_ID,
                    TRACE_ID,
                    SPAN_ID,
                    SAMPLE_RATE
                ],
                "{kind}"
            );
            assert_eq!(schema.fields().last().unwrap().name(), OVERFLOW, "{kind}");
        }
    }

    #[test]
    fn the_log_schema_has_the_log_columns() {
        let schema = schema(Kind::Log);
        assert!(schema.column_with_name(BODY).is_some());
        assert!(schema.column_with_name(SEVERITY_NUMBER).is_some());
        assert!(schema.column_with_name(NAME).is_none());
    }

    #[test]
    fn schemas_are_shared_not_rebuilt() {
        assert!(Arc::ptr_eq(&schema(Kind::Log), &schema(Kind::Log)));
    }

    #[test]
    fn builds_a_log_batch_with_bounds_and_overflow() {
        let batch =
            EventBatch::from_logs(tenant(), dataset(), &[log(100, "a"), log(50, "b")]).unwrap();
        assert_eq!(batch.len(), 2);
        assert_eq!(batch.kind(), Kind::Log);
        assert_eq!(
            batch.time_bounds(),
            (Timestamp::from_nanos(50), Timestamp::from_nanos(100))
        );
        assert_eq!(
            batch.observed_bounds(),
            (Timestamp::from_nanos(55), Timestamp::from_nanos(105))
        );
        let overflow = batch.rows().column_by_name(OVERFLOW).unwrap();
        let overflow = overflow.as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(
            overflow.value(0),
            r#"{"attr.http.status_code":500,"resource.service.name":"checkout"}"#
        );
    }

    #[test]
    fn a_log_without_attributes_has_a_null_overflow() {
        let row = LogRow {
            attributes: BTreeMap::new(),
            ..log(1, "x")
        };
        let batch = EventBatch::from_logs(tenant(), dataset(), &[row]).unwrap();
        assert!(batch.rows().column_by_name(OVERFLOW).unwrap().is_null(0));
    }

    #[test]
    fn rejects_empty_batches() {
        assert!(matches!(
            EventBatch::from_logs(tenant(), dataset(), &[]),
            Err(InvalidBatch::Empty)
        ));
        let empty = RecordBatch::new_empty(schema(Kind::Log));
        assert!(matches!(
            EventBatch::new(tenant(), dataset(), Kind::Log, empty),
            Err(InvalidBatch::Empty)
        ));
    }

    #[test]
    fn rejects_rows_of_another_kind() {
        let batch = EventBatch::from_logs(tenant(), dataset(), &[log(1, "x")]).unwrap();
        let error =
            EventBatch::new(tenant(), dataset(), Kind::Span, batch.rows().clone()).unwrap_err();
        assert!(matches!(error, InvalidBatch::Schema { kind: Kind::Span }));
    }

    #[test]
    fn scalars_serialize_as_plain_json_values() {
        let attributes: Attributes = BTreeMap::from([
            ("a".to_owned(), Scalar::Bool(true)),
            ("b".to_owned(), Scalar::Float(1.5)),
            ("c".to_owned(), Scalar::Int(2)),
        ]);
        let json = serde_json::to_string(&attributes).unwrap();
        assert_eq!(json, r#"{"a":true,"b":1.5,"c":2}"#);
        assert_eq!(
            serde_json::from_str::<Attributes>(&json).unwrap(),
            attributes
        );
    }

    #[test]
    fn label_names_drop_the_scope_and_sanitize() {
        assert_eq!(label_name("resource.service.name"), "service_name");
        assert_eq!(
            label_name("attr.http.request.method"),
            "http_request_method"
        );
        assert_eq!(label_name("scope.k8s-pod/name"), "k8s_pod_name");
        assert_eq!(label_name("level"), "level");
        assert_eq!(
            label_name("attr.scope.x"),
            "scope_x",
            "only the leading scope goes"
        );
    }
}
