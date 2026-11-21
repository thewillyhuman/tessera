// SPDX-License-Identifier: Apache-2.0

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

use std::cmp::Ordering;
use std::collections::BTreeMap;
use std::sync::{Arc, OnceLock};

use arrow::array::{
    Array, ArrayRef, BooleanArray, FixedSizeBinaryArray, Float64Array, Int32Array, Int64Array,
    RecordBatch, StringArray, StringBuilder, TimestampNanosecondArray, UInt64Array,
};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::{DatasetName, Kind, TenantId, Timestamp};

/// Event time, nanoseconds; required.
pub const TIMESTAMP: &str = "timestamp";
/// Ingest time, nanoseconds.
pub const OBSERVED_TIMESTAMP: &str = "observed_timestamp";
/// The signal, as its lowercase name.
pub const KIND: &str = "kind";
/// Identity for deduplication (section 7.5), 16 bytes. Metric rows have
/// none stored: a sample is identified by its series and its time,
/// which the rows already carry, and sixteen bytes per sample would
/// cost more than every other column together.
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
/// The parent of a span, 8 bytes; null for a root span.
pub const PARENT_SPAN_ID: &str = "parent_span_id";
/// The span kind, as its lowercase OpenTelemetry name (`server`, ...).
pub const SPAN_KIND: &str = "span_kind";
/// The span duration in nanoseconds.
pub const DURATION_NS: &str = "duration_ns";
/// The span status, as its lowercase OpenTelemetry name (`unset`, `ok`,
/// `error`).
pub const STATUS_CODE: &str = "status_code";
/// The span status message.
pub const STATUS_MESSAGE: &str = "status_message";
/// Span links (DM-9): a JSON list of objects with `trace_id` and
/// `span_id` as hex and the link's flattened attributes.
pub const LINKS: &str = "links";
/// The metric type, as its lowercase name (`gauge`, `sum`, `histogram`,
/// `exp_histogram`, `summary`).
pub const METRIC_TYPE: &str = "metric_type";
/// The unit of a metric, as the producer wrote it (`ms`, `By`, ...);
/// empty when unknown.
pub const UNIT: &str = "unit";
/// The value of a gauge or sum data point.
pub const VALUE: &str = "value";
/// The buckets of a histogram, exponential histogram or summary data
/// point, as a JSON object (section 4.2's `histogram` struct).
pub const HISTOGRAM: &str = "histogram";
/// The aggregation temporality of a sum or histogram (`delta`,
/// `cumulative`, `unspecified`).
pub const TEMPORALITY: &str = "temporality";
/// Whether a sum only grows.
pub const IS_MONOTONIC: &str = "is_monotonic";
/// The identity of a series: a digest of the metric name and its
/// sorted attributes, 16 bytes.
pub const SERIES_ID: &str = "series_id";
/// Attributes without a dedicated column, as a JSON object.
pub const OVERFLOW: &str = "_overflow";
/// The log pattern a compacted log row was assigned (section 13.2), a
/// dedicated column of compacted log files.
pub const PATTERN_ID: &str = "pattern_id";
/// The value column of a metric result (`RangeVector`, section 10.2):
/// the same name as a stored metric's value.
pub const VALUE_LABELS: &str = VALUE;
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

/// The schema of the `events` table (DM-1): every well-known column of
/// every kind once, in the order the kinds introduce them, `_overflow`
/// last. A column a kind lacks is null in its rows, so only the columns
/// every kind has are required.
pub fn unified_schema() -> SchemaRef {
    static SCHEMA: OnceLock<SchemaRef> = OnceLock::new();
    Arc::clone(SCHEMA.get_or_init(|| {
        let mut fields: Vec<Field> = Vec::new();
        for kind in Kind::ALL {
            for field in schema(kind).fields() {
                if field.name() != OVERFLOW && fields.iter().all(|f| f.name() != field.name()) {
                    let shared = [TIMESTAMP, OBSERVED_TIMESTAMP, KIND, SAMPLE_RATE];
                    let nullable = !shared.contains(&field.name().as_str());
                    fields.push(Field::new(
                        field.name(),
                        field.data_type().clone(),
                        nullable,
                    ));
                }
            }
        }
        fields.push(Field::new(OVERFLOW, DataType::Utf8, true));
        Arc::new(Schema::new(fields))
    }))
}

fn build_schema(kind: Kind) -> Schema {
    let ns = DataType::Timestamp(TimeUnit::Nanosecond, None);
    let mut fields = vec![
        Field::new(TIMESTAMP, ns.clone(), false),
        Field::new(OBSERVED_TIMESTAMP, ns, false),
        Field::new(KIND, DataType::Utf8, false),
    ];
    if kind != Kind::Metric {
        fields.push(Field::new(EVENT_ID, DataType::FixedSizeBinary(16), false));
    }
    fields.extend([
        Field::new(TRACE_ID, DataType::FixedSizeBinary(16), true),
        Field::new(SPAN_ID, DataType::FixedSizeBinary(8), true),
        Field::new(SAMPLE_RATE, DataType::Float64, false),
    ]);
    match kind {
        Kind::Log => fields.extend([
            Field::new(SEVERITY_NUMBER, DataType::Int32, true),
            Field::new(SEVERITY_TEXT, DataType::Utf8, true),
            Field::new(BODY, DataType::Utf8, true),
        ]),
        Kind::Span => fields.extend([
            Field::new(PARENT_SPAN_ID, DataType::FixedSizeBinary(8), true),
            Field::new(NAME, DataType::Utf8, true),
            Field::new(SPAN_KIND, DataType::Utf8, true),
            Field::new(DURATION_NS, DataType::Int64, true),
            Field::new(STATUS_CODE, DataType::Utf8, true),
            Field::new(STATUS_MESSAGE, DataType::Utf8, true),
            Field::new(LINKS, DataType::Utf8, true),
        ]),
        Kind::Metric => fields.extend([
            Field::new(NAME, DataType::Utf8, true),
            Field::new(METRIC_TYPE, DataType::Utf8, true),
            Field::new(UNIT, DataType::Utf8, true),
            Field::new(VALUE, DataType::Float64, true),
            Field::new(HISTOGRAM, DataType::Utf8, true),
            Field::new(TEMPORALITY, DataType::Utf8, true),
            Field::new(IS_MONOTONIC, DataType::Boolean, true),
            Field::new(SERIES_ID, DataType::FixedSizeBinary(16), true),
        ]),
        Kind::Event | Kind::Alert => {
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

/// The attribute scopes of section 4.2, and the fields of a
/// structured log body (DM-10), which are attributes of the row too.
pub const SCOPES: [&str; 4] = ["attr.", "resource.", "scope.", "body."];

/// The marker between an attribute path and the type class of one of
/// its sub-columns (DM-5): a path seen with values of several types is
/// stored as `attr.code#i64`, `attr.code#str`, one column per class,
/// each null where the value is of another class.
pub const SUB_COLUMN: char = '#';

/// The class of a sub-column holding integers.
pub const CLASS_INT: &str = "i64";
/// The class of a sub-column holding floats, and the integers of a path
/// that also has floats, which compare numerically with them.
pub const CLASS_FLOAT: &str = "f64";
/// The class of a sub-column holding text.
pub const CLASS_TEXT: &str = "str";
/// The class of a sub-column holding booleans.
pub const CLASS_BOOL: &str = "bool";
/// Every class a sub-column can hold.
pub const CLASSES: [&str; 4] = [CLASS_INT, CLASS_FLOAT, CLASS_TEXT, CLASS_BOOL];

/// The sub-column of `path` holding its values of `class`.
#[must_use]
pub fn sub_column(path: &str, class: &str) -> String {
    format!("{path}{SUB_COLUMN}{class}")
}

/// The path and class of a sub-column; none for a plain column, whose
/// name ends in no class marker.
#[must_use]
pub fn sub_class(column: &str) -> Option<(&str, &str)> {
    let (path, class) = column.rsplit_once(SUB_COLUMN)?;
    CLASSES.contains(&class).then_some((path, class))
}

/// The path a column stores: itself, or the path before the class
/// marker of a sub-column.
#[must_use]
pub fn logical_path(column: &str) -> &str {
    sub_class(column).map_or(column, |(path, _)| path)
}

/// The scopes a label name resolves against (QL-20), in order: the
/// more specific scope wins. Body fields are not labels until a parser
/// stage makes them so, as in Loki.
pub const LABEL_SCOPES: [&str; 3] = ["attr.", "resource.", "scope."];

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

impl Scalar {
    /// How `self` orders against `other`: numbers among themselves, an
    /// integer against a float as floats, text against text and
    /// booleans against booleans; none across kinds or for a NaN.
    #[must_use]
    pub fn compare(&self, other: &Scalar) -> Option<Ordering> {
        #[allow(clippy::cast_precision_loss)]
        match (self, other) {
            (Scalar::Int(a), Scalar::Int(b)) => Some(a.cmp(b)),
            (Scalar::Float(a), Scalar::Float(b)) => a.partial_cmp(b),
            (Scalar::Int(a), Scalar::Float(b)) => (*a as f64).partial_cmp(b),
            (Scalar::Float(a), Scalar::Int(b)) => a.partial_cmp(&(*b as f64)),
            (Scalar::Text(a), Scalar::Text(b)) => Some(a.cmp(b)),
            (Scalar::Bool(a), Scalar::Bool(b)) => Some(a.cmp(b)),
            _ => None,
        }
    }
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

    /// A batch of spans.
    ///
    /// # Errors
    ///
    /// `Empty` if there are no rows; `Arrow` if a column cannot be built.
    pub fn from_spans(
        tenant: TenantId,
        dataset: DatasetName,
        spans: &[SpanRow],
    ) -> Result<Self, InvalidBatch> {
        if spans.is_empty() {
            return Err(InvalidBatch::Empty);
        }
        let columns: Vec<ArrayRef> = vec![
            timestamps(spans.iter().map(|s| s.timestamp)),
            timestamps(spans.iter().map(|s| s.observed_timestamp)),
            Arc::new(StringArray::from(vec![Kind::Span.as_str(); spans.len()])),
            fixed(spans.iter().map(|s| Some(&s.event_id[..])), 16)?,
            fixed(spans.iter().map(|s| Some(&s.trace_id[..])), 16)?,
            fixed(spans.iter().map(|s| Some(&s.span_id[..])), 8)?,
            Arc::new(Float64Array::from(vec![1.0; spans.len()])),
            fixed(
                spans
                    .iter()
                    .map(|s| s.parent_span_id.as_ref().map(|id| &id[..])),
                8,
            )?,
            Arc::new(StringArray::from(
                spans
                    .iter()
                    .map(|s| Some(s.name.as_str()))
                    .collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from(
                spans
                    .iter()
                    .map(|s| Some(s.span_kind.as_str()))
                    .collect::<Vec<_>>(),
            )),
            Arc::new(Int64Array::from(
                spans
                    .iter()
                    .map(|s| Some(s.duration_ns))
                    .collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from(
                spans
                    .iter()
                    .map(|s| Some(s.status_code.as_str()))
                    .collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from(
                spans
                    .iter()
                    .map(|s| s.status_message.as_deref())
                    .collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from(
                spans.iter().map(|s| s.links.as_deref()).collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from(
                spans
                    .iter()
                    .map(|s| overflow(&s.attributes))
                    .collect::<Vec<_>>(),
            )),
        ];
        let rows = RecordBatch::try_new(schema(Kind::Span), columns)?;
        Ok(EventBatch {
            tenant,
            dataset,
            kind: Kind::Span,
            rows,
        })
    }

    /// A batch of events of kind `event`: free-form events and span
    /// events (DM-8).
    ///
    /// # Errors
    ///
    /// `Empty` if there are no rows; `Arrow` if a column cannot be built.
    pub fn from_events(
        tenant: TenantId,
        dataset: DatasetName,
        events: &[EventRow],
    ) -> Result<Self, InvalidBatch> {
        Self::rows_of_kind(Kind::Event, tenant, dataset, events)
    }

    /// A batch of alert state transitions (DM-3), which have the shape
    /// of events: a name, the rule's, and attributes.
    ///
    /// # Errors
    ///
    /// `Empty` if there are no rows; `Arrow` if a column cannot be built.
    pub fn from_alerts(
        tenant: TenantId,
        dataset: DatasetName,
        alerts: &[EventRow],
    ) -> Result<Self, InvalidBatch> {
        Self::rows_of_kind(Kind::Alert, tenant, dataset, alerts)
    }

    fn rows_of_kind(
        kind: Kind,
        tenant: TenantId,
        dataset: DatasetName,
        events: &[EventRow],
    ) -> Result<Self, InvalidBatch> {
        if events.is_empty() {
            return Err(InvalidBatch::Empty);
        }
        let columns: Vec<ArrayRef> = vec![
            timestamps(events.iter().map(|e| e.timestamp)),
            timestamps(events.iter().map(|e| e.observed_timestamp)),
            Arc::new(StringArray::from(vec![kind.as_str(); events.len()])),
            fixed(events.iter().map(|e| Some(&e.event_id[..])), 16)?,
            fixed(
                events.iter().map(|e| e.trace_id.as_ref().map(|id| &id[..])),
                16,
            )?,
            fixed(
                events.iter().map(|e| e.span_id.as_ref().map(|id| &id[..])),
                8,
            )?,
            Arc::new(Float64Array::from(vec![1.0; events.len()])),
            Arc::new(StringArray::from(
                events
                    .iter()
                    .map(|e| Some(e.name.as_str()))
                    .collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from(
                events
                    .iter()
                    .map(|e| overflow(&e.attributes))
                    .collect::<Vec<_>>(),
            )),
        ];
        let rows = RecordBatch::try_new(schema(kind), columns)?;
        Ok(EventBatch {
            tenant,
            dataset,
            kind,
            rows,
        })
    }

    /// A batch of metric data points.
    ///
    /// # Errors
    ///
    /// `Empty` if there are no rows; `Arrow` if a column cannot be built.
    pub fn from_metrics(
        tenant: TenantId,
        dataset: DatasetName,
        points: &[MetricRow],
    ) -> Result<Self, InvalidBatch> {
        if points.is_empty() {
            return Err(InvalidBatch::Empty);
        }
        let strings = |f: &dyn Fn(&MetricRow) -> Option<&str>| -> ArrayRef {
            Arc::new(StringArray::from(points.iter().map(f).collect::<Vec<_>>()))
        };
        let columns: Vec<ArrayRef> = vec![
            timestamps(points.iter().map(|p| p.timestamp)),
            timestamps(points.iter().map(|p| p.observed_timestamp)),
            Arc::new(StringArray::from(vec![Kind::Metric.as_str(); points.len()])),
            fixed(
                points.iter().map(|p| p.trace_id.as_ref().map(|id| &id[..])),
                16,
            )?,
            fixed(
                points.iter().map(|p| p.span_id.as_ref().map(|id| &id[..])),
                8,
            )?,
            Arc::new(Float64Array::from(vec![1.0; points.len()])),
            strings(&|p| Some(p.name.as_str())),
            strings(&|p| Some(p.metric_type.as_str())),
            strings(&|p| Some(p.unit.as_str())),
            Arc::new(Float64Array::from(
                points.iter().map(|p| p.value).collect::<Vec<_>>(),
            )),
            strings(&|p| p.histogram.as_deref()),
            strings(&|p| Some(p.temporality.as_str())),
            Arc::new(BooleanArray::from(
                points.iter().map(|p| p.is_monotonic).collect::<Vec<_>>(),
            )),
            fixed(points.iter().map(|p| Some(&p.series_id[..])), 16)?,
            Arc::new(StringArray::from(
                points
                    .iter()
                    .map(|p| overflow(&p.attributes))
                    .collect::<Vec<_>>(),
            )),
        ];
        let rows = RecordBatch::try_new(schema(Kind::Metric), columns)?;
        Ok(EventBatch {
            tenant,
            dataset,
            kind: Kind::Metric,
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

/// The kind of a span (section 4.2), as OpenTelemetry names it.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
pub enum SpanKind {
    /// Not specified by the producer.
    #[default]
    Unspecified,
    /// An operation inside the application.
    Internal,
    /// The server side of a remote request.
    Server,
    /// The client side of a remote request.
    Client,
    /// The sending side of an asynchronous message.
    Producer,
    /// The receiving side of an asynchronous message.
    Consumer,
}

impl SpanKind {
    /// Every kind, in OpenTelemetry's numbering order.
    pub const ALL: [SpanKind; 6] = [
        SpanKind::Unspecified,
        SpanKind::Internal,
        SpanKind::Server,
        SpanKind::Client,
        SpanKind::Producer,
        SpanKind::Consumer,
    ];

    /// The lowercase name stored in the `span_kind` column.
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            SpanKind::Unspecified => "unspecified",
            SpanKind::Internal => "internal",
            SpanKind::Server => "server",
            SpanKind::Client => "client",
            SpanKind::Producer => "producer",
            SpanKind::Consumer => "consumer",
        }
    }

    /// The kind with OpenTelemetry's number `code`; `Unspecified` for
    /// an unknown number.
    #[must_use]
    pub fn from_code(code: i32) -> Self {
        usize::try_from(code)
            .ok()
            .and_then(|i| SpanKind::ALL.get(i).copied())
            .unwrap_or_default()
    }

    /// The kind with the given stored name, if any.
    #[must_use]
    pub fn parse(name: &str) -> Option<Self> {
        SpanKind::ALL.into_iter().find(|k| k.as_str() == name)
    }
}

/// The status of a span (section 4.2), as OpenTelemetry names it.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
pub enum StatusCode {
    /// No status was set.
    #[default]
    Unset,
    /// The operation succeeded.
    Ok,
    /// The operation failed.
    Error,
}

impl StatusCode {
    /// Every status, in OpenTelemetry's numbering order.
    pub const ALL: [StatusCode; 3] = [StatusCode::Unset, StatusCode::Ok, StatusCode::Error];

    /// The lowercase name stored in the `status_code` column.
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            StatusCode::Unset => "unset",
            StatusCode::Ok => "ok",
            StatusCode::Error => "error",
        }
    }

    /// The status with OpenTelemetry's number `code`; `Unset` for an
    /// unknown number.
    #[must_use]
    pub fn from_code(code: i32) -> Self {
        usize::try_from(code)
            .ok()
            .and_then(|i| StatusCode::ALL.get(i).copied())
            .unwrap_or_default()
    }

    /// The status with the given stored name, if any.
    #[must_use]
    pub fn parse(name: &str) -> Option<Self> {
        StatusCode::ALL.into_iter().find(|s| s.as_str() == name)
    }
}

/// One span, before it is turned into a column of a batch.
#[derive(Clone, Debug, PartialEq, Default)]
pub struct SpanRow {
    /// Start time.
    pub timestamp: Timestamp,
    /// Ingest time.
    pub observed_timestamp: Timestamp,
    /// Identity for deduplication.
    pub event_id: [u8; 16],
    /// The trace.
    pub trace_id: [u8; 16],
    /// The span.
    pub span_id: [u8; 8],
    /// The parent span; none for a root.
    pub parent_span_id: Option<[u8; 8]>,
    /// The operation name.
    pub name: String,
    /// The kind.
    pub span_kind: SpanKind,
    /// End time minus start time, in nanoseconds.
    pub duration_ns: i64,
    /// The status.
    pub status_code: StatusCode,
    /// The status message, when one was set.
    pub status_message: Option<String>,
    /// Links as the JSON text of the `links` column, when there are any.
    pub links: Option<String>,
    /// Flattened attributes.
    pub attributes: Attributes,
}

/// One event of kind `event`, before it is turned into a column of a
/// batch: a free-form event, or a span event (DM-8) carrying the ids of
/// its span.
#[derive(Clone, Debug, PartialEq, Default)]
pub struct EventRow {
    /// Event time.
    pub timestamp: Timestamp,
    /// Ingest time.
    pub observed_timestamp: Timestamp,
    /// Identity for deduplication.
    pub event_id: [u8; 16],
    /// The trace this event belongs to, if any.
    pub trace_id: Option<[u8; 16]>,
    /// The span this event belongs to, if any.
    pub span_id: Option<[u8; 8]>,
    /// The event name.
    pub name: String,
    /// Flattened attributes.
    pub attributes: Attributes,
}

/// The type of a metric (section 4.2).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
pub enum MetricType {
    /// A value sampled at a time.
    #[default]
    Gauge,
    /// A sum over time, cumulative or delta.
    Sum,
    /// Counts in explicit buckets.
    Histogram,
    /// Counts in exponential buckets.
    ExpHistogram,
    /// Quantiles computed by the producer.
    Summary,
}

impl MetricType {
    /// Every type.
    pub const ALL: [MetricType; 5] = [
        MetricType::Gauge,
        MetricType::Sum,
        MetricType::Histogram,
        MetricType::ExpHistogram,
        MetricType::Summary,
    ];

    /// The lowercase name stored in the `metric_type` column.
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            MetricType::Gauge => "gauge",
            MetricType::Sum => "sum",
            MetricType::Histogram => "histogram",
            MetricType::ExpHistogram => "exp_histogram",
            MetricType::Summary => "summary",
        }
    }

    /// The type with the given stored name, if any.
    #[must_use]
    pub fn parse(name: &str) -> Option<Self> {
        MetricType::ALL.into_iter().find(|t| t.as_str() == name)
    }
}

/// The aggregation temporality of a sum or histogram.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
pub enum Temporality {
    /// Not stated by the producer.
    #[default]
    Unspecified,
    /// Each point covers the interval since the previous one.
    Delta,
    /// Each point covers the interval since the start time.
    Cumulative,
}

impl Temporality {
    /// The lowercase name stored in the `temporality` column.
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Temporality::Unspecified => "unspecified",
            Temporality::Delta => "delta",
            Temporality::Cumulative => "cumulative",
        }
    }

    /// The temporality with OpenTelemetry's number `code`; `Unspecified`
    /// for an unknown number.
    #[must_use]
    pub fn from_code(code: i32) -> Self {
        match code {
            1 => Temporality::Delta,
            2 => Temporality::Cumulative,
            _ => Temporality::Unspecified,
        }
    }
}

/// The identity of a series (section 4.5): a digest of the metric name
/// and the sorted attributes.
#[must_use]
pub fn series_id(name: &str, attributes: &Attributes) -> [u8; 16] {
    let mut canonical = String::from(name);
    for (path, value) in attributes {
        canonical.push('\0');
        canonical.push_str(path);
        canonical.push('=');
        canonical.push_str(&serde_json::to_string(value).unwrap_or_default());
    }
    let a = crate::stable_hash(canonical.as_bytes());
    let b = crate::stable_hash(&a.to_le_bytes());
    let mut out = [0u8; 16];
    out[..8].copy_from_slice(&a.to_le_bytes());
    out[8..].copy_from_slice(&b.to_le_bytes());
    out
}

/// One metric data point, before it is turned into a column of a batch.
#[derive(Clone, Debug, PartialEq, Default)]
pub struct MetricRow {
    /// Sample time.
    pub timestamp: Timestamp,
    /// Ingest time.
    pub observed_timestamp: Timestamp,
    /// An exemplar's trace, if any.
    pub trace_id: Option<[u8; 16]>,
    /// An exemplar's span, if any.
    pub span_id: Option<[u8; 8]>,
    /// The metric name, as received.
    pub name: String,
    /// The type.
    pub metric_type: MetricType,
    /// The unit, as received; empty when unknown.
    pub unit: String,
    /// The value of a gauge or sum point.
    pub value: Option<f64>,
    /// The JSON of a histogram, exponential histogram or summary point.
    pub histogram: Option<String>,
    /// The temporality.
    pub temporality: Temporality,
    /// Whether a sum only grows.
    pub is_monotonic: Option<bool>,
    /// The series identity.
    pub series_id: [u8; 16],
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
/// The attributes of every row as one JSON object per row: what
/// `_overflow` holds, plus the attributes a file stores in dedicated
/// columns (AD-4), which compaction leaves out of `_overflow` so that
/// nothing is stored twice. A batch without dedicated attribute columns
/// gives `_overflow` itself; a batch with neither gives nothing. Readers
/// that enumerate attributes, such as label sets, go through this;
/// readers of one path use the dedicated column when there is one.
#[must_use]
pub fn attributes_column(rows: &RecordBatch) -> Option<StringArray> {
    let overflow = rows
        .column_by_name(OVERFLOW)
        .and_then(|c| c.as_any().downcast_ref::<StringArray>());
    let schema = rows.schema();
    let dedicated: Vec<(&str, &ArrayRef)> = schema
        .fields()
        .iter()
        .zip(rows.columns())
        .filter(|(field, _)| SCOPES.iter().any(|scope| field.name().starts_with(scope)))
        .map(|(field, column)| (logical_path(field.name()), column))
        .collect();
    // Columns that hold nothing on these rows add nothing: the stored
    // text is the whole of it, as it is without such columns. A file
    // has the service's column whether any row names a service or not.
    if dedicated
        .iter()
        .all(|(_, column)| column.null_count() == column.len())
    {
        return overflow.cloned();
    }
    // The stored text is already a JSON object: the dedicated values are
    // appended to it as encoded pairs, without parsing it. A path stored
    // both ways, as files from before promotion stripped it are, appears
    // twice with the same value, and a parser keeps the last. The
    // sub-columns of one path (DM-5) are null but for one per row, so
    // the path appears once under its own name.
    let names: Vec<String> = dedicated
        .iter()
        .map(|(path, _)| Value::String((*path).to_owned()).to_string())
        .collect();
    let stored = |i: usize| overflow.filter(|o| !o.is_null(i)).map(|o| o.value(i));
    let mut merged = StringBuilder::with_capacity(rows.num_rows(), 0);
    // The object of the row at hand, written into one buffer for every
    // row, and whether it holds any attribute. Rows of one stream that
    // follow each other have the same cells: their object is written
    // once.
    let mut object: Vec<u8> = Vec::new();
    let mut any = false;
    for i in 0..rows.num_rows() {
        let repeats = i > 0
            && stored(i) == stored(i - 1)
            && dedicated.iter().all(|(_, column)| repeats(column, i));
        if !repeats {
            let text = stored(i)
                .map(str::trim)
                .filter(|text| text.len() >= 2 && text.starts_with('{') && text.ends_with('}'));
            object.clear();
            match text {
                Some(text) => object.extend_from_slice(&text.as_bytes()[..text.len() - 1]),
                None => object.push(b'{'),
            }
            any = text.is_some_and(|text| text.len() > 2);
            for (name, (_, column)) in names.iter().zip(&dedicated) {
                let before = object.len();
                if any {
                    object.push(b',');
                }
                object.extend_from_slice(name.as_bytes());
                object.push(b':');
                if write_cell(&mut object, column, i) {
                    any = true;
                } else {
                    object.truncate(before);
                }
            }
            object.push(b'}');
        }
        // Whole pieces of text and JSON that serde wrote: UTF-8.
        match std::str::from_utf8(&object) {
            Ok(text) if any => merged.append_value(text),
            _ => merged.append_null(),
        }
    }
    Some(merged.finish())
}

/// Writes the value at `row` of a dedicated attribute column to `out`
/// as JSON; false, with nothing written, where the cell is null, a
/// number JSON has no way to write, or of a type attributes do not
/// have.
fn write_cell(out: &mut Vec<u8>, column: &ArrayRef, row: usize) -> bool {
    if column.is_null(row) {
        return false;
    }
    let any = column.as_any();
    macro_rules! written {
        ($array:ty) => {
            any.downcast_ref::<$array>()
                .is_some_and(|a| serde_json::to_writer(&mut *out, &a.value(row)).is_ok())
        };
    }
    match column.data_type() {
        DataType::Utf8 => written!(StringArray),
        DataType::Int64 => written!(Int64Array),
        DataType::Int32 => written!(Int32Array),
        DataType::UInt64 => written!(UInt64Array),
        DataType::Float64 => any
            .downcast_ref::<Float64Array>()
            .map(|a| a.value(row))
            .filter(|value| value.is_finite())
            .is_some_and(|value| serde_json::to_writer(&mut *out, &value).is_ok()),
        DataType::Boolean => written!(BooleanArray),
        DataType::Timestamp(TimeUnit::Nanosecond, _) => written!(TimestampNanosecondArray),
        _ => false,
    }
}

/// Whether the cell of `column` at `row` is the one of the row before,
/// as far as [`write_cell`] tells cells apart: a column of a type it
/// writes nothing for is the same on every row.
fn repeats(column: &ArrayRef, row: usize) -> bool {
    match (column.is_null(row), column.is_null(row - 1)) {
        (true, true) => return true,
        (false, false) => {}
        _ => return false,
    }
    let any = column.as_any();
    macro_rules! same {
        ($array:ty) => {
            any.downcast_ref::<$array>()
                .is_none_or(|a| a.value(row) == a.value(row - 1))
        };
    }
    match column.data_type() {
        DataType::Utf8 => same!(StringArray),
        DataType::Int64 => same!(Int64Array),
        DataType::Int32 => same!(Int32Array),
        DataType::UInt64 => same!(UInt64Array),
        DataType::Float64 => any
            .downcast_ref::<Float64Array>()
            .is_none_or(|a| a.value(row).to_bits() == a.value(row - 1).to_bits()),
        DataType::Boolean => same!(BooleanArray),
        DataType::Timestamp(TimeUnit::Nanosecond, _) => same!(TimestampNanosecondArray),
        _ => true,
    }
}

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
    #[test]
    fn the_unified_schema_has_every_column_once_with_the_overflow_last() {
        let unified = unified_schema();
        let names: Vec<&str> = unified.fields().iter().map(|f| f.name().as_str()).collect();
        for kind in Kind::ALL {
            for field in schema(kind).fields() {
                assert!(
                    names.contains(&field.name().as_str()),
                    "{} of {kind}",
                    field.name()
                );
                assert_eq!(
                    unified.field_with_name(field.name()).unwrap().data_type(),
                    field.data_type()
                );
            }
        }
        let mut sorted = names.clone();
        sorted.sort_unstable();
        sorted.dedup();
        assert_eq!(sorted.len(), names.len(), "no column twice");
        assert_eq!(names.first(), Some(&TIMESTAMP));
        assert_eq!(names.last(), Some(&OVERFLOW));
        assert!(!unified.field_with_name(KIND).unwrap().is_nullable());
        assert!(
            unified.field_with_name(EVENT_ID).unwrap().is_nullable(),
            "metrics have no event id"
        );
    }

    /// Rows of one stream follow each other with the same attributes,
    /// and the object of such a row is the one of the row before: built
    /// once, not once a row. A change in any cell, a value, a null or
    /// the stored text, starts another.
    #[test]
    fn a_row_with_the_cells_of_the_one_before_has_its_attributes() {
        let schema = Arc::new(Schema::new(vec![
            Field::new(OVERFLOW, DataType::Utf8, true),
            Field::new("attr.host", DataType::Utf8, true),
            Field::new("attr.n", DataType::Int64, true),
        ]));
        let rare = Some(r#"{"attr.rare":true}"#);
        let cells: Vec<(Option<&str>, Option<&str>, Option<i64>)> = vec![
            (None, Some("h1"), Some(7)),
            (None, Some("h1"), Some(7)),
            (None, Some("h1"), Some(8)),
            (None, Some("h2"), Some(8)),
            (None, None, Some(8)),
            (None, None, Some(8)),
            (rare, None, Some(8)),
            (rare, None, Some(8)),
            (rare, None, None),
            (None, None, None),
            (None, None, None),
            (None, Some("h1"), None),
        ];
        let rows = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(
                    cells.iter().map(|c| c.0).collect::<Vec<_>>(),
                )),
                Arc::new(StringArray::from(
                    cells.iter().map(|c| c.1).collect::<Vec<_>>(),
                )),
                Arc::new(Int64Array::from(
                    cells.iter().map(|c| c.2).collect::<Vec<_>>(),
                )),
            ],
        )
        .unwrap();
        let merged = attributes_column(&rows).unwrap();
        for (i, (stored, host, n)) in cells.iter().enumerate() {
            let mut expected = serde_json::Map::new();
            if stored.is_some() {
                expected.insert("attr.rare".into(), true.into());
            }
            if let Some(host) = host {
                expected.insert("attr.host".into(), (*host).into());
            }
            if let Some(n) = n {
                expected.insert("attr.n".into(), (*n).into());
            }
            if expected.is_empty() {
                assert!(merged.is_null(i), "row {i}");
                continue;
            }
            let found: serde_json::Value = serde_json::from_str(merged.value(i)).unwrap();
            assert_eq!(found, serde_json::Value::Object(expected), "row {i}");
        }
    }

    #[test]
    fn dedicated_attribute_columns_merge_back_into_the_overflow_view() {
        let schema = Arc::new(Schema::new(vec![
            Field::new(OVERFLOW, DataType::Utf8, true),
            Field::new("attr.host", DataType::Utf8, true),
            Field::new("attr.n", DataType::Int64, true),
            Field::new("body", DataType::Utf8, true),
        ]));
        let rows = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec![Some(r#"{"attr.rare":true}"#), None])),
                Arc::new(StringArray::from(vec![Some("h1"), None])),
                Arc::new(Int64Array::from(vec![Some(7), Some(8)])),
                Arc::new(StringArray::from(vec!["x", "y"])),
            ],
        )
        .unwrap();
        let merged = attributes_column(&rows).unwrap();
        let unset = rows
            .project(&[0, 3])
            .unwrap()
            .columns()
            .iter()
            .cloned()
            .chain([arrow::array::new_null_array(&DataType::Utf8, 2)])
            .collect::<Vec<_>>();
        let unset = RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new(OVERFLOW, DataType::Utf8, true),
                Field::new("body", DataType::Utf8, true),
                Field::new("resource.service.name", DataType::Utf8, true),
            ])),
            unset,
        )
        .unwrap();
        assert_eq!(
            attributes_column(&unset).unwrap().value(0),
            r#"{"attr.rare":true}"#,
            "a dedicated column no row has a value in adds nothing"
        );
        let first: serde_json::Value = serde_json::from_str(merged.value(0)).unwrap();
        assert_eq!(first["attr.rare"], true);
        assert_eq!(first["attr.host"], "h1");
        assert_eq!(first["attr.n"], 7);
        assert!(first.get("body").is_none(), "not an attribute column");
        let second: serde_json::Value = serde_json::from_str(merged.value(1)).unwrap();
        assert_eq!(second, serde_json::json!({ "attr.n": 8 }));
        let plain = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new(
                OVERFLOW,
                DataType::Utf8,
                true,
            )])),
            vec![Arc::new(StringArray::from(vec![Some("{}")]))],
        )
        .unwrap();
        assert_eq!(
            attributes_column(&plain).unwrap().value(0),
            "{}",
            "the column itself"
        );
    }

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
            // Metrics store no event id: a sample is its series and time.
            let shared: Vec<&str> = [
                TIMESTAMP,
                OBSERVED_TIMESTAMP,
                KIND,
                EVENT_ID,
                TRACE_ID,
                SPAN_ID,
                SAMPLE_RATE,
            ]
            .into_iter()
            .filter(|c| kind != Kind::Metric || *c != EVENT_ID)
            .collect();
            assert_eq!(&names[..shared.len()], shared.as_slice(), "{kind}");
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
    fn the_span_schema_has_the_span_columns() {
        let schema = schema(Kind::Span);
        for column in [
            PARENT_SPAN_ID,
            NAME,
            SPAN_KIND,
            DURATION_NS,
            STATUS_CODE,
            STATUS_MESSAGE,
            LINKS,
        ] {
            assert!(schema.column_with_name(column).is_some(), "{column}");
        }
        assert!(schema.column_with_name(BODY).is_none());
        let events = super::schema(Kind::Event);
        assert!(events.column_with_name(NAME).is_some());
        assert!(events.column_with_name(DURATION_NS).is_none());
    }

    #[test]
    fn span_kinds_and_statuses_round_trip_their_codes_and_names() {
        assert_eq!(SpanKind::from_code(2), SpanKind::Server);
        assert_eq!(SpanKind::from_code(99), SpanKind::Unspecified);
        assert_eq!(SpanKind::from_code(-1), SpanKind::Unspecified);
        assert_eq!(SpanKind::parse("consumer"), Some(SpanKind::Consumer));
        assert_eq!(SpanKind::parse("nope"), None);
        assert_eq!(StatusCode::from_code(2), StatusCode::Error);
        assert_eq!(StatusCode::from_code(7), StatusCode::Unset);
        assert_eq!(StatusCode::parse("ok"), Some(StatusCode::Ok));
        for kind in SpanKind::ALL {
            assert_eq!(SpanKind::parse(kind.as_str()), Some(kind));
        }
    }

    #[test]
    fn builds_a_span_batch() {
        let span = SpanRow {
            timestamp: Timestamp::from_nanos(100),
            observed_timestamp: Timestamp::from_nanos(105),
            event_id: [2; 16],
            trace_id: [3; 16],
            span_id: [4; 8],
            parent_span_id: None,
            name: "GET /".into(),
            span_kind: SpanKind::Server,
            duration_ns: 1_500,
            status_code: StatusCode::Error,
            status_message: Some("boom".into()),
            links: Some(r#"[{"trace_id":"00","span_id":"01"}]"#.into()),
            attributes: BTreeMap::from([(
                "attr.http.method".to_owned(),
                Scalar::Text("GET".into()),
            )]),
        };
        let child = SpanRow {
            span_id: [5; 8],
            parent_span_id: Some([4; 8]),
            status_message: None,
            links: None,
            attributes: BTreeMap::new(),
            ..span.clone()
        };
        let batch = EventBatch::from_spans(tenant(), dataset(), &[span, child]).unwrap();
        assert_eq!(batch.kind(), Kind::Span);
        assert_eq!(batch.len(), 2);
        let rows = batch.rows();
        let parent = rows.column_by_name(PARENT_SPAN_ID).unwrap();
        assert!(parent.is_null(0));
        assert!(parent.is_valid(1));
        let kind = rows.column_by_name(SPAN_KIND).unwrap();
        let kind = kind.as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(kind.value(0), "server");
        let duration = rows.column_by_name(DURATION_NS).unwrap();
        let duration = duration.as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(duration.value(1), 1_500);
        let status = rows.column_by_name(STATUS_CODE).unwrap();
        let status = status.as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(status.value(0), "error");
        assert!(rows.column_by_name(STATUS_MESSAGE).unwrap().is_null(1));
        assert!(rows.column_by_name(LINKS).unwrap().is_valid(0));
        assert!(rows.column_by_name(OVERFLOW).unwrap().is_null(1));
    }

    #[test]
    fn the_metric_schema_has_the_metric_columns() {
        let schema = schema(Kind::Metric);
        for column in [
            NAME,
            METRIC_TYPE,
            UNIT,
            VALUE,
            HISTOGRAM,
            TEMPORALITY,
            IS_MONOTONIC,
            SERIES_ID,
        ] {
            assert!(schema.column_with_name(column).is_some(), "{column}");
        }
        assert!(schema.column_with_name(BODY).is_none());
        assert_eq!(
            MetricType::parse("exp_histogram"),
            Some(MetricType::ExpHistogram)
        );
        assert_eq!(Temporality::from_code(2), Temporality::Cumulative);
        assert_eq!(Temporality::from_code(9), Temporality::Unspecified);
    }

    #[test]
    fn series_ids_depend_on_the_name_and_every_attribute() {
        let a: Attributes = BTreeMap::from([("attr.job".to_owned(), Scalar::Text("api".into()))]);
        let mut b = a.clone();
        b.insert("attr.instance".to_owned(), Scalar::Text("x".into()));
        assert_eq!(series_id("up", &a), series_id("up", &a));
        assert_ne!(series_id("up", &a), series_id("up", &b));
        assert_ne!(series_id("up", &a), series_id("down", &a));
    }

    #[test]
    fn builds_a_metric_batch() {
        let point = MetricRow {
            timestamp: Timestamp::from_nanos(100),
            observed_timestamp: Timestamp::from_nanos(105),
            name: "http.server.duration".into(),
            metric_type: MetricType::Sum,
            unit: "ms".into(),
            value: Some(12.5),
            temporality: Temporality::Cumulative,
            is_monotonic: Some(true),
            series_id: [7; 16],
            attributes: BTreeMap::from([("attr.method".to_owned(), Scalar::Text("GET".into()))]),
            ..MetricRow::default()
        };
        let histogram = MetricRow {
            metric_type: MetricType::Histogram,
            value: None,
            histogram: Some(r#"{"count":2}"#.into()),
            is_monotonic: None,
            ..point.clone()
        };
        let batch = EventBatch::from_metrics(tenant(), dataset(), &[point, histogram]).unwrap();
        assert_eq!(batch.kind(), Kind::Metric);
        let rows = batch.rows();
        let value = rows.column_by_name(VALUE).unwrap();
        let value = value.as_any().downcast_ref::<Float64Array>().unwrap();
        assert!((value.value(0) - 12.5).abs() < f64::EPSILON);
        assert!(value.is_null(1));
        assert!(rows.column_by_name(HISTOGRAM).unwrap().is_null(0));
        assert!(rows.column_by_name(HISTOGRAM).unwrap().is_valid(1));
        let kind = rows.column_by_name(METRIC_TYPE).unwrap();
        let kind = kind.as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(kind.value(1), "histogram");
        let monotonic = rows.column_by_name(IS_MONOTONIC).unwrap();
        let monotonic = monotonic.as_any().downcast_ref::<BooleanArray>().unwrap();
        assert!(monotonic.value(0));
        assert!(monotonic.is_null(1));
        assert!(rows.column_by_name(SERIES_ID).unwrap().is_valid(0));
        assert!(matches!(
            EventBatch::from_metrics(tenant(), dataset(), &[]),
            Err(InvalidBatch::Empty)
        ));
    }

    #[test]
    fn builds_an_event_batch_for_span_events() {
        let event = EventRow {
            timestamp: Timestamp::from_nanos(7),
            observed_timestamp: Timestamp::from_nanos(9),
            event_id: [6; 16],
            trace_id: Some([3; 16]),
            span_id: Some([4; 8]),
            name: "exception".into(),
            attributes: BTreeMap::from([(
                "attr.exception.type".to_owned(),
                Scalar::Text("IOError".into()),
            )]),
        };
        let batch = EventBatch::from_events(tenant(), dataset(), &[event]).unwrap();
        assert_eq!(batch.kind(), Kind::Event);
        let name = batch.rows().column_by_name(NAME).unwrap();
        let name = name.as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(name.value(0), "exception");
        assert!(batch.rows().column_by_name(SPAN_ID).unwrap().is_valid(0));
        assert!(matches!(
            EventBatch::from_events(tenant(), dataset(), &[]),
            Err(InvalidBatch::Empty)
        ));
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

    #[test]
    fn sub_columns_name_a_path_and_a_class() {
        assert_eq!(sub_column("attr.code", CLASS_INT), "attr.code#i64");
        assert_eq!(sub_class("attr.code#str"), Some(("attr.code", "str")));
        assert_eq!(sub_class("attr.code"), None);
        assert_eq!(sub_class("attr.c#d"), None, "not a class");
        assert_eq!(logical_path("attr.code#bool"), "attr.code");
        assert_eq!(logical_path("attr.code"), "attr.code");
    }

    #[test]
    fn sub_columns_merge_back_under_their_path() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("attr.code#i64", DataType::Int64, true),
            Field::new("attr.code#str", DataType::Utf8, true),
        ]));
        let rows = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![Some(500), None])),
                Arc::new(StringArray::from(vec![None, Some("500")])),
            ],
        )
        .unwrap();
        let merged = attributes_column(&rows).unwrap();
        assert_eq!(merged.value(0), r#"{"attr.code":500}"#);
        assert_eq!(merged.value(1), r#"{"attr.code":"500"}"#);
    }
}

#[cfg(test)]
mod scalar_tests {
    use super::*;

    #[test]
    fn scalars_compare_within_their_kind() {
        assert_eq!(
            Scalar::Int(1).compare(&Scalar::Int(2)),
            Some(Ordering::Less)
        );
        assert_eq!(
            Scalar::Int(2).compare(&Scalar::Float(1.5)),
            Some(Ordering::Greater)
        );
        assert_eq!(
            Scalar::Float(1.5).compare(&Scalar::Int(2)),
            Some(Ordering::Less)
        );
        assert_eq!(
            Scalar::Text("a".into()).compare(&Scalar::Text("a".into())),
            Some(Ordering::Equal)
        );
        assert_eq!(
            Scalar::Bool(false).compare(&Scalar::Bool(true)),
            Some(Ordering::Less)
        );
        assert_eq!(Scalar::Int(1).compare(&Scalar::Text("1".into())), None);
        assert_eq!(Scalar::Float(f64::NAN).compare(&Scalar::Float(1.0)), None);
    }
}
