// SPDX-License-Identifier: Apache-2.0

//! Prometheus remote write (IN-2): decoding a snappy-compressed
//! `WriteRequest` and normalizing its samples into metric rows.
//!
//! A series' `__name__` label is the metric name and every other label
//! is a record attribute under `attr.` (the label-name mapping of
//! QL-19, applied on ingest). Remote write carries no types with its
//! samples; the request's metadata, when present, makes the family's
//! series counters (monotonic sums), and everything else is stored as
//! a gauge. A sample's identity is its series and time (section 7.5).
//! Prometheus' staleness markers are kept as the NaN they are, so that
//! queries can honour them. A native histogram sample (QL-5) is stored
//! as a histogram point: an exponential one in the shape an OTLP
//! exponential histogram takes, a custom-bucket one as a classic point
//! of bounds and counts, each with its reset hint. Exemplars are not
//! stored.

use std::collections::BTreeMap;

use prost::Message;
use serde_json::{Map, Value, json};
use tessera_types::event::{Attributes, Scalar};
use tessera_types::{
    DatasetName, EventBatch, InvalidBatch, MetricRow, MetricType, Temporality, TenantId, Timestamp,
    series_id,
};

/// A remote write request (`prometheus.WriteRequest`).
#[derive(Clone, PartialEq, Message)]
pub struct WriteRequest {
    /// The series with their samples.
    #[prost(message, repeated, tag = "1")]
    pub timeseries: Vec<TimeSeries>,
    /// The metadata of the metric families, when sent.
    #[prost(message, repeated, tag = "3")]
    pub metadata: Vec<MetricMetadata>,
}

/// One series of a request.
#[derive(Clone, PartialEq, Message)]
pub struct TimeSeries {
    /// The labels, `__name__` among them.
    #[prost(message, repeated, tag = "1")]
    pub labels: Vec<Label>,
    /// The samples.
    #[prost(message, repeated, tag = "2")]
    pub samples: Vec<Sample>,
    /// The native histogram samples.
    #[prost(message, repeated, tag = "4")]
    pub histograms: Vec<Histogram>,
}

/// A native histogram sample (`prometheus.Histogram`): the buckets of
/// each side as spans of consecutive indexes, their counts as deltas
/// from one bucket to the next for an integer histogram or absolute
/// for a float one.
#[derive(Clone, PartialEq, Message)]
pub struct Histogram {
    /// The observations in all.
    #[prost(oneof = "Count", tags = "1, 2")]
    pub count: Option<Count>,
    /// The sum of the observations; a staleness marker's NaN for one.
    #[prost(double, tag = "3")]
    pub sum: f64,
    /// The schema: -4 to 8 for exponential buckets, -53 for custom ones.
    #[prost(sint32, tag = "4")]
    pub schema: i32,
    /// Half the width of the zero bucket.
    #[prost(double, tag = "5")]
    pub zero_threshold: f64,
    /// The observations in the zero bucket.
    #[prost(oneof = "ZeroCount", tags = "6, 7")]
    pub zero_count: Option<ZeroCount>,
    /// The spans of negative buckets.
    #[prost(message, repeated, tag = "8")]
    pub negative_spans: Vec<BucketSpan>,
    /// The negative buckets' counts as deltas, for an integer histogram.
    #[prost(sint64, repeated, tag = "9")]
    pub negative_deltas: Vec<i64>,
    /// The negative buckets' counts, for a float histogram.
    #[prost(double, repeated, tag = "10")]
    pub negative_counts: Vec<f64>,
    /// The spans of positive buckets.
    #[prost(message, repeated, tag = "11")]
    pub positive_spans: Vec<BucketSpan>,
    /// The positive buckets' counts as deltas, for an integer histogram.
    #[prost(sint64, repeated, tag = "12")]
    pub positive_deltas: Vec<i64>,
    /// The positive buckets' counts, for a float histogram.
    #[prost(double, repeated, tag = "13")]
    pub positive_counts: Vec<f64>,
    /// What the sample says about counter resets: 0 unknown, 1 a reset,
    /// 2 no reset, 3 a gauge.
    #[prost(int32, tag = "14")]
    pub reset_hint: i32,
    /// Milliseconds since the epoch.
    #[prost(int64, tag = "15")]
    pub timestamp: i64,
    /// The upper bounds of custom buckets.
    #[prost(double, repeated, tag = "16")]
    pub custom_values: Vec<f64>,
}

/// A count of observations, as an integer or a float.
#[derive(Clone, PartialEq, prost::Oneof)]
pub enum Count {
    /// An integer count.
    #[prost(uint64, tag = "1")]
    Int(u64),
    /// A float count.
    #[prost(double, tag = "2")]
    Float(f64),
}

/// The zero bucket's count, as an integer or a float.
#[derive(Clone, PartialEq, prost::Oneof)]
pub enum ZeroCount {
    /// An integer count.
    #[prost(uint64, tag = "6")]
    Int(u64),
    /// A float count.
    #[prost(double, tag = "7")]
    Float(f64),
}

/// A run of consecutive buckets.
#[derive(Clone, PartialEq, Message)]
pub struct BucketSpan {
    /// The gap from the previous span's end; the first bucket's index
    /// for the first span.
    #[prost(sint32, tag = "1")]
    pub offset: i32,
    /// The number of buckets.
    #[prost(uint32, tag = "2")]
    pub length: u32,
}

/// A label.
#[derive(Clone, PartialEq, Message)]
pub struct Label {
    /// The name.
    #[prost(string, tag = "1")]
    pub name: String,
    /// The value.
    #[prost(string, tag = "2")]
    pub value: String,
}

/// A sample.
#[derive(Clone, PartialEq, Message)]
pub struct Sample {
    /// The value; a staleness marker is a NaN with a fixed payload.
    #[prost(double, tag = "1")]
    pub value: f64,
    /// Milliseconds since the epoch.
    #[prost(int64, tag = "2")]
    pub timestamp: i64,
}

/// The metadata of a metric family.
#[derive(Clone, PartialEq, Message)]
pub struct MetricMetadata {
    /// The type: 1 counter, 2 gauge, 3 histogram, 4 gauge histogram,
    /// 5 summary, 6 info, 7 stateset.
    #[prost(int32, tag = "1")]
    pub r#type: i32,
    /// The family name.
    #[prost(string, tag = "2")]
    pub metric_family_name: String,
    /// The help text.
    #[prost(string, tag = "4")]
    pub help: String,
    /// The unit.
    #[prost(string, tag = "5")]
    pub unit: String,
}

/// The bit pattern of Prometheus' staleness marker.
pub const STALE_NAN_BITS: u64 = 0x7ff0_0000_0000_0002;

/// Why a request could not be decoded.
#[derive(Debug, thiserror::Error)]
pub enum RemoteWriteError {
    /// The body is not snappy-compressed.
    #[error("malformed snappy body: {0}")]
    Snappy(#[from] snap::Error),
    /// The body is not a write request.
    #[error("malformed remote write protobuf: {0}")]
    Protobuf(#[from] prost::DecodeError),
    /// A series has no `__name__` label.
    #[error("a series has no __name__ label")]
    Unnamed,
    /// The rows could not be turned into a batch.
    #[error(transparent)]
    Batch(#[from] InvalidBatch),
}

/// Decodes a snappy-compressed protobuf write request.
///
/// # Errors
///
/// `Snappy` or `Protobuf` when the body is not one.
pub fn decode(body: &[u8]) -> Result<WriteRequest, RemoteWriteError> {
    let bytes = snap::raw::Decoder::new().decompress_vec(body)?;
    Ok(WriteRequest::decode(bytes.as_slice())?)
}

/// Normalizes every sample of `request` into one batch for the given
/// tenant and dataset, or `None` if the request has no samples.
///
/// # Errors
///
/// `Unnamed` for a series without a name; `Batch` if the rows cannot be
/// encoded.
pub fn to_batch(
    request: &WriteRequest,
    tenant: TenantId,
    dataset: DatasetName,
    now: Timestamp,
) -> Result<Option<EventBatch>, RemoteWriteError> {
    let families: BTreeMap<&str, &MetricMetadata> = request
        .metadata
        .iter()
        .map(|m| (m.metric_family_name.as_str(), m))
        .collect();
    let mut rows = Vec::new();
    for series in &request.timeseries {
        let mut name = None;
        let mut attributes = Attributes::new();
        for label in &series.labels {
            if label.name == "__name__" {
                name = Some(label.value.as_str());
            } else {
                attributes.insert(
                    format!("attr.{}", label.name),
                    Scalar::Text(label.value.clone()),
                );
            }
        }
        let name = name.ok_or(RemoteWriteError::Unnamed)?;
        let metadata = families
            .get(name)
            .or_else(|| families.get(family_of(name)))
            .copied();
        let (metric_type, is_monotonic) = match metadata.map(|m| m.r#type) {
            Some(1) => (MetricType::Sum, Some(true)),
            _ => (MetricType::Gauge, None),
        };
        let id = series_id(name, &attributes);
        for sample in &series.samples {
            let timestamp = Timestamp::from_nanos(sample.timestamp.saturating_mul(1_000_000));
            rows.push(MetricRow {
                timestamp,
                observed_timestamp: now,
                name: name.to_owned(),
                metric_type,
                unit: metadata.map(|m| m.unit.clone()).unwrap_or_default(),
                value: Some(sample.value),
                temporality: if metric_type == MetricType::Sum {
                    Temporality::Cumulative
                } else {
                    Temporality::Unspecified
                },
                is_monotonic,
                series_id: id,
                attributes: attributes.clone(),
                ..MetricRow::default()
            });
        }
        for histogram in &series.histograms {
            let (metric_type, point) = histogram_point(histogram);
            rows.push(MetricRow {
                timestamp: Timestamp::from_nanos(histogram.timestamp.saturating_mul(1_000_000)),
                observed_timestamp: now,
                name: name.to_owned(),
                metric_type,
                unit: metadata.map(|m| m.unit.clone()).unwrap_or_default(),
                histogram: Some(point),
                temporality: Temporality::Cumulative,
                series_id: id,
                attributes: attributes.clone(),
                ..MetricRow::default()
            });
        }
    }
    if rows.is_empty() {
        return Ok(None);
    }
    Ok(Some(EventBatch::from_metrics(tenant, dataset, &rows)?))
}

/// The stored point of a native histogram sample and its metric type:
/// an exponential histogram's point keeps the schema as its scale and
/// each side as a run of counts from an offset one below Prometheus'
/// first index, as an OTLP point does; a custom-bucket histogram's
/// point is a classic one, the bounds and a count per bucket with the
/// `+Inf` bucket last. The reset hint is kept when it says anything.
#[allow(clippy::cast_precision_loss)]
fn histogram_point(histogram: &Histogram) -> (MetricType, String) {
    let count = match histogram.count {
        Some(Count::Int(n)) => n as f64,
        Some(Count::Float(n)) => n,
        None => 0.0,
    };
    let zero_count = match histogram.zero_count {
        Some(ZeroCount::Int(n)) => n as f64,
        Some(ZeroCount::Float(n)) => n,
        None => 0.0,
    };
    let positive = buckets_of(
        &histogram.positive_spans,
        &histogram.positive_deltas,
        &histogram.positive_counts,
    );
    let mut object = Map::new();
    object.insert("count".into(), number(count));
    object.insert("sum".into(), number(histogram.sum));
    let metric_type = if histogram.schema == CUSTOM_BUCKETS_SCHEMA {
        let mut counts = vec![0.0; histogram.custom_values.len() + 1];
        for (index, count) in positive {
            if let Some(slot) = usize::try_from(index).ok().and_then(|i| counts.get_mut(i)) {
                *slot = count;
            }
        }
        object.insert("bounds".into(), json!(histogram.custom_values));
        object.insert(
            "counts".into(),
            counts.into_iter().map(number).collect::<Vec<_>>().into(),
        );
        MetricType::Histogram
    } else {
        let negative = buckets_of(
            &histogram.negative_spans,
            &histogram.negative_deltas,
            &histogram.negative_counts,
        );
        object.insert("scale".into(), json!(histogram.schema));
        object.insert("zero_count".into(), number(zero_count));
        object.insert("zero_threshold".into(), number(histogram.zero_threshold));
        object.insert("positive".into(), dense(&positive));
        object.insert("negative".into(), dense(&negative));
        MetricType::ExpHistogram
    };
    let hint = match histogram.reset_hint {
        1 => Some("reset"),
        2 => Some("no_reset"),
        3 => Some("gauge"),
        _ => None,
    };
    if let Some(hint) = hint {
        object.insert("hint".into(), json!(hint));
    }
    (metric_type, Value::Object(object).to_string())
}

/// The schema of a histogram with custom buckets.
const CUSTOM_BUCKETS_SCHEMA: i32 = -53;

/// The buckets of one side as Prometheus indexes with absolute counts:
/// the spans give the indexes, deltas add up along them, float counts
/// stand as they are.
#[allow(clippy::cast_precision_loss)]
fn buckets_of(spans: &[BucketSpan], deltas: &[i64], counts: &[f64]) -> Vec<(i64, f64)> {
    let mut out = Vec::new();
    let (mut index, mut running, mut at) = (0i64, 0i64, 0usize);
    for span in spans {
        index = index.saturating_add(i64::from(span.offset));
        for _ in 0..span.length {
            let count = if counts.is_empty() {
                running = running.saturating_add(deltas.get(at).copied().unwrap_or(0));
                running as f64
            } else {
                counts.get(at).copied().unwrap_or(0.0)
            };
            out.push((index, count));
            index = index.saturating_add(1);
            at += 1;
        }
    }
    out
}

/// One side as the stored offset, one below the first Prometheus
/// index, and a dense run of counts.
fn dense(buckets: &[(i64, f64)]) -> Value {
    let (Some(first), Some(last)) = (buckets.first(), buckets.last()) else {
        return json!({ "offset": 0, "counts": [] });
    };
    let Ok(width) = usize::try_from(last.0 - first.0 + 1) else {
        return json!({ "offset": 0, "counts": [] });
    };
    let mut counts = vec![0.0; width];
    for (index, count) in buckets {
        if let Some(slot) = usize::try_from(index - first.0)
            .ok()
            .and_then(|i| counts.get_mut(i))
        {
            *slot = *count;
        }
    }
    json!({
        "offset": first.0 - 1,
        "counts": counts.into_iter().map(number).collect::<Vec<_>>(),
    })
}

/// A number as the stored point holds it: as text when JSON cannot
/// hold it, the staleness marker's NaN apart from any other.
fn number(value: f64) -> Value {
    if value.is_finite() {
        json!(value)
    } else if value.to_bits() == STALE_NAN_BITS {
        json!("StaleNaN")
    } else if value.is_nan() {
        json!("NaN")
    } else if value > 0.0 {
        json!("+Inf")
    } else {
        json!("-Inf")
    }
}

/// The family a series name belongs to: the name without the suffix a
/// histogram or summary series carries.
fn family_of(name: &str) -> &str {
    ["_bucket", "_count", "_sum", "_total"]
        .iter()
        .find_map(|suffix| name.strip_suffix(suffix))
        .unwrap_or(name)
}

#[cfg(test)]
mod tests {
    use arrow::array::{Array, FixedSizeBinaryArray, Float64Array, StringArray};
    use tessera_types::Kind;
    use tessera_types::event::{
        HISTOGRAM, METRIC_TYPE, NAME, OVERFLOW, SERIES_ID, TEMPORALITY, UNIT, VALUE,
    };

    use super::*;

    fn series(name: &str, labels: &[(&str, &str)], samples: &[(i64, f64)]) -> TimeSeries {
        let mut all = vec![Label {
            name: "__name__".into(),
            value: name.into(),
        }];
        all.extend(labels.iter().map(|(n, v)| Label {
            name: (*n).into(),
            value: (*v).into(),
        }));
        TimeSeries {
            labels: all,
            samples: samples
                .iter()
                .map(|(t, v)| Sample {
                    value: *v,
                    timestamp: *t,
                })
                .collect(),
            histograms: Vec::new(),
        }
    }

    fn batch(request: &WriteRequest) -> Option<EventBatch> {
        to_batch(
            request,
            TenantId::DEFAULT,
            DatasetName::new("prod").unwrap(),
            Timestamp::from_nanos(5),
        )
        .unwrap()
    }

    fn text(batch: &EventBatch, column: &str, row: usize) -> String {
        batch
            .rows()
            .column_by_name(column)
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .value(row)
            .to_owned()
    }

    #[test]
    fn decodes_a_compressed_request() {
        let request = WriteRequest {
            timeseries: vec![series("up", &[("job", "api")], &[(1_700_000_000_000, 1.0)])],
            metadata: Vec::new(),
        };
        let compressed = snap::raw::Encoder::new()
            .compress_vec(&request.encode_to_vec())
            .unwrap();
        assert_eq!(decode(&compressed).unwrap(), request);
        assert!(matches!(
            decode(b"not snappy"),
            Err(RemoteWriteError::Snappy(_))
        ));
    }

    #[test]
    fn labels_become_attributes_and_metadata_makes_counters() {
        let request = WriteRequest {
            timeseries: vec![
                series(
                    "http_requests_total",
                    &[("job", "api"), ("instance", "a:9090")],
                    &[(1_700_000_000_000, 10.0), (1_700_000_015_000, 12.0)],
                ),
                series("up", &[("job", "api")], &[(1_700_000_000_000, 1.0)]),
            ],
            metadata: vec![MetricMetadata {
                r#type: 1,
                metric_family_name: "http_requests".into(),
                help: String::new(),
                unit: String::new(),
            }],
        };
        let batch = batch(&request).unwrap();
        assert_eq!(batch.kind(), Kind::Metric);
        assert_eq!(batch.len(), 3);
        assert_eq!(text(&batch, NAME, 0), "http_requests_total");
        assert_eq!(
            text(&batch, METRIC_TYPE, 0),
            "sum",
            "a counter by its family's metadata"
        );
        assert_eq!(text(&batch, METRIC_TYPE, 2), "gauge");
        assert_eq!(text(&batch, UNIT, 0), "");
        let attributes: Attributes = serde_json::from_str(&text(&batch, OVERFLOW, 0)).unwrap();
        assert_eq!(attributes["attr.job"], Scalar::Text("api".into()));
        assert_eq!(attributes["attr.instance"], Scalar::Text("a:9090".into()));
        assert_eq!(
            batch.time_bounds().0,
            Timestamp::from_nanos(1_700_000_000_000_000_000)
        );
        let values = batch.rows().column_by_name(VALUE).unwrap();
        let values = values.as_any().downcast_ref::<Float64Array>().unwrap();
        assert!((values.value(1) - 12.0).abs() < f64::EPSILON);
        let ids = batch.rows().column_by_name(SERIES_ID).unwrap();
        let ids = ids.as_any().downcast_ref::<FixedSizeBinaryArray>().unwrap();
        assert_eq!(ids.value(0), ids.value(1));
        assert_ne!(ids.value(0), ids.value(2));
    }

    #[test]
    fn stale_markers_survive_and_unnamed_series_are_rejected() {
        let stale = f64::from_bits(STALE_NAN_BITS);
        let request = WriteRequest {
            timeseries: vec![series("up", &[], &[(1_700_000_000_000, stale)])],
            metadata: Vec::new(),
        };
        let batch = batch(&request).unwrap();
        let values = batch.rows().column_by_name(VALUE).unwrap();
        let values = values.as_any().downcast_ref::<Float64Array>().unwrap();
        assert_eq!(values.value(0).to_bits(), STALE_NAN_BITS);
        let unnamed = WriteRequest {
            timeseries: vec![TimeSeries {
                labels: vec![Label {
                    name: "job".into(),
                    value: "x".into(),
                }],
                samples: vec![Sample {
                    value: 1.0,
                    timestamp: 1,
                }],
                histograms: Vec::new(),
            }],
            metadata: Vec::new(),
        };
        assert!(matches!(
            to_batch(
                &unnamed,
                TenantId::DEFAULT,
                DatasetName::new("prod").unwrap(),
                Timestamp::from_nanos(5)
            ),
            Err(RemoteWriteError::Unnamed)
        ));
        assert!(batch_of_empty().is_none());
    }

    /// QL-5: a native histogram sample is stored as a histogram point
    /// with its reset hint, exponential buckets under their stored
    /// offset and custom buckets as bounds and counts.
    #[test]
    fn native_histograms_are_stored_as_points() {
        let span = |offset: i32, length: u32| BucketSpan { offset, length };
        let exponential = Histogram {
            count: Some(Count::Int(7)),
            sum: 3.5,
            schema: 1,
            zero_threshold: 0.001,
            zero_count: Some(ZeroCount::Int(1)),
            positive_spans: vec![span(0, 2), span(1, 1)],
            positive_deltas: vec![2, -2, 3],
            negative_spans: vec![span(3, 1)],
            negative_deltas: vec![1],
            reset_hint: 2,
            timestamp: 1_700_000_000_000,
            ..Histogram::default()
        };
        let custom = Histogram {
            count: Some(Count::Float(4.0)),
            sum: 5.0,
            schema: -53,
            positive_spans: vec![span(0, 3)],
            positive_counts: vec![1.0, 2.0, 1.0],
            custom_values: vec![5.0, 10.0],
            timestamp: 1_700_000_015_000,
            ..Histogram::default()
        };
        let stale = Histogram {
            count: Some(Count::Float(f64::from_bits(STALE_NAN_BITS))),
            sum: f64::from_bits(STALE_NAN_BITS),
            timestamp: 1_700_000_030_000,
            ..Histogram::default()
        };
        let request = WriteRequest {
            timeseries: vec![TimeSeries {
                labels: vec![
                    Label {
                        name: "__name__".into(),
                        value: "latency".into(),
                    },
                    Label {
                        name: "job".into(),
                        value: "api".into(),
                    },
                ],
                samples: Vec::new(),
                histograms: vec![exponential, custom, stale],
            }],
            metadata: Vec::new(),
        };
        let batch = batch(&request).unwrap();
        assert_eq!(batch.len(), 3);
        assert_eq!(text(&batch, NAME, 0), "latency");
        assert_eq!(text(&batch, METRIC_TYPE, 0), "exp_histogram");
        assert_eq!(text(&batch, TEMPORALITY, 0), "cumulative");
        assert!(batch.rows().column_by_name(VALUE).unwrap().is_null(0));
        let point: serde_json::Value = serde_json::from_str(&text(&batch, HISTOGRAM, 0)).unwrap();
        assert_eq!(point["count"], 7.0);
        assert_eq!(point["sum"], 3.5);
        assert_eq!(point["scale"], 1);
        assert_eq!(point["zero_count"], 1.0);
        assert_eq!(point["zero_threshold"], 0.001);
        assert_eq!(
            point["positive"],
            json!({ "offset": -1, "counts": [2.0, 0.0, 0.0, 3.0] }),
            "deltas add up, a gap is filled, the offset is one below the first index"
        );
        assert_eq!(point["negative"], json!({ "offset": 2, "counts": [1.0] }));
        assert_eq!(point["hint"], "no_reset");
        assert_eq!(text(&batch, METRIC_TYPE, 1), "histogram");
        let point: serde_json::Value = serde_json::from_str(&text(&batch, HISTOGRAM, 1)).unwrap();
        assert_eq!(point["bounds"], json!([5.0, 10.0]));
        assert_eq!(point["counts"], json!([1.0, 2.0, 1.0]));
        assert!(point.get("hint").is_none(), "an unknown hint says nothing");
        assert_eq!(
            batch.time_bounds().1,
            Timestamp::from_nanos(1_700_000_030_000_000_000)
        );
        let point: serde_json::Value = serde_json::from_str(&text(&batch, HISTOGRAM, 2)).unwrap();
        assert_eq!(
            point["sum"], "StaleNaN",
            "a staleness marker keeps its bits"
        );
    }

    fn batch_of_empty() -> Option<EventBatch> {
        batch(&WriteRequest {
            timeseries: Vec::new(),
            metadata: Vec::new(),
        })
    }
}
