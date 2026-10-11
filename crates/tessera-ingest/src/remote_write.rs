// SPDX-License-Identifier: LicenseRef-Tessera-Pending

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
//! queries can honour them. Exemplars and native histograms in the
//! request are not stored.

use std::collections::BTreeMap;

use prost::Message;
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
            let mut identity = Vec::with_capacity(24);
            identity.extend_from_slice(&id);
            identity.extend_from_slice(&timestamp.as_nanos().to_le_bytes());
            rows.push(MetricRow {
                timestamp,
                observed_timestamp: now,
                event_id: crate::otlp::hash16(&identity),
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
    }
    if rows.is_empty() {
        return Ok(None);
    }
    Ok(Some(EventBatch::from_metrics(tenant, dataset, &rows)?))
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
    use tessera_types::event::{METRIC_TYPE, NAME, OVERFLOW, SERIES_ID, UNIT, VALUE};

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

    fn batch_of_empty() -> Option<EventBatch> {
        batch(&WriteRequest {
            timeseries: Vec::new(),
            metadata: Vec::new(),
        })
    }
}
