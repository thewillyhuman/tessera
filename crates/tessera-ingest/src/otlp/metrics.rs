// SPDX-License-Identifier: LicenseRef-Tessera-Pending

//! OTLP metrics (IN-1): decoding an export request and normalizing its
//! data points (IN-5).
//!
//! Every data point becomes a row of kind `metric` with the metric's
//! name, type and unit, the point's value or its buckets as JSON, and
//! the series identity digested from the name and the attributes
//! (section 4.5). A point's identity for deduplication is its series
//! and its time (section 7.5). The first exemplar's trace and span ids
//! ride along in the row's `trace_id` and `span_id`; the exemplars'
//! values and the points' start times are not stored.

use opentelemetry_proto::tonic::collector::metrics::v1::ExportMetricsServiceRequest;
use opentelemetry_proto::tonic::metrics::v1::metric::Data;
use opentelemetry_proto::tonic::metrics::v1::{
    Exemplar, ExponentialHistogramDataPoint, HistogramDataPoint, Metric, NumberDataPoint,
    ResourceMetrics, SummaryDataPoint, number_data_point,
};
use prost::Message;
use serde_json::{Value, json};
use tessera_types::event::{Attributes, Scalar};
use tessera_types::{
    DatasetName, EventBatch, MetricRow, MetricType, Temporality, TenantId, Timestamp, series_id,
};

use super::{OtlpError, flatten_all, nanos};

/// Parses the protobuf encoding of an export request.
///
/// # Errors
///
/// `Protobuf` if the bytes are not a request.
pub fn parse_protobuf(bytes: &[u8]) -> Result<ExportMetricsServiceRequest, OtlpError> {
    Ok(ExportMetricsServiceRequest::decode(bytes)?)
}

/// Parses the JSON encoding of an export request.
///
/// # Errors
///
/// `Json` if the bytes are not a request.
pub fn parse_json(bytes: &[u8]) -> Result<ExportMetricsServiceRequest, OtlpError> {
    Ok(serde_json::from_slice(bytes)?)
}

/// Normalizes every data point of `request` into one batch for the
/// given tenant and dataset, or `None` if the request has no points.
///
/// # Errors
///
/// `Batch` if the rows cannot be encoded.
pub fn to_batch(
    request: &ExportMetricsServiceRequest,
    tenant: TenantId,
    dataset: DatasetName,
    now: Timestamp,
) -> Result<Option<EventBatch>, OtlpError> {
    let mut rows = Vec::new();
    for resource_metrics in &request.resource_metrics {
        collect(resource_metrics, now, &mut rows);
    }
    if rows.is_empty() {
        return Ok(None);
    }
    Ok(Some(EventBatch::from_metrics(tenant, dataset, &rows)?))
}

fn collect(resource_metrics: &ResourceMetrics, now: Timestamp, rows: &mut Vec<MetricRow>) {
    let mut resource = Attributes::new();
    if let Some(r) = &resource_metrics.resource {
        flatten_all("resource", &r.attributes, &mut resource);
    }
    for scope_metrics in &resource_metrics.scope_metrics {
        let mut base = resource.clone();
        if let Some(scope) = &scope_metrics.scope {
            if !scope.name.is_empty() {
                base.insert("scope.name".into(), Scalar::Text(scope.name.clone()));
            }
            if !scope.version.is_empty() {
                base.insert("scope.version".into(), Scalar::Text(scope.version.clone()));
            }
            flatten_all("scope", &scope.attributes, &mut base);
        }
        for metric in &scope_metrics.metrics {
            metric_rows(metric, &base, now, rows);
        }
    }
}

/// The common part of every row of `metric`.
struct Shape<'a> {
    metric: &'a Metric,
    base: &'a Attributes,
    now: Timestamp,
    metric_type: MetricType,
    temporality: Temporality,
    is_monotonic: Option<bool>,
}

impl Shape<'_> {
    /// A row for a point at `time` with `attributes`, before its value.
    fn row(
        &self,
        time: u64,
        attributes: &[opentelemetry_proto::tonic::common::v1::KeyValue],
        exemplars: &[Exemplar],
    ) -> MetricRow {
        let mut all = self.base.clone();
        flatten_all("attr", attributes, &mut all);
        let timestamp = if time == 0 { self.now } else { nanos(time) };
        let series_id = series_id(&self.metric.name, &all);
        let exemplar = exemplars.first();
        MetricRow {
            timestamp,
            observed_timestamp: self.now,
            trace_id: exemplar.and_then(|e| e.trace_id.as_slice().try_into().ok()),
            span_id: exemplar.and_then(|e| e.span_id.as_slice().try_into().ok()),
            name: self.metric.name.clone(),
            metric_type: self.metric_type,
            unit: self.metric.unit.clone(),
            value: None,
            histogram: None,
            temporality: self.temporality,
            is_monotonic: self.is_monotonic,
            series_id,
            attributes: all,
        }
    }
}

fn metric_rows(metric: &Metric, base: &Attributes, now: Timestamp, rows: &mut Vec<MetricRow>) {
    let shape = |metric_type, temporality, is_monotonic| Shape {
        metric,
        base,
        now,
        metric_type,
        temporality,
        is_monotonic,
    };
    match &metric.data {
        Some(Data::Gauge(gauge)) => {
            let shape = shape(MetricType::Gauge, Temporality::Unspecified, None);
            rows.extend(gauge.data_points.iter().map(|p| number_row(&shape, p)));
        }
        Some(Data::Sum(sum)) => {
            let shape = shape(
                MetricType::Sum,
                Temporality::from_code(sum.aggregation_temporality),
                Some(sum.is_monotonic),
            );
            rows.extend(sum.data_points.iter().map(|p| number_row(&shape, p)));
        }
        Some(Data::Histogram(histogram)) => {
            let shape = shape(
                MetricType::Histogram,
                Temporality::from_code(histogram.aggregation_temporality),
                None,
            );
            rows.extend(
                histogram
                    .data_points
                    .iter()
                    .map(|p| histogram_row(&shape, p)),
            );
        }
        Some(Data::ExponentialHistogram(histogram)) => {
            let shape = shape(
                MetricType::ExpHistogram,
                Temporality::from_code(histogram.aggregation_temporality),
                None,
            );
            rows.extend(
                histogram
                    .data_points
                    .iter()
                    .map(|p| exponential_row(&shape, p)),
            );
        }
        Some(Data::Summary(summary)) => {
            let shape = shape(MetricType::Summary, Temporality::Unspecified, None);
            rows.extend(summary.data_points.iter().map(|p| summary_row(&shape, p)));
        }
        None => {}
    }
}

fn number_row(shape: &Shape<'_>, point: &NumberDataPoint) -> MetricRow {
    let mut row = shape.row(point.time_unix_nano, &point.attributes, &point.exemplars);
    #[allow(clippy::cast_precision_loss)]
    let value = match point.value {
        Some(number_data_point::Value::AsDouble(v)) => Some(v),
        Some(number_data_point::Value::AsInt(v)) => Some(v as f64),
        None => None,
    };
    row.value = value;
    row
}

fn histogram_row(shape: &Shape<'_>, point: &HistogramDataPoint) -> MetricRow {
    let mut row = shape.row(point.time_unix_nano, &point.attributes, &point.exemplars);
    row.histogram = Some(
        json!({
            "count": point.count,
            "sum": point.sum,
            "bounds": point.explicit_bounds,
            "counts": point.bucket_counts,
            "min": point.min,
            "max": point.max,
        })
        .to_string(),
    );
    row
}

fn exponential_row(shape: &Shape<'_>, point: &ExponentialHistogramDataPoint) -> MetricRow {
    let mut row = shape.row(point.time_unix_nano, &point.attributes, &point.exemplars);
    let buckets = |b: &Option<
        opentelemetry_proto::tonic::metrics::v1::exponential_histogram_data_point::Buckets,
    >| {
        b.as_ref().map_or(
            Value::Null,
            |b| json!({ "offset": b.offset, "counts": b.bucket_counts }),
        )
    };
    row.histogram = Some(
        json!({
            "count": point.count,
            "sum": point.sum,
            "scale": point.scale,
            "zero_count": point.zero_count,
            "zero_threshold": point.zero_threshold,
            "positive": buckets(&point.positive),
            "negative": buckets(&point.negative),
            "min": point.min,
            "max": point.max,
        })
        .to_string(),
    );
    row
}

fn summary_row(shape: &Shape<'_>, point: &SummaryDataPoint) -> MetricRow {
    let mut row = shape.row(point.time_unix_nano, &point.attributes, &[]);
    row.histogram = Some(
        json!({
            "count": point.count,
            "sum": point.sum,
            "quantiles": point
                .quantile_values
                .iter()
                .map(|q| json!([q.quantile, q.value]))
                .collect::<Vec<_>>(),
        })
        .to_string(),
    );
    row
}

#[cfg(test)]
mod tests {
    use arrow::array::{Array, BooleanArray, FixedSizeBinaryArray, Float64Array, StringArray};
    use opentelemetry_proto::tonic::common::v1::{AnyValue, KeyValue, any_value};
    use opentelemetry_proto::tonic::metrics::v1::summary_data_point::ValueAtQuantile;
    use opentelemetry_proto::tonic::metrics::v1::{
        Gauge, Histogram, ScopeMetrics, Sum, Summary, exemplar,
    };
    use opentelemetry_proto::tonic::resource::v1::Resource;
    use tessera_types::Kind;
    use tessera_types::event::{
        EVENT_ID, HISTOGRAM, IS_MONOTONIC, METRIC_TYPE, NAME, OVERFLOW, SERIES_ID, TEMPORALITY,
        TRACE_ID, UNIT, VALUE,
    };

    use super::*;

    fn kv(key: &str, value: &str) -> KeyValue {
        KeyValue {
            key: key.into(),
            value: Some(AnyValue {
                value: Some(any_value::Value::StringValue(value.into())),
            }),
            ..Default::default()
        }
    }

    fn point(time: u64, value: f64, method: &str) -> NumberDataPoint {
        NumberDataPoint {
            attributes: vec![kv("http.method", method)],
            time_unix_nano: time,
            value: Some(number_data_point::Value::AsDouble(value)),
            ..Default::default()
        }
    }

    fn request(metrics: Vec<Metric>) -> ExportMetricsServiceRequest {
        ExportMetricsServiceRequest {
            resource_metrics: vec![ResourceMetrics {
                resource: Some(Resource {
                    attributes: vec![kv("service.name", "checkout")],
                    ..Default::default()
                }),
                scope_metrics: vec![ScopeMetrics {
                    scope: None,
                    metrics,
                    schema_url: String::new(),
                }],
                schema_url: String::new(),
            }],
        }
    }

    fn decode(request: &ExportMetricsServiceRequest) -> Option<EventBatch> {
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
    fn normalizes_gauges_and_sums_with_their_series() {
        let batch = decode(&request(vec![
            Metric {
                name: "cpu.usage".into(),
                unit: "1".into(),
                data: Some(Data::Gauge(Gauge {
                    data_points: vec![point(1_700_000_000_000_000_000, 0.5, "GET")],
                })),
                ..Default::default()
            },
            Metric {
                name: "http.requests".into(),
                data: Some(Data::Sum(Sum {
                    data_points: vec![
                        point(1_700_000_000_000_000_000, 10.0, "GET"),
                        point(1_700_000_001_000_000_000, 12.0, "GET"),
                        point(1_700_000_000_000_000_000, 3.0, "POST"),
                    ],
                    aggregation_temporality: 2,
                    is_monotonic: true,
                })),
                ..Default::default()
            },
        ]))
        .unwrap();
        assert_eq!(batch.kind(), Kind::Metric);
        assert_eq!(batch.len(), 4);
        assert_eq!(text(&batch, NAME, 0), "cpu.usage");
        assert_eq!(text(&batch, METRIC_TYPE, 0), "gauge");
        assert_eq!(text(&batch, UNIT, 0), "1");
        assert_eq!(text(&batch, TEMPORALITY, 1), "cumulative");
        let values = batch.rows().column_by_name(VALUE).unwrap();
        let values = values.as_any().downcast_ref::<Float64Array>().unwrap();
        assert!((values.value(2) - 12.0).abs() < f64::EPSILON);
        let monotonic = batch.rows().column_by_name(IS_MONOTONIC).unwrap();
        let monotonic = monotonic.as_any().downcast_ref::<BooleanArray>().unwrap();
        assert!(monotonic.is_null(0) && monotonic.value(1));
        let series = batch.rows().column_by_name(SERIES_ID).unwrap();
        let series = series
            .as_any()
            .downcast_ref::<FixedSizeBinaryArray>()
            .unwrap();
        assert_eq!(series.value(1), series.value(2), "same name and labels");
        assert_ne!(series.value(1), series.value(3), "another method");
        assert!(
            batch.rows().column_by_name(EVENT_ID).is_none(),
            "a sample is identified by its series and time, nothing stored"
        );
        let attributes: Attributes = serde_json::from_str(&text(&batch, OVERFLOW, 0)).unwrap();
        assert_eq!(
            attributes["resource.service.name"],
            Scalar::Text("checkout".into())
        );
        assert_eq!(attributes["attr.http.method"], Scalar::Text("GET".into()));
    }

    #[test]
    fn histograms_and_summaries_keep_their_buckets_as_json() {
        let batch = decode(&request(vec![
            Metric {
                name: "latency".into(),
                unit: "ms".into(),
                data: Some(Data::Histogram(Histogram {
                    data_points: vec![HistogramDataPoint {
                        time_unix_nano: 1_700_000_000_000_000_000,
                        count: 3,
                        sum: Some(30.0),
                        bucket_counts: vec![1, 2, 0],
                        explicit_bounds: vec![10.0, 20.0],
                        exemplars: vec![Exemplar {
                            trace_id: vec![9; 16],
                            span_id: vec![8; 8],
                            value: Some(exemplar::Value::AsDouble(12.0)),
                            ..Default::default()
                        }],
                        ..Default::default()
                    }],
                    aggregation_temporality: 1,
                })),
                ..Default::default()
            },
            Metric {
                name: "rpc".into(),
                data: Some(Data::Summary(Summary {
                    data_points: vec![SummaryDataPoint {
                        time_unix_nano: 0,
                        count: 2,
                        sum: 5.0,
                        quantile_values: vec![ValueAtQuantile {
                            quantile: 0.5,
                            value: 2.5,
                        }],
                        ..Default::default()
                    }],
                })),
                ..Default::default()
            },
        ]))
        .unwrap();
        assert_eq!(text(&batch, METRIC_TYPE, 0), "histogram");
        assert_eq!(text(&batch, TEMPORALITY, 0), "delta");
        let histogram: Value = serde_json::from_str(&text(&batch, HISTOGRAM, 0)).unwrap();
        assert_eq!(histogram["count"], 3);
        assert_eq!(histogram["bounds"], json!([10.0, 20.0]));
        assert_eq!(histogram["counts"], json!([1, 2, 0]));
        assert!(batch.rows().column_by_name(VALUE).unwrap().is_null(0));
        let trace = batch.rows().column_by_name(TRACE_ID).unwrap();
        let trace = trace
            .as_any()
            .downcast_ref::<FixedSizeBinaryArray>()
            .unwrap();
        assert_eq!(trace.value(0), [9; 16], "the exemplar's trace");
        assert_eq!(text(&batch, METRIC_TYPE, 1), "summary");
        let summary: Value = serde_json::from_str(&text(&batch, HISTOGRAM, 1)).unwrap();
        assert_eq!(summary["quantiles"], json!([[0.5, 2.5]]));
        assert_eq!(
            batch.time_bounds().0,
            Timestamp::from_nanos(5),
            "a point without a time takes the ingest time"
        );
    }

    #[test]
    fn empty_requests_and_both_encodings() {
        assert!(decode(&ExportMetricsServiceRequest::default()).is_none());
        let request = request(vec![Metric {
            name: "x".into(),
            data: Some(Data::Gauge(Gauge {
                data_points: vec![point(1, 1.0, "GET")],
            })),
            ..Default::default()
        }]);
        assert_eq!(parse_protobuf(&request.encode_to_vec()).unwrap(), request);
        assert_eq!(
            parse_json(&serde_json::to_vec(&request).unwrap()).unwrap(),
            request
        );
        let retried = decode(&request).unwrap();
        let again = decode(&request).unwrap();
        assert_eq!(
            retried.rows(),
            again.rows(),
            "a retried export deduplicates"
        );
    }
}
