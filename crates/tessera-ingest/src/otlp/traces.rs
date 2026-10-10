// SPDX-License-Identifier: LicenseRef-Tessera-Pending

//! OTLP traces (IN-1): decoding an export request and normalizing its
//! spans (IN-5).
//!
//! A span becomes a row of kind `span`; its events become rows of kind
//! `event` carrying the span's ids (DM-8); its links go to the `links`
//! column as JSON (DM-9). Attributes flatten as for logs. A span's
//! identity is its trace and span id (section 7.5), so a retried export
//! deduplicates; a span event's identity adds the event's time and name.
//! Trace state and dropped-attribute counts are not stored.

use opentelemetry_proto::tonic::collector::trace::v1::ExportTraceServiceRequest;
use opentelemetry_proto::tonic::trace::v1::span::{Event, Link};
use opentelemetry_proto::tonic::trace::v1::{ResourceSpans, Span, Status};
use prost::Message;
use serde_json::{Map, Value};
use tessera_types::event::{Attributes, Scalar};
use tessera_types::{
    DatasetName, EventBatch, EventRow, SpanKind, SpanRow, StatusCode, TenantId, Timestamp,
};

use super::{OtlpError, flatten_all, hash16, hex, json, nanos};

/// Parses the protobuf encoding of an export request.
///
/// # Errors
///
/// `Protobuf` if the bytes are not a request.
pub fn parse_protobuf(bytes: &[u8]) -> Result<ExportTraceServiceRequest, OtlpError> {
    Ok(ExportTraceServiceRequest::decode(bytes)?)
}

/// Parses the JSON encoding of an export request.
///
/// # Errors
///
/// `Json` if the bytes are not a request.
pub fn parse_json(bytes: &[u8]) -> Result<ExportTraceServiceRequest, OtlpError> {
    Ok(serde_json::from_slice(bytes)?)
}

/// Normalizes every span of `request` into a batch of spans and, when
/// any span has events, a batch of events, for the given tenant and
/// dataset. An empty request gives no batches.
///
/// # Errors
///
/// `Invalid` if a span's ids are not 16 and 8 bytes; `Batch` if the
/// rows cannot be encoded.
pub fn to_batches(
    request: &ExportTraceServiceRequest,
    tenant: TenantId,
    dataset: DatasetName,
    now: Timestamp,
) -> Result<Vec<EventBatch>, OtlpError> {
    let mut spans = Vec::new();
    let mut events = Vec::new();
    for resource_spans in &request.resource_spans {
        collect(resource_spans, now, &mut spans, &mut events)?;
    }
    let mut batches = Vec::new();
    if !spans.is_empty() {
        batches.push(EventBatch::from_spans(
            tenant.clone(),
            dataset.clone(),
            &spans,
        )?);
    }
    if !events.is_empty() {
        batches.push(EventBatch::from_events(tenant, dataset, &events)?);
    }
    Ok(batches)
}

fn collect(
    resource_spans: &ResourceSpans,
    now: Timestamp,
    spans: &mut Vec<SpanRow>,
    events: &mut Vec<EventRow>,
) -> Result<(), OtlpError> {
    let mut resource = Attributes::new();
    if let Some(r) = &resource_spans.resource {
        flatten_all("resource", &r.attributes, &mut resource);
    }
    for scope_spans in &resource_spans.scope_spans {
        let mut base = resource.clone();
        if let Some(scope) = &scope_spans.scope {
            if !scope.name.is_empty() {
                base.insert("scope.name".into(), Scalar::Text(scope.name.clone()));
            }
            if !scope.version.is_empty() {
                base.insert("scope.version".into(), Scalar::Text(scope.version.clone()));
            }
            flatten_all("scope", &scope.attributes, &mut base);
        }
        for span in &scope_spans.spans {
            let row = span_row(span, &base, now)?;
            for event in &span.events {
                events.push(event_row(event, &row, &base));
            }
            spans.push(row);
        }
    }
    Ok(())
}

fn span_row(span: &Span, base: &Attributes, now: Timestamp) -> Result<SpanRow, OtlpError> {
    let trace_id: [u8; 16] = span
        .trace_id
        .as_slice()
        .try_into()
        .map_err(|_| OtlpError::Invalid("a span's trace id must be 16 bytes".into()))?;
    let span_id: [u8; 8] = span
        .span_id
        .as_slice()
        .try_into()
        .map_err(|_| OtlpError::Invalid("a span's id must be 8 bytes".into()))?;
    let parent_span_id = match span.parent_span_id.as_slice() {
        [] => None,
        bytes => Some(
            bytes
                .try_into()
                .map_err(|_| OtlpError::Invalid("a parent span id must be 8 bytes".into()))?,
        ),
    };
    let mut identity = Vec::with_capacity(24);
    identity.extend_from_slice(&trace_id);
    identity.extend_from_slice(&span_id);
    let mut attributes = base.clone();
    flatten_all("attr", &span.attributes, &mut attributes);
    let (status_code, status_message) = status(span.status.as_ref());
    let elapsed = span
        .end_time_unix_nano
        .saturating_sub(span.start_time_unix_nano);
    Ok(SpanRow {
        timestamp: nanos(span.start_time_unix_nano),
        observed_timestamp: now,
        event_id: hash16(&identity),
        trace_id,
        span_id,
        parent_span_id,
        name: span.name.clone(),
        span_kind: SpanKind::from_code(span.kind),
        duration_ns: i64::try_from(elapsed).unwrap_or(i64::MAX),
        status_code,
        status_message,
        links: links(&span.links),
        attributes,
    })
}

fn status(status: Option<&Status>) -> (StatusCode, Option<String>) {
    match status {
        None => (StatusCode::Unset, None),
        Some(status) => (
            StatusCode::from_code(status.code),
            (!status.message.is_empty()).then(|| status.message.clone()),
        ),
    }
}

/// The `links` column value: a JSON list of objects with the linked
/// `trace_id` and `span_id` as hex and the link's `attributes` by key.
fn links(links: &[Link]) -> Option<String> {
    if links.is_empty() {
        return None;
    }
    let list: Vec<Value> = links
        .iter()
        .map(|link| {
            let attributes: Map<String, Value> = link
                .attributes
                .iter()
                .filter_map(|kv| {
                    kv.value
                        .as_ref()
                        .and_then(|v| v.value.as_ref())
                        .map(|v| (kv.key.clone(), json(v)))
                })
                .collect();
            let mut object = Map::new();
            object.insert("trace_id".into(), Value::String(hex(&link.trace_id)));
            object.insert("span_id".into(), Value::String(hex(&link.span_id)));
            object.insert("attributes".into(), Value::Object(attributes));
            Value::Object(object)
        })
        .collect();
    Some(Value::Array(list).to_string())
}

fn event_row(event: &Event, span: &SpanRow, base: &Attributes) -> EventRow {
    let mut identity = Vec::with_capacity(32 + event.name.len());
    identity.extend_from_slice(&span.trace_id);
    identity.extend_from_slice(&span.span_id);
    identity.extend_from_slice(&event.time_unix_nano.to_le_bytes());
    identity.extend_from_slice(event.name.as_bytes());
    let mut attributes = base.clone();
    flatten_all("attr", &event.attributes, &mut attributes);
    EventRow {
        timestamp: nanos(event.time_unix_nano),
        observed_timestamp: span.observed_timestamp,
        event_id: hash16(&identity),
        trace_id: Some(span.trace_id),
        span_id: Some(span.span_id),
        name: event.name.clone(),
        attributes,
    }
}

#[cfg(test)]
mod tests {
    use arrow::array::{Array, FixedSizeBinaryArray, Int64Array, StringArray};
    use opentelemetry_proto::tonic::common::v1::{AnyValue, KeyValue, any_value};
    use opentelemetry_proto::tonic::resource::v1::Resource;
    use opentelemetry_proto::tonic::trace::v1::ScopeSpans;
    use tessera_types::Kind;
    use tessera_types::event::{
        DURATION_NS, EVENT_ID, LINKS, NAME, OVERFLOW, PARENT_SPAN_ID, SPAN_ID, SPAN_KIND,
        STATUS_CODE, STATUS_MESSAGE, TRACE_ID,
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

    fn span(span_id: u8, parent: Option<u8>) -> Span {
        Span {
            trace_id: vec![7; 16],
            span_id: vec![span_id; 8],
            parent_span_id: parent.map(|p| vec![p; 8]).unwrap_or_default(),
            name: "GET /".into(),
            kind: 2,
            start_time_unix_nano: 1_700_000_000_000_000_000,
            end_time_unix_nano: 1_700_000_000_000_001_500,
            attributes: vec![kv("http.method", "GET")],
            status: Some(Status {
                code: 2,
                message: "boom".into(),
            }),
            ..Default::default()
        }
    }

    fn request(spans: Vec<Span>) -> ExportTraceServiceRequest {
        ExportTraceServiceRequest {
            resource_spans: vec![ResourceSpans {
                resource: Some(Resource {
                    attributes: vec![kv("service.name", "checkout")],
                    ..Default::default()
                }),
                scope_spans: vec![ScopeSpans {
                    scope: None,
                    spans,
                    schema_url: String::new(),
                }],
                schema_url: String::new(),
            }],
        }
    }

    fn decode(request: &ExportTraceServiceRequest) -> Vec<EventBatch> {
        to_batches(
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

    fn bytes(batch: &EventBatch, column: &str, row: usize) -> Vec<u8> {
        batch
            .rows()
            .column_by_name(column)
            .unwrap()
            .as_any()
            .downcast_ref::<FixedSizeBinaryArray>()
            .unwrap()
            .value(row)
            .to_vec()
    }

    #[test]
    fn normalizes_spans_with_their_columns() {
        let batches = decode(&request(vec![span(1, None), span(2, Some(1))]));
        assert_eq!(batches.len(), 1, "no events, so only a span batch");
        let batch = &batches[0];
        assert_eq!(batch.kind(), Kind::Span);
        assert_eq!(batch.len(), 2);
        assert_eq!(bytes(batch, TRACE_ID, 0), vec![7; 16]);
        assert_eq!(bytes(batch, SPAN_ID, 1), vec![2; 8]);
        assert!(
            batch
                .rows()
                .column_by_name(PARENT_SPAN_ID)
                .unwrap()
                .is_null(0)
        );
        assert_eq!(bytes(batch, PARENT_SPAN_ID, 1), vec![1; 8]);
        assert_eq!(text(batch, NAME, 0), "GET /");
        assert_eq!(text(batch, SPAN_KIND, 0), "server");
        assert_eq!(text(batch, STATUS_CODE, 0), "error");
        assert_eq!(text(batch, STATUS_MESSAGE, 0), "boom");
        let duration = batch.rows().column_by_name(DURATION_NS).unwrap();
        let duration = duration.as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(duration.value(0), 1_500);
        assert!(batch.rows().column_by_name(LINKS).unwrap().is_null(0));
        let attributes: Attributes = serde_json::from_str(&text(batch, OVERFLOW, 0)).unwrap();
        assert_eq!(
            attributes["resource.service.name"],
            Scalar::Text("checkout".into())
        );
        assert_eq!(attributes["attr.http.method"], Scalar::Text("GET".into()));
        assert_eq!(
            batch.time_bounds().0,
            Timestamp::from_nanos(1_700_000_000_000_000_000)
        );
        assert_eq!(batch.observed_bounds().0, Timestamp::from_nanos(5));
    }

    #[test]
    fn span_events_become_event_rows_tied_to_their_span() {
        let mut with_event = span(1, None);
        with_event.events.push(Event {
            time_unix_nano: 1_700_000_000_000_000_700,
            name: "exception".into(),
            attributes: vec![kv("exception.type", "IOError")],
            dropped_attributes_count: 0,
        });
        let batches = decode(&request(vec![with_event]));
        assert_eq!(batches.len(), 2);
        let events = &batches[1];
        assert_eq!(events.kind(), Kind::Event);
        assert_eq!(events.len(), 1);
        assert_eq!(text(events, NAME, 0), "exception");
        assert_eq!(bytes(events, TRACE_ID, 0), vec![7; 16]);
        assert_eq!(bytes(events, SPAN_ID, 0), vec![1; 8]);
        assert_eq!(
            events.time_bounds().0,
            Timestamp::from_nanos(1_700_000_000_000_000_700)
        );
        let attributes: Attributes = serde_json::from_str(&text(events, OVERFLOW, 0)).unwrap();
        assert_eq!(
            attributes["attr.exception.type"],
            Scalar::Text("IOError".into())
        );
        assert_eq!(
            attributes["resource.service.name"],
            Scalar::Text("checkout".into()),
            "events carry the resource"
        );
    }

    #[test]
    fn links_are_kept_as_json() {
        let mut with_link = span(1, None);
        with_link.links.push(Link {
            trace_id: vec![0xab; 16],
            span_id: vec![0x01; 8],
            attributes: vec![kv("k", "v")],
            ..Default::default()
        });
        let batches = decode(&request(vec![with_link]));
        let links: Value = serde_json::from_str(&text(&batches[0], LINKS, 0)).unwrap();
        assert_eq!(
            links,
            serde_json::json!([{
                "trace_id": "ab".repeat(16),
                "span_id": "0101010101010101",
                "attributes": {"k": "v"}
            }])
        );
    }

    #[test]
    fn ids_are_stable_across_retries() {
        let first = decode(&request(vec![span(1, None)]));
        let second = decode(&request(vec![span(1, None)]));
        assert_eq!(
            bytes(&first[0], EVENT_ID, 0),
            bytes(&second[0], EVENT_ID, 0)
        );
        let other = decode(&request(vec![span(2, None)]));
        assert_ne!(bytes(&first[0], EVENT_ID, 0), bytes(&other[0], EVENT_ID, 0));
    }

    #[test]
    fn rejects_malformed_ids_and_accepts_empty_requests() {
        let mut short = span(1, None);
        short.trace_id = vec![1; 3];
        let error = to_batches(
            &request(vec![short]),
            TenantId::DEFAULT,
            DatasetName::new("prod").unwrap(),
            Timestamp::from_nanos(5),
        )
        .unwrap_err();
        assert!(matches!(error, OtlpError::Invalid(_)), "{error}");
        assert!(decode(&ExportTraceServiceRequest::default()).is_empty());
    }

    #[test]
    fn a_span_without_status_is_unset_and_an_inverted_span_has_no_duration() {
        let mut bare = span(1, None);
        bare.status = None;
        bare.end_time_unix_nano = bare.start_time_unix_nano - 1;
        let batches = decode(&request(vec![bare]));
        assert_eq!(text(&batches[0], STATUS_CODE, 0), "unset");
        assert!(
            batches[0]
                .rows()
                .column_by_name(STATUS_MESSAGE)
                .unwrap()
                .is_null(0)
        );
        let duration = batches[0].rows().column_by_name(DURATION_NS).unwrap();
        let duration = duration.as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(duration.value(0), 0);
    }

    #[test]
    fn parses_both_encodings() {
        let request = request(vec![span(1, None)]);
        let from_protobuf = parse_protobuf(&request.encode_to_vec()).unwrap();
        assert_eq!(from_protobuf, request);
        let from_json = parse_json(&serde_json::to_vec(&request).unwrap()).unwrap();
        assert_eq!(from_json, request);
        assert!(parse_json(b"{").is_err());
    }
}
