// SPDX-License-Identifier: LicenseRef-Tessera-Pending

//! OTLP logs (IN-1): decoding an export request and normalizing it to an
//! event batch (IN-5). Traces are in [`traces`] and metrics in
//! [`metrics`], sharing the attribute normalization below.
//!
//! Resource, scope and record attributes are flattened to dotted paths
//! under `resource.`, `scope.` and `attr.`; nested maps flatten into the
//! path, lists and bytes become JSON text. A string body is the `body`
//! column; any other body becomes its JSON text. A record's event id is
//! the client-supplied `log.record.uid` when present, otherwise random
//! (section 7.5).

use opentelemetry_proto::tonic::collector::logs::v1::ExportLogsServiceRequest;
use opentelemetry_proto::tonic::common::v1::{AnyValue, KeyValue, any_value};
use prost::Message;
use tessera_types::event::{Attributes, Scalar};
use tessera_types::{
    DatasetName, EventBatch, Id128, InvalidBatch, LogRow, Random, TenantId, Timestamp,
};

pub mod metrics;
pub mod traces;

/// The record attribute that carries a client-supplied identity.
const RECORD_UID: &str = "log.record.uid";

/// Why a request could not be decoded.
#[derive(Debug, thiserror::Error)]
pub enum OtlpError {
    /// The body is not a protobuf export request.
    #[error("malformed OTLP protobuf: {0}")]
    Protobuf(#[from] prost::DecodeError),
    /// The body is not a JSON export request.
    #[error("malformed OTLP JSON: {0}")]
    Json(#[from] serde_json::Error),
    /// The records could not be turned into a batch.
    #[error(transparent)]
    Batch(#[from] InvalidBatch),
    /// A record violates the protocol.
    #[error("invalid OTLP request: {0}")]
    Invalid(String),
}

/// Parses the protobuf encoding of an export request.
///
/// # Errors
///
/// `Protobuf` if the bytes are not a request.
pub fn parse_protobuf(bytes: &[u8]) -> Result<ExportLogsServiceRequest, OtlpError> {
    Ok(ExportLogsServiceRequest::decode(bytes)?)
}

/// Parses the JSON encoding of an export request.
///
/// # Errors
///
/// `Json` if the bytes are not a request.
pub fn parse_json(bytes: &[u8]) -> Result<ExportLogsServiceRequest, OtlpError> {
    Ok(serde_json::from_slice(bytes)?)
}

/// Normalizes every log record of `request` into one batch for the given
/// tenant and dataset, or `None` if the request has no records.
///
/// # Errors
///
/// `Batch` if the rows cannot be encoded.
pub fn to_batch(
    request: &ExportLogsServiceRequest,
    tenant: TenantId,
    dataset: DatasetName,
    now: Timestamp,
    random: &mut impl Random,
) -> Result<Option<EventBatch>, OtlpError> {
    let mut rows = Vec::new();
    for resource_logs in &request.resource_logs {
        let mut resource = Attributes::new();
        if let Some(r) = &resource_logs.resource {
            flatten_all("resource", &r.attributes, &mut resource);
        }
        for scope_logs in &resource_logs.scope_logs {
            let mut base = resource.clone();
            if let Some(scope) = &scope_logs.scope {
                if !scope.name.is_empty() {
                    base.insert("scope.name".into(), Scalar::Text(scope.name.clone()));
                }
                if !scope.version.is_empty() {
                    base.insert("scope.version".into(), Scalar::Text(scope.version.clone()));
                }
                flatten_all("scope", &scope.attributes, &mut base);
            }
            for record in &scope_logs.log_records {
                let mut attributes = base.clone();
                flatten_all("attr", &record.attributes, &mut attributes);
                let event_id = match attributes.get(&format!("attr.{RECORD_UID}")) {
                    Some(Scalar::Text(uid)) => {
                        Id128::from_u128(u128::from_le_bytes(hash16(uid.as_bytes())))
                    }
                    _ => Id128::random(random),
                };
                let timestamp = if record.time_unix_nano == 0 {
                    if record.observed_time_unix_nano == 0 {
                        now
                    } else {
                        nanos(record.observed_time_unix_nano)
                    }
                } else {
                    nanos(record.time_unix_nano)
                };
                rows.push(LogRow {
                    timestamp,
                    observed_timestamp: now,
                    event_id: event_id.as_u128().to_le_bytes(),
                    trace_id: record.trace_id.as_slice().try_into().ok(),
                    span_id: record.span_id.as_slice().try_into().ok(),
                    severity_number: (record.severity_number != 0)
                        .then_some(record.severity_number),
                    severity_text: (!record.severity_text.is_empty())
                        .then(|| record.severity_text.clone()),
                    body: record.body.as_ref().and_then(body_text),
                    attributes,
                });
            }
        }
    }
    if rows.is_empty() {
        return Ok(None);
    }
    Ok(Some(EventBatch::from_logs(tenant, dataset, &rows)?))
}

pub(crate) fn nanos(value: u64) -> Timestamp {
    Timestamp::from_nanos(i64::try_from(value).unwrap_or(i64::MAX))
}

/// A stable 16-byte digest of an identity supplied by the client.
pub(crate) fn hash16(bytes: &[u8]) -> [u8; 16] {
    let a = tessera_types::stable_hash(bytes);
    let b = tessera_types::stable_hash(&a.to_le_bytes());
    let mut out = [0u8; 16];
    out[..8].copy_from_slice(&a.to_le_bytes());
    out[8..].copy_from_slice(&b.to_le_bytes());
    out
}

fn body_text(body: &AnyValue) -> Option<String> {
    match &body.value {
        Some(any_value::Value::StringValue(text)) => Some(text.clone()),
        // Interned strings need the request's string table, which this
        // release does not support; the value is treated as absent.
        Some(any_value::Value::StringValueStrindex(_)) | None => None,
        Some(other) => Some(json(other).to_string()),
    }
}

pub(crate) fn flatten_all(prefix: &str, attributes: &[KeyValue], into: &mut Attributes) {
    for attribute in attributes {
        if let Some(value) = &attribute.value {
            flatten(&format!("{prefix}.{}", attribute.key), value, into);
        }
    }
}

fn flatten(path: &str, value: &AnyValue, into: &mut Attributes) {
    match &value.value {
        None | Some(any_value::Value::StringValueStrindex(_)) => {}
        Some(any_value::Value::StringValue(text)) => {
            into.insert(path.to_owned(), Scalar::Text(text.clone()));
        }
        Some(any_value::Value::BoolValue(flag)) => {
            into.insert(path.to_owned(), Scalar::Bool(*flag));
        }
        Some(any_value::Value::IntValue(number)) => {
            into.insert(path.to_owned(), Scalar::Int(*number));
        }
        Some(any_value::Value::DoubleValue(number)) => {
            into.insert(path.to_owned(), Scalar::Float(*number));
        }
        Some(any_value::Value::KvlistValue(list)) => flatten_all(path, &list.values, into),
        Some(other @ (any_value::Value::ArrayValue(_) | any_value::Value::BytesValue(_))) => {
            into.insert(path.to_owned(), Scalar::Text(json(other).to_string()));
        }
    }
}

/// A JSON rendering of a non-scalar value.
pub(crate) fn json(value: &any_value::Value) -> serde_json::Value {
    match value {
        any_value::Value::StringValue(text) => serde_json::Value::String(text.clone()),
        any_value::Value::BoolValue(flag) => serde_json::Value::Bool(*flag),
        any_value::Value::IntValue(number) => serde_json::Value::from(*number),
        any_value::Value::DoubleValue(number) => serde_json::Value::from(*number),
        any_value::Value::ArrayValue(array) => serde_json::Value::Array(
            array
                .values
                .iter()
                .filter_map(|v| v.value.as_ref())
                .map(json)
                .collect(),
        ),
        any_value::Value::KvlistValue(list) => serde_json::Value::Object(
            list.values
                .iter()
                .filter_map(|kv| {
                    kv.value
                        .as_ref()
                        .and_then(|v| v.value.as_ref())
                        .map(|v| (kv.key.clone(), json(v)))
                })
                .collect(),
        ),
        any_value::Value::BytesValue(bytes) => serde_json::Value::String(hex(bytes)),
        any_value::Value::StringValueStrindex(_) => serde_json::Value::Null,
    }
}

/// Lowercase hex of `bytes`.
pub(crate) fn hex(bytes: &[u8]) -> String {
    bytes.iter().fold(String::new(), |mut hex, b| {
        use std::fmt::Write;
        let _ = write!(hex, "{b:02x}");
        hex
    })
}

#[cfg(test)]
mod tests {
    use arrow::array::{Array, FixedSizeBinaryArray, Int32Array, StringArray};
    use opentelemetry_proto::tonic::common::v1::{ArrayValue, InstrumentationScope, KeyValueList};
    use opentelemetry_proto::tonic::logs::v1::{LogRecord, ResourceLogs, ScopeLogs};
    use opentelemetry_proto::tonic::resource::v1::Resource;
    use tessera_types::SeededRandom;
    use tessera_types::event::{BODY, EVENT_ID, OVERFLOW, SEVERITY_NUMBER, TRACE_ID};

    use super::*;

    fn kv(key: &str, value: any_value::Value) -> KeyValue {
        KeyValue {
            key: key.into(),
            value: Some(AnyValue { value: Some(value) }),
            ..Default::default()
        }
    }

    fn text(value: &str) -> any_value::Value {
        any_value::Value::StringValue(value.into())
    }

    fn request(records: Vec<LogRecord>) -> ExportLogsServiceRequest {
        ExportLogsServiceRequest {
            resource_logs: vec![ResourceLogs {
                resource: Some(Resource {
                    attributes: vec![kv("service.name", text("checkout"))],
                    ..Default::default()
                }),
                scope_logs: vec![ScopeLogs {
                    scope: Some(InstrumentationScope {
                        name: "app".into(),
                        ..Default::default()
                    }),
                    log_records: records,
                    schema_url: String::new(),
                }],
                schema_url: String::new(),
            }],
        }
    }

    fn record(body: &str) -> LogRecord {
        LogRecord {
            time_unix_nano: 1_700_000_000_000_000_000,
            severity_number: 17,
            severity_text: "ERROR".into(),
            body: Some(AnyValue {
                value: Some(text(body)),
            }),
            attributes: vec![
                kv("http.status_code", any_value::Value::IntValue(500)),
                kv(
                    "nested",
                    any_value::Value::KvlistValue(KeyValueList {
                        values: vec![kv("inner", any_value::Value::BoolValue(true))],
                    }),
                ),
                kv(
                    "tags",
                    any_value::Value::ArrayValue(ArrayValue {
                        values: vec![AnyValue {
                            value: Some(text("a")),
                        }],
                    }),
                ),
            ],
            trace_id: vec![7; 16],
            span_id: vec![8; 8],
            ..Default::default()
        }
    }

    fn decode_with(
        request: &ExportLogsServiceRequest,
        random: &mut impl Random,
    ) -> Option<EventBatch> {
        to_batch(
            request,
            TenantId::DEFAULT,
            DatasetName::new("prod").unwrap(),
            Timestamp::from_nanos(5),
            random,
        )
        .unwrap()
    }

    fn decode(request: &ExportLogsServiceRequest) -> Option<EventBatch> {
        decode_with(request, &mut SeededRandom::new(1))
    }

    #[test]
    fn normalizes_records_with_flattened_attributes() {
        let batch = decode(&request(vec![record("boom")])).unwrap();
        assert_eq!(batch.len(), 1);
        let rows = batch.rows();
        let body = rows
            .column_by_name(BODY)
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(body.value(0), "boom");
        let severity = rows
            .column_by_name(SEVERITY_NUMBER)
            .unwrap()
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        assert_eq!(severity.value(0), 17);
        let trace = rows
            .column_by_name(TRACE_ID)
            .unwrap()
            .as_any()
            .downcast_ref::<FixedSizeBinaryArray>()
            .unwrap();
        assert_eq!(trace.value(0), [7; 16]);
        let overflow = rows
            .column_by_name(OVERFLOW)
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let attributes: Attributes = serde_json::from_str(overflow.value(0)).unwrap();
        assert_eq!(
            attributes["resource.service.name"],
            Scalar::Text("checkout".into())
        );
        assert_eq!(attributes["scope.name"], Scalar::Text("app".into()));
        assert_eq!(attributes["attr.http.status_code"], Scalar::Int(500));
        assert_eq!(attributes["attr.nested.inner"], Scalar::Bool(true));
        assert_eq!(attributes["attr.tags"], Scalar::Text(r#"["a"]"#.into()));
        assert_eq!(
            batch.time_bounds().0,
            Timestamp::from_nanos(1_700_000_000_000_000_000)
        );
        assert_eq!(
            batch.observed_bounds().0,
            Timestamp::from_nanos(5),
            "observed time is ingest time"
        );
    }

    #[test]
    fn a_client_supplied_uid_gives_a_stable_event_id() {
        let mut with_uid = record("x");
        with_uid.attributes.push(kv(RECORD_UID, text("abc")));
        let first = decode(&request(vec![with_uid.clone()])).unwrap();
        let second = decode(&request(vec![with_uid])).unwrap();
        let id = |b: &EventBatch| {
            b.rows()
                .column_by_name(EVENT_ID)
                .unwrap()
                .as_any()
                .downcast_ref::<FixedSizeBinaryArray>()
                .unwrap()
                .value(0)
                .to_vec()
        };
        assert_eq!(id(&first), id(&second));
        let mut random = SeededRandom::new(2);
        let random_a = decode_with(&request(vec![record("x")]), &mut random).unwrap();
        let random_b = decode_with(&request(vec![record("x")]), &mut random).unwrap();
        assert_ne!(
            id(&random_a),
            id(&random_b),
            "without a uid, ids are random"
        );
    }

    #[test]
    fn a_missing_time_falls_back_to_observed_then_now() {
        let mut no_time = record("x");
        no_time.time_unix_nano = 0;
        no_time.observed_time_unix_nano = 42;
        assert_eq!(
            decode(&request(vec![no_time.clone()]))
                .unwrap()
                .time_bounds()
                .0,
            Timestamp::from_nanos(42)
        );
        no_time.observed_time_unix_nano = 0;
        assert_eq!(
            decode(&request(vec![no_time])).unwrap().time_bounds().0,
            Timestamp::from_nanos(5)
        );
    }

    #[test]
    fn a_structured_body_becomes_json_text() {
        let mut structured = record("");
        structured.body = Some(AnyValue {
            value: Some(any_value::Value::KvlistValue(KeyValueList {
                values: vec![kv("k", text("v"))],
            })),
        });
        let batch = decode(&request(vec![structured])).unwrap();
        let body = batch
            .rows()
            .column_by_name(BODY)
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(body.value(0), r#"{"k":"v"}"#);
    }

    #[test]
    fn an_empty_request_has_no_batch() {
        assert!(decode(&request(Vec::new())).is_none());
        assert!(decode(&ExportLogsServiceRequest::default()).is_none());
    }

    #[test]
    fn protobuf_and_json_round_trip() {
        let original = request(vec![record("x")]);
        let bytes = original.encode_to_vec();
        assert_eq!(parse_protobuf(&bytes).unwrap(), original);
        let json = serde_json::to_vec(&original).unwrap();
        assert_eq!(parse_json(&json).unwrap(), original);
        assert!(matches!(
            parse_protobuf(b"\xff\xff"),
            Err(OtlpError::Protobuf(_))
        ));
        assert!(matches!(parse_json(b"{"), Err(OtlpError::Json(_))));
    }
}
