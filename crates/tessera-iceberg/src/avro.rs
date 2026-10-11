// SPDX-License-Identifier: LicenseRef-Tessera-Pending

//! The Avro object container file the Iceberg manifests are written
//! as, with the binary encoding of the values they hold [1].
//!
//! Only what Iceberg metadata needs is here: the primitive types, the
//! record, the nullable union and the array, written with the `null`
//! codec and the schema in the header exactly as given, so that the
//! `field-id` attributes an Iceberg reader maps fields by survive. An
//! Avro library writes its own rendering of the schema and drops them.
//!
//! [1] <https://avro.apache.org/docs/1.12.0/specification/>

/// A value as the Avro binary encoding writes it. The encoding carries
/// no type tags, so a value must have the shape its field's schema
/// gives it.
#[derive(Clone, Debug, PartialEq)]
pub enum Value {
    /// The `null` type.
    Null,
    /// A `boolean`.
    Boolean(bool),
    /// An `int`.
    Int(i32),
    /// A `long`.
    Long(i64),
    /// A `string`.
    String(String),
    /// `bytes`.
    Bytes(Vec<u8>),
    /// A record's fields, in schema order.
    Record(Vec<Value>),
    /// A union's branch and the value of that branch.
    Union(u32, Box<Value>),
    /// An array's items.
    Array(Vec<Value>),
}

impl Value {
    /// A `["null", T]` union: the first branch for none, the second for
    /// a value.
    #[must_use]
    pub fn optional(value: Option<Value>) -> Value {
        match value {
            None => Value::Union(0, Box::new(Value::Null)),
            Some(value) => Value::Union(1, Box::new(value)),
        }
    }
}

const MAGIC: &[u8; 4] = b"Obj\x01";

/// The marker after the header and after every block. A file is
/// written whole and read whole, so one fixed marker serves: a reader
/// never has to find it by scanning.
const SYNC: [u8; 16] = *b"tessera.iceberg.";

/// An object container file: `schema`, the Avro schema as JSON text,
/// and `metadata` in the header, `records` in one block.
///
/// # Panics
///
/// If a length does not fit in 63 bits, which no object does.
#[must_use]
pub fn file(schema: &str, metadata: &[(&str, &[u8])], records: &[Value]) -> Vec<u8> {
    let mut out = Vec::new();
    out.extend_from_slice(MAGIC);
    let mut header = vec![
        ("avro.schema", schema.as_bytes()),
        ("avro.codec", b"null".as_slice()),
    ];
    header.extend(metadata.iter().copied());
    long(&mut out, length(header.len()));
    for (key, value) in header {
        bytes(&mut out, key.as_bytes());
        bytes(&mut out, value);
    }
    long(&mut out, 0);
    out.extend_from_slice(&SYNC);
    if !records.is_empty() {
        let mut block = Vec::new();
        for record in records {
            encode(record, &mut block);
        }
        long(&mut out, length(records.len()));
        long(&mut out, length(block.len()));
        out.extend_from_slice(&block);
        out.extend_from_slice(&SYNC);
    }
    out
}

fn encode(value: &Value, out: &mut Vec<u8>) {
    match value {
        Value::Null => {}
        Value::Boolean(b) => out.push(u8::from(*b)),
        Value::Int(i) => long(out, i64::from(*i)),
        Value::Long(l) => long(out, *l),
        Value::String(s) => bytes(out, s.as_bytes()),
        Value::Bytes(b) => bytes(out, b),
        Value::Record(fields) => {
            for field in fields {
                encode(field, out);
            }
        }
        Value::Union(branch, value) => {
            long(out, i64::from(*branch));
            encode(value, out);
        }
        Value::Array(items) => {
            if !items.is_empty() {
                long(out, length(items.len()));
                for item in items {
                    encode(item, out);
                }
            }
            long(out, 0);
        }
    }
}

/// A zig-zag varint, the encoding of `int` and `long`.
#[allow(
    clippy::cast_sign_loss,
    clippy::cast_possible_truncation,
    reason = "zig-zag maps the sign into the low bit and the bytes are masked to seven bits"
)]
fn long(out: &mut Vec<u8>, value: i64) {
    let mut zigzag = ((value << 1) ^ (value >> 63)) as u64;
    loop {
        let byte = (zigzag & 0x7f) as u8;
        zigzag >>= 7;
        if zigzag == 0 {
            out.push(byte);
            return;
        }
        out.push(byte | 0x80);
    }
}

fn bytes(out: &mut Vec<u8>, value: &[u8]) {
    long(out, length(value.len()));
    out.extend_from_slice(value);
}

fn length(len: usize) -> i64 {
    i64::try_from(len).unwrap_or_else(|_| unreachable!("a length fits in 63 bits"))
}

#[cfg(test)]
mod tests {
    use apache_avro::Reader;
    use apache_avro::types::Value as Read;

    use super::*;

    const SCHEMA: &str = r#"{"type":"record","name":"row","fields":[
        {"name":"a","type":"long","field-id":1},
        {"name":"b","type":["null","string"],"default":null,"field-id":2},
        {"name":"c","type":{"type":"array","items":"int","element-id":3},"field-id":4},
        {"name":"d","type":"bytes","field-id":5},
        {"name":"e","type":"boolean","field-id":6},
        {"name":"f","type":{"type":"record","name":"inner","fields":[{"name":"x","type":"int","field-id":7}]},"field-id":8}
    ]}"#;

    fn row(a: i64, b: Option<&str>, c: Vec<i32>) -> Value {
        Value::Record(vec![
            Value::Long(a),
            Value::optional(b.map(|b| Value::String(b.to_owned()))),
            Value::Array(c.into_iter().map(Value::Int).collect()),
            Value::Bytes(vec![1, 2, 3]),
            Value::Boolean(true),
            Value::Record(vec![Value::Int(-7)]),
        ])
    }

    #[test]
    fn a_file_reads_back_with_an_avro_library() {
        let bytes = file(
            SCHEMA,
            &[("format-version", b"3")],
            &[row(1, Some("one"), vec![1, 2]), row(-2, None, vec![])],
        );
        let reader = Reader::new(&bytes[..]).unwrap();
        assert_eq!(
            reader.user_metadata().get("format-version"),
            Some(&b"3".to_vec())
        );
        let rows: Vec<Read> = reader.map(Result::unwrap).collect();
        assert_eq!(rows.len(), 2);
        let Read::Record(fields) = &rows[0] else {
            panic!("{rows:?}");
        };
        assert_eq!(fields[0], ("a".to_owned(), Read::Long(1)));
        assert_eq!(
            fields[1],
            (
                "b".to_owned(),
                Read::Union(1, Box::new(Read::String("one".to_owned())))
            )
        );
        assert_eq!(
            fields[2],
            (
                "c".to_owned(),
                Read::Array(vec![Read::Int(1), Read::Int(2)])
            )
        );
        assert_eq!(fields[3], ("d".to_owned(), Read::Bytes(vec![1, 2, 3])));
        assert_eq!(fields[4], ("e".to_owned(), Read::Boolean(true)));
        assert_eq!(
            fields[5],
            (
                "f".to_owned(),
                Read::Record(vec![("x".to_owned(), Read::Int(-7))])
            )
        );
        let Read::Record(fields) = &rows[1] else {
            panic!("{rows:?}");
        };
        assert_eq!(fields[0].1, Read::Long(-2));
        assert_eq!(fields[1].1, Read::Union(0, Box::new(Read::Null)));
        assert_eq!(fields[2].1, Read::Array(vec![]));
    }

    #[test]
    fn the_schema_is_written_as_given_so_field_ids_survive() {
        let bytes = file(SCHEMA, &[], &[]);
        let text = String::from_utf8_lossy(&bytes);
        assert!(text.contains(r#""field-id":1"#), "{text}");
        assert!(text.contains(r#""element-id":3"#), "{text}");
        let reader = Reader::new(&bytes[..]).unwrap();
        assert_eq!(reader.count(), 0, "no block follows an empty file's header");
    }

    #[test]
    fn integers_are_zigzag_varints() {
        let encoded = |value: i64| {
            let mut out = Vec::new();
            long(&mut out, value);
            out
        };
        assert_eq!(encoded(0), [0x00]);
        assert_eq!(encoded(-1), [0x01]);
        assert_eq!(encoded(1), [0x02]);
        assert_eq!(encoded(-64), [0x7f]);
        assert_eq!(encoded(64), [0x80, 0x01]);
        assert_eq!(
            encoded(i64::MIN),
            [0xff; 9].iter().copied().chain([0x01]).collect::<Vec<_>>()
        );
    }
}
