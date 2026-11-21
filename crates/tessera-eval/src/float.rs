// SPDX-License-Identifier: LicenseRef-Tessera-Pending

//! The floats of the rule state, as JSON.
//!
//! JSON has no NaN and no infinity: `serde_json` writes them as
//! `null`, which does not read back as a number, so one such value
//! made a state log undecodable. An alert's value may well be one, as
//! in Prometheus, and so may a sum over samples that are. A float of
//! the state is therefore written as a number when it is finite and as
//! `"NaN"`, `"+Inf"` or `"-Inf"` when it is not, and read from either.
//! A `null`, as written before, reads as NaN.
//!
//! Use on a field with `#[serde(with = "crate::float")]`.

use serde::{Deserialize, Deserializer, Serializer, de};

const NAN: &str = "NaN";
const INFINITY: &str = "+Inf";
const NEG_INFINITY: &str = "-Inf";

/// Writes `value` as a number, or by name when it is not finite.
///
/// # Errors
///
/// If the serializer fails.
// The signature is the one `serde(with)` calls.
#[allow(clippy::trivially_copy_pass_by_ref)]
pub(crate) fn serialize<S: Serializer>(value: &f64, serializer: S) -> Result<S::Ok, S::Error> {
    if value.is_finite() {
        serializer.serialize_f64(*value)
    } else if value.is_nan() {
        serializer.serialize_str(NAN)
    } else if value.is_sign_positive() {
        serializer.serialize_str(INFINITY)
    } else {
        serializer.serialize_str(NEG_INFINITY)
    }
}

/// Reads a number, the name of a value that is not finite, or the
/// `null` such a value was once written as.
///
/// # Errors
///
/// If what is read is none of them.
pub(crate) fn deserialize<'de, D: Deserializer<'de>>(deserializer: D) -> Result<f64, D::Error> {
    #[derive(Deserialize)]
    #[serde(untagged)]
    enum Written {
        Number(f64),
        Name(String),
    }
    match Option::<Written>::deserialize(deserializer)? {
        None => Ok(f64::NAN),
        Some(Written::Number(value)) => Ok(value),
        Some(Written::Name(name)) => match name.as_str() {
            NAN => Ok(f64::NAN),
            INFINITY => Ok(f64::INFINITY),
            NEG_INFINITY => Ok(f64::NEG_INFINITY),
            other => Err(de::Error::custom(format!("{other:?} is not a float"))),
        },
    }
}

#[cfg(test)]
mod tests {
    use serde::{Deserialize, Serialize};

    #[derive(Serialize, Deserialize)]
    struct Held {
        #[serde(with = "super")]
        value: f64,
    }

    fn written(value: f64) -> String {
        serde_json::to_string(&Held { value }).unwrap()
    }

    fn read(json: &str) -> f64 {
        serde_json::from_str::<Held>(json).unwrap().value
    }

    #[test]
    fn a_finite_float_is_a_json_number() {
        assert_eq!(written(1.5), r#"{"value":1.5}"#);
        assert!((read(r#"{"value":1.5}"#) - 1.5).abs() < f64::EPSILON);
        assert!((read(r#"{"value":3}"#) - 3.0).abs() < f64::EPSILON);
    }

    #[test]
    fn a_float_that_is_not_finite_reads_back_as_it_was() {
        assert_eq!(written(f64::NAN), r#"{"value":"NaN"}"#);
        assert!(read(&written(f64::NAN)).is_nan());
        for infinite in [f64::INFINITY, f64::NEG_INFINITY] {
            assert!(read(&written(infinite)).to_bits() == infinite.to_bits());
        }
    }

    #[test]
    fn a_null_written_before_reads_as_nan() {
        assert!(read(r#"{"value":null}"#).is_nan());
        assert!(serde_json::from_str::<Held>(r#"{"value":"high"}"#).is_err());
    }
}
