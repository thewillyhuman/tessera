// SPDX-License-Identifier: LicenseRef-Tessera-Pending

//! Stage outputs between workers (QE-14): a partial stage keeps its
//! output on the node that ran it, split by a hash of the group keys
//! into as many partitions as the final stage has workers, and each
//! worker fetches its partition from every producer. The coordinator
//! thus holds one partition of the groups rather than every partial
//! output, and the final work is spread over the workers.
//!
//! A kept partition is taken once, and a kept output is forgotten
//! after [`TTL`] in any case, so a coordinator that never comes back
//! does not pin it.

use std::collections::HashMap;
use std::sync::{Arc, Mutex, PoisonError};
use std::time::{Duration, Instant};

use arrow::array::{ArrayRef, BooleanArray, RecordBatch};
use arrow::compute::filter_record_batch;
use arrow::datatypes::SchemaRef;
use tessera_kernels::buckets_of;

use crate::ExecError;
use crate::remote::Exchange;

/// How long a kept output waits to be fetched.
pub(crate) const TTL: Duration = Duration::from_secs(300);

/// The outputs the partial stages of this node kept for exchanges.
#[derive(Default)]
pub(crate) struct Exchanges {
    kept: Mutex<HashMap<String, Kept>>,
}

struct Kept {
    since: Instant,
    schema: SchemaRef,
    parts: Vec<Option<Vec<RecordBatch>>>,
}

impl Exchanges {
    /// Keeps `rows` under `exchange`, split into its partitions.
    ///
    /// # Errors
    ///
    /// If a key is not a column of the rows.
    pub(crate) fn keep(
        &self,
        exchange: &Exchange,
        schema: SchemaRef,
        rows: Vec<RecordBatch>,
    ) -> Result<(), ExecError> {
        let parts = partition(rows, &exchange.keys, exchange.partitions)?;
        let mut kept = self.kept.lock().unwrap_or_else(PoisonError::into_inner);
        kept.retain(|_, k| k.since.elapsed() < TTL);
        kept.insert(
            exchange.id.clone(),
            Kept {
                since: Instant::now(),
                schema,
                parts: parts.into_iter().map(Some).collect(),
            },
        );
        Ok(())
    }

    /// The rows kept under `id` for `partition`, with their schema,
    /// once: a second take finds nothing.
    pub(crate) fn take(&self, id: &str, partition: u32) -> Option<(SchemaRef, Vec<RecordBatch>)> {
        let mut kept = self.kept.lock().unwrap_or_else(PoisonError::into_inner);
        kept.retain(|_, k| k.since.elapsed() < TTL);
        let entry = kept.get_mut(id)?;
        let rows = entry
            .parts
            .get_mut(usize::try_from(partition).ok()?)?
            .take()?;
        let schema = Arc::clone(&entry.schema);
        if entry.parts.iter().all(Option::is_none) {
            kept.remove(id);
        }
        Some((schema, rows))
    }
}

/// `rows` split into `partitions` parts by a hash of the `keys`
/// columns, the same on every node for equal keys; everything in the
/// first part when there are no keys.
///
/// # Errors
///
/// If a key is not a column of the rows.
pub(crate) fn partition(
    rows: Vec<RecordBatch>,
    keys: &[String],
    partitions: u32,
) -> Result<Vec<Vec<RecordBatch>>, ExecError> {
    let count = usize::try_from(partitions).unwrap_or(1).max(1);
    let mut out: Vec<Vec<RecordBatch>> = (0..count).map(|_| Vec::new()).collect();
    if count == 1 || keys.is_empty() {
        out[0] = rows;
        return Ok(out);
    }
    for batch in rows {
        let columns: Vec<ArrayRef> = keys
            .iter()
            .map(|key| {
                batch.column_by_name(key).cloned().ok_or_else(|| {
                    ExecError::Unsupported(format!(
                        "exchange key {key} is not a column of the stage's output"
                    ))
                })
            })
            .collect::<Result<_, _>>()?;
        let buckets = buckets_of(&columns, partitions)?;
        for (index, part) in out.iter_mut().enumerate() {
            let mask: BooleanArray = buckets
                .iter()
                .map(|b| Some(usize::try_from(*b).unwrap_or(usize::MAX) == index))
                .collect();
            let selected = filter_record_batch(&batch, &mask)?;
            if selected.num_rows() > 0 {
                part.push(selected);
            }
        }
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use arrow::array::{Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};

    use super::*;

    fn rows(keys: &[&str], values: &[i64]) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("k", DataType::Utf8, false),
            Field::new("v", DataType::Int64, false),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(keys.to_vec())),
                Arc::new(Int64Array::from(values.to_vec())),
            ],
        )
        .unwrap()
    }

    fn key_values(parts: &[Vec<RecordBatch>]) -> Vec<Vec<(String, i64)>> {
        parts
            .iter()
            .map(|part| {
                let mut pairs: Vec<(String, i64)> = part
                    .iter()
                    .flat_map(|b| {
                        let k = b.column(0).as_any().downcast_ref::<StringArray>().unwrap();
                        let v = b.column(1).as_any().downcast_ref::<Int64Array>().unwrap();
                        (0..b.num_rows())
                            .map(|i| (k.value(i).to_owned(), v.value(i)))
                            .collect::<Vec<_>>()
                    })
                    .collect();
                pairs.sort();
                pairs
            })
            .collect()
    }

    /// QE-14: equal keys land in the same partition whatever batch
    /// they came in, every row lands somewhere, and the split is the
    /// same again.
    #[test]
    fn partitions_follow_the_keys() {
        let keys = vec!["k".to_owned()];
        let batches = vec![
            rows(&["a", "b", "c", "a"], &[1, 2, 3, 4]),
            rows(&["b", "c", "d"], &[5, 6, 7]),
        ];
        let parts = partition(batches.clone(), &keys, 4).unwrap();
        assert_eq!(parts.len(), 4);
        let values = key_values(&parts);
        let total: usize = values.iter().map(Vec::len).sum();
        assert_eq!(total, 7);
        for part in &values {
            let keys: std::collections::BTreeSet<&String> = part.iter().map(|(k, _)| k).collect();
            for key in keys {
                let elsewhere = values
                    .iter()
                    .filter(|other| !std::ptr::eq(*other, part))
                    .any(|other| other.iter().any(|(k, _)| k == key));
                assert!(!elsewhere, "key {key} is split across partitions");
            }
        }
        assert_eq!(
            key_values(&partition(batches.clone(), &keys, 4).unwrap()),
            values
        );
        let whole = partition(batches.clone(), &[], 4).unwrap();
        assert_eq!(key_values(&whole)[0].len(), 7, "no keys, one partition");
        assert!(whole[1..].iter().all(Vec::is_empty));
        assert!(matches!(
            partition(batches, &["missing".to_owned()], 2),
            Err(ExecError::Unsupported(_))
        ));
    }

    #[test]
    fn kept_partitions_are_taken_once() {
        let exchanges = Exchanges::default();
        let exchange = Exchange {
            id: "x".into(),
            partitions: 2,
            keys: vec!["k".into()],
        };
        let batch = rows(&["a", "b", "c", "d"], &[1, 2, 3, 4]);
        exchanges
            .keep(&exchange, batch.schema(), vec![batch])
            .unwrap();
        let first = exchanges.take("x", 0).expect("partition 0");
        let second = exchanges.take("x", 1).expect("partition 1");
        let total: usize = first
            .1
            .iter()
            .chain(&second.1)
            .map(RecordBatch::num_rows)
            .sum();
        assert_eq!(total, 4);
        assert_eq!(first.0.fields().len(), 2);
        assert!(exchanges.take("x", 0).is_none(), "taken once");
        assert!(exchanges.take("y", 0).is_none());
        assert!(exchanges.take("x", 5).is_none());
    }
}
