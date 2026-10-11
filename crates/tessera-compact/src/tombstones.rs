// SPDX-License-Identifier: LicenseRef-Tessera-Pending

//! Applying deletes by predicate at compaction (SE-5): the rows a
//! tombstone's predicate matches, observed before the delete, are left
//! out of the output file, so that a file written under the tombstone's
//! epoch or later is clean and the tombstone can go once no older file
//! remains.

use arrow::array::{BooleanArray, RecordBatch};
use arrow::compute::filter_record_batch;
use tessera_ir::{BinaryOp, Expr};
use tessera_kernels::evaluate_predicate;
use tessera_manifest::Tombstone;
use tessera_types::event::OBSERVED_TIMESTAMP;

use crate::CompactError;

/// `rows` without the ones `tombstones` delete, and how many went.
///
/// # Errors
///
/// If a tombstone's predicate does not decode or cannot be evaluated.
pub fn remove_deleted(
    rows: &RecordBatch,
    tombstones: &[Tombstone],
) -> Result<(RecordBatch, u64), CompactError> {
    let mut rows = rows.clone();
    let mut removed = 0;
    for tombstone in tombstones {
        let predicate: Expr = serde_json::from_value(tombstone.predicate.clone()).map_err(|e| {
            CompactError::Tombstone(format!("{} does not decode: {e}", tombstone.id))
        })?;
        let deleted = predicate.and(
            Expr::column(OBSERVED_TIMESTAMP)
                .binary(BinaryOp::LtEq, Expr::literal(tombstone.created_at)),
        );
        let matches: BooleanArray = evaluate_predicate(&deleted, &rows)?;
        let keep = BooleanArray::from_unary(&matches, |m| !m);
        removed += u64::try_from(matches.true_count()).unwrap_or(0);
        rows = filter_record_batch(&rows, &keep)?;
    }
    Ok((rows, removed))
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use tessera_types::event::Scalar;
    use tessera_types::{DatasetName, EventBatch, LogRow, TenantId, Timestamp};

    use super::*;

    #[test]
    fn rows_matching_a_tombstone_and_observed_before_it_are_removed() {
        let rows: Vec<LogRow> = (0..6u8)
            .map(|i| LogRow {
                timestamp: Timestamp::from_nanos(i64::from(i)),
                observed_timestamp: Timestamp::from_nanos(i64::from(i) * 10),
                event_id: [i; 16],
                body: Some("x".into()),
                attributes: BTreeMap::from([(
                    "attr.user".to_owned(),
                    Scalar::Text(if i % 2 == 0 { "u1" } else { "u2" }.into()),
                )]),
                ..LogRow::default()
            })
            .collect();
        let batch = EventBatch::from_logs(TenantId::DEFAULT, DatasetName::new("p").unwrap(), &rows)
            .unwrap();
        let predicate = Expr::Path {
            path: "attr.user".into(),
            data_type: arrow::datatypes::DataType::Utf8,
        }
        .binary(BinaryOp::Eq, Expr::literal("u1"));
        let tombstone = Tombstone {
            id: "t".into(),
            predicate: serde_json::to_value(&predicate).unwrap(),
            text: "attr.user = 'u1'".into(),
            created_at: Timestamp::from_nanos(25),
            epoch: 1,
        };
        let (kept, removed) = remove_deleted(batch.rows(), &[tombstone]).unwrap();
        // u1 rows observed at 0 and 20 go; the one observed at 40 stays.
        assert_eq!(removed, 2);
        assert_eq!(kept.num_rows(), 4);
    }
}
