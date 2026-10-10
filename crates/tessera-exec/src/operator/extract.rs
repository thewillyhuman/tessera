// SPDX-License-Identifier: LicenseRef-Tessera-Pending

//! `Extract`: derive labels from a text column into `_extracted`, and
//! `Format`: rewrite the line and the labels of each row.

use std::sync::Arc;

use arrow::array::{Array, ArrayRef, RecordBatch, StringArray};
use arrow::datatypes::SchemaRef;
use tessera_ir::{Expr, ExtractStage};
use tessera_kernels::{evaluate, extract, row_labels, template};
use tessera_types::event::{BODY, EXTRACTED, OVERFLOW, SEVERITY_TEXT};

use crate::ExecError;
use crate::batch::Batch;
use crate::operator::{Flow, Operator, Sink};

/// Parses a text expression and merges the labels found into
/// `_extracted`; `unpack` also replaces the body.
#[derive(Debug)]
pub struct Extract {
    source: Expr,
    stage: ExtractStage,
    schema: SchemaRef,
}

impl Extract {
    /// An extraction producing `schema`, the input's schema plus
    /// `_extracted`.
    #[must_use]
    pub fn new(source: Expr, stage: ExtractStage, schema: SchemaRef) -> Self {
        Extract {
            source,
            stage,
            schema,
        }
    }
}

impl Operator for Extract {
    fn push(&mut self, input: Batch, out: &mut dyn Sink) -> Result<Flow, ExecError> {
        let rows = input.materialize()?;
        let source = evaluate(&self.source, &rows)?;
        let text = source
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| ExecError::Unsupported(format!("{} is not text", self.source)))?;
        let existing = rows
            .column_by_name(EXTRACTED)
            .and_then(|c| c.as_any().downcast_ref::<StringArray>());
        let extracted = extract(&self.stage, text, existing)?;
        let mut columns: Vec<ArrayRef> = Vec::with_capacity(self.schema.fields().len());
        for field in self.schema.fields() {
            let column: ArrayRef =
                match field.name().as_str() {
                    EXTRACTED => Arc::new(extracted.labels.clone()),
                    BODY if extracted.line.is_some() => match &extracted.line {
                        Some(line) => Arc::new(line.clone()),
                        None => Arc::clone(rows.column_by_name(BODY).ok_or_else(|| {
                            ExecError::Unsupported("input lacks the body".into())
                        })?),
                    },
                    name => Arc::clone(rows.column_by_name(name).ok_or_else(|| {
                        ExecError::Unsupported(format!("input lacks column {name}"))
                    })?),
                };
            columns.push(column);
        }
        out.push(Batch::new(RecordBatch::try_new(
            Arc::clone(&self.schema),
            columns,
        )?))
    }

    fn finish(&mut self, _: &mut dyn Sink) -> Result<(), ExecError> {
        Ok(())
    }
}

/// Rewrites lines and labels from templates, drops and keeps labels.
#[derive(Debug)]
pub struct Format {
    line: Option<String>,
    labels: Vec<(String, String)>,
    drop: Vec<String>,
    keep: Option<Vec<String>>,
    schema: SchemaRef,
}

impl Format {
    /// A format step producing `schema`.
    #[must_use]
    pub fn new(
        line: Option<String>,
        labels: Vec<(String, String)>,
        drop: Vec<String>,
        keep: Option<Vec<String>>,
        schema: SchemaRef,
    ) -> Self {
        Format {
            line,
            labels,
            drop,
            keep,
            schema,
        }
    }

    fn kept(&self, name: &str) -> bool {
        !self.drop.iter().any(|d| d == name)
            && self
                .keep
                .as_ref()
                .is_none_or(|k| k.iter().any(|x| x == name))
    }
}

fn text_column<'a>(rows: &'a RecordBatch, name: &str) -> Option<&'a StringArray> {
    rows.column_by_name(name)
        .and_then(|c| c.as_any().downcast_ref::<StringArray>())
}

fn cell(column: Option<&StringArray>, i: usize) -> Option<&str> {
    column.filter(|c| !c.is_null(i)).map(|c| c.value(i))
}

impl Operator for Format {
    fn push(&mut self, input: Batch, out: &mut dyn Sink) -> Result<Flow, ExecError> {
        let rows = input.materialize()?;
        let body = text_column(&rows, BODY);
        let overflow = text_column(&rows, OVERFLOW);
        let extracted = text_column(&rows, EXTRACTED);
        let severity = text_column(&rows, SEVERITY_TEXT);
        let mut new_lines: Vec<Option<String>> = Vec::with_capacity(rows.num_rows());
        let mut new_extracted: Vec<Option<String>> = Vec::with_capacity(rows.num_rows());
        let mut new_overflow: Vec<Option<String>> = Vec::with_capacity(rows.num_rows());
        for i in 0..rows.num_rows() {
            let labels = row_labels(cell(overflow, i), cell(extracted, i), cell(severity, i));
            let line = cell(body, i).unwrap_or("");
            let lookup = |name: &str| labels.get(name).cloned();
            new_lines.push(match &self.line {
                Some(template) => Some(template::render(template, lookup, line)),
                None => cell(body, i).map(str::to_owned),
            });
            // Derived labels: the existing ones plus the formatted ones,
            // minus what is dropped or not kept.
            let mut derived: serde_json::Map<String, serde_json::Value> = cell(extracted, i)
                .and_then(|e| serde_json::from_str(e).ok())
                .unwrap_or_default();
            for (name, template) in &self.labels {
                let value = template::render(template, lookup, line);
                derived.insert(name.clone(), serde_json::Value::String(value));
            }
            derived.retain(|name, _| self.kept(name));
            new_extracted.push(if derived.is_empty() {
                None
            } else {
                Some(serde_json::Value::Object(derived).to_string())
            });
            // Stored attributes lose dropped labels too, so that the
            // stream a row belongs to reflects the query.
            let attributes: Option<serde_json::Map<String, serde_json::Value>> =
                cell(overflow, i).and_then(|o| serde_json::from_str(o).ok());
            new_overflow.push(attributes.map(|mut object| {
                object.retain(|path, _| self.kept(&tessera_types::label_name(path)));
                serde_json::Value::Object(object).to_string()
            }));
        }
        let mut columns: Vec<ArrayRef> = Vec::with_capacity(self.schema.fields().len());
        for field in self.schema.fields() {
            let column: ArrayRef =
                match field.name().as_str() {
                    BODY => Arc::new(StringArray::from(new_lines.clone())),
                    EXTRACTED => Arc::new(StringArray::from(new_extracted.clone())),
                    OVERFLOW => Arc::new(StringArray::from(new_overflow.clone())),
                    name => Arc::clone(rows.column_by_name(name).ok_or_else(|| {
                        ExecError::Unsupported(format!("input lacks column {name}"))
                    })?),
                };
            columns.push(column);
        }
        out.push(Batch::new(RecordBatch::try_new(
            Arc::clone(&self.schema),
            columns,
        )?))
    }

    fn finish(&mut self, _: &mut dyn Sink) -> Result<(), ExecError> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use arrow::datatypes::{DataType, Field, Schema};
    use tessera_ir::Expr;

    use super::*;
    use crate::operator::testing::run;

    fn rows(bodies: &[&str], overflow: &[Option<&str>]) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new(BODY, DataType::Utf8, true),
            Field::new(OVERFLOW, DataType::Utf8, true),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(bodies.to_vec())),
                Arc::new(StringArray::from(overflow.to_vec())),
            ],
        )
        .unwrap()
    }

    fn with_extracted(schema: &SchemaRef) -> SchemaRef {
        let mut fields: Vec<Field> = schema.fields().iter().map(|f| f.as_ref().clone()).collect();
        fields.push(Field::new(EXTRACTED, DataType::Utf8, true));
        Arc::new(Schema::new(fields))
    }

    #[test]
    fn extracts_then_formats_lines_and_labels() {
        let input = rows(
            &[r#"{"status":500,"path":"/a"}"#, "plain"],
            &[Some(r#"{"attr.app":"web"}"#), Some(r#"{"attr.app":"web"}"#)],
        );
        let schema = with_extracted(&input.schema());
        let mut extract = Extract::new(Expr::column(BODY), ExtractStage::Json, Arc::clone(&schema));
        let extracted = run(&mut extract, vec![input]);
        let labels = text_column(&extracted[0], EXTRACTED).unwrap();
        assert_eq!(labels.value(0), r#"{"path":"/a","status":"500"}"#);
        assert!(labels.is_null(1));

        let mut format = Format::new(
            Some("{{.app}} {{.status}}: {{__line__}}".into()),
            vec![("code".into(), "s{{.status}}".into())],
            vec!["path".into(), "app".into()],
            None,
            Arc::clone(&schema),
        );
        let formatted = run(&mut format, extracted);
        let body = text_column(&formatted[0], BODY).unwrap();
        assert_eq!(body.value(0), r#"web 500: {"status":500,"path":"/a"}"#);
        assert_eq!(body.value(1), "web : plain");
        let labels = text_column(&formatted[0], EXTRACTED).unwrap();
        assert_eq!(labels.value(0), r#"{"code":"s500","status":"500"}"#);
        assert_eq!(labels.value(1), r#"{"code":"s"}"#);
        let attributes = text_column(&formatted[0], OVERFLOW).unwrap();
        assert_eq!(
            attributes.value(0),
            "{}",
            "the dropped app label leaves the attributes"
        );

        let mut keep = Format::new(None, vec![], vec![], Some(vec!["status".into()]), schema);
        let kept = run(&mut keep, formatted);
        assert_eq!(
            text_column(&kept[0], EXTRACTED).unwrap().value(0),
            r#"{"status":"500"}"#
        );
    }
}
