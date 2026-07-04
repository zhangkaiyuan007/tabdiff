use arrow::datatypes::Schema;
use serde::Serialize;

#[derive(Debug, Serialize)]
pub struct ColumnDesc {
    pub name: String,
    pub data_type: String,
}

#[derive(Debug, Serialize)]
pub struct TypeChange {
    pub name: String,
    pub left: String,
    pub right: String,
}

#[derive(Debug, Serialize)]
pub struct RenamedColumn {
    pub left: String,
    pub right: String,
    /// Content similarity that backed the claim (match rate or Jaccard).
    pub similarity: f64,
}

#[derive(Debug, Serialize)]
pub struct SchemaDiff {
    pub added: Vec<ColumnDesc>,
    pub removed: Vec<ColumnDesc>,
    pub type_changed: Vec<TypeChange>,
    pub renamed: Vec<RenamedColumn>,
    /// Columns present on both sides, in left-schema order.
    #[serde(skip)]
    pub mutual: Vec<String>,
}

impl SchemaDiff {
    pub fn is_empty(&self) -> bool {
        self.added.is_empty()
            && self.removed.is_empty()
            && self.type_changed.is_empty()
            && self.renamed.is_empty()
    }
}

pub fn diff_schemas(left: &Schema, right: &Schema) -> SchemaDiff {
    let mut diff = SchemaDiff {
        added: vec![],
        removed: vec![],
        type_changed: vec![],
        renamed: vec![],
        mutual: vec![],
    };
    for f in left.fields() {
        match right.field_with_name(f.name()) {
            Ok(rf) => {
                diff.mutual.push(f.name().clone());
                if rf.data_type() != f.data_type() {
                    diff.type_changed.push(TypeChange {
                        name: f.name().clone(),
                        left: f.data_type().to_string(),
                        right: rf.data_type().to_string(),
                    });
                }
            }
            Err(_) => diff.removed.push(ColumnDesc {
                name: f.name().clone(),
                data_type: f.data_type().to_string(),
            }),
        }
    }
    for f in right.fields() {
        if left.field_with_name(f.name()).is_err() {
            diff.added.push(ColumnDesc {
                name: f.name().clone(),
                data_type: f.data_type().to_string(),
            });
        }
    }
    diff
}
