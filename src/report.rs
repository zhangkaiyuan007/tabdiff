use std::collections::BTreeMap;

use serde::Serialize;

use crate::schema_diff::SchemaDiff;

#[derive(Debug, Serialize)]
pub struct KeyVal {
    pub column: String,
    pub value: String,
}

#[derive(Debug, Serialize)]
pub struct KeyInfo {
    pub columns: Vec<String>,
    pub inferred: bool,
}

#[derive(Debug, Serialize)]
pub struct Change {
    pub column: String,
    pub left: String,
    pub right: String,
}

#[derive(Debug, Serialize)]
pub struct ModifiedRow {
    pub key: Vec<KeyVal>,
    pub changes: Vec<Change>,
}

#[derive(Debug, Serialize)]
pub struct RowCounts {
    pub left: usize,
    pub right: usize,
}

#[derive(Debug, Serialize)]
pub struct DiffCounts {
    pub added: usize,
    pub removed: usize,
    pub modified: usize,
}

#[derive(Debug, Serialize)]
pub struct Samples {
    pub added: Vec<Vec<KeyVal>>,
    pub removed: Vec<Vec<KeyVal>>,
    pub modified: Vec<ModifiedRow>,
}

#[derive(Debug, Serialize)]
pub struct DiffReport {
    pub schema: SchemaDiff,
    pub key: KeyInfo,
    pub rows: RowCounts,
    pub diff: DiffCounts,
    pub columns_changed: BTreeMap<String, usize>,
    pub samples: Samples,
    /// True when --fail-fast stopped the scan early; counts are lower bounds.
    pub truncated: bool,
}

impl DiffReport {
    pub fn has_differences(&self) -> bool {
        !self.schema.is_empty()
            || self.diff.added + self.diff.removed + self.diff.modified > 0
    }

    pub fn to_json(&self) -> String {
        serde_json::to_string_pretty(self).expect("report serializes")
    }

    pub fn render_human(&self, color: bool) -> String {
        let (g, r, y, b, z) = if color {
            ("\x1b[32m", "\x1b[31m", "\x1b[33m", "\x1b[1m", "\x1b[0m")
        } else {
            ("", "", "", "", "")
        };
        let mut o = String::new();

        o.push_str(&format!("{b}Schema{z}\n"));
        if self.schema.is_empty() {
            o.push_str("  identical\n");
        }
        for c in &self.schema.added {
            o.push_str(&format!("  {g}+ {} ({}){z}\n", c.name, c.data_type));
        }
        for c in &self.schema.removed {
            o.push_str(&format!("  {r}- {} ({}){z}\n", c.name, c.data_type));
        }
        for c in &self.schema.type_changed {
            o.push_str(&format!("  {y}~ {}: {} → {}{z}\n", c.name, c.left, c.right));
        }

        o.push_str(&format!(
            "{b}Key{z}: {}{}\n",
            self.key.columns.join(", "),
            if self.key.inferred { " (inferred)" } else { "" }
        ));

        o.push_str(&format!("{b}Rows{z}: {} → {}\n", self.rows.left, self.rows.right));
        let total = self.diff.added + self.diff.removed + self.diff.modified;
        if total == 0 {
            o.push_str("  no row differences\n");
        } else {
            o.push_str(&format!("  {g}+ {} added{z}\n", self.diff.added));
            o.push_str(&format!("  {r}- {} removed{z}\n", self.diff.removed));
            let cols = self
                .columns_changed
                .iter()
                .map(|(k, v)| format!("{k}: {v}"))
                .collect::<Vec<_>>()
                .join(", ");
            o.push_str(&format!(
                "  {y}~ {} modified{z}{}\n",
                self.diff.modified,
                if cols.is_empty() {
                    String::new()
                } else {
                    format!("  ({cols})")
                }
            ));
            o.push('\n');
            for m in &self.samples.modified {
                let ch = m
                    .changes
                    .iter()
                    .map(|c| format!("{}: {} → {}", c.column, c.left, c.right))
                    .collect::<Vec<_>>()
                    .join(", ");
                o.push_str(&format!("{y}~ {}{z}  {}\n", fmt_key(&m.key), ch));
            }
            for k in &self.samples.added {
                o.push_str(&format!("{g}+ {}{z}\n", fmt_key(k)));
            }
            for k in &self.samples.removed {
                o.push_str(&format!("{r}- {}{z}\n", fmt_key(k)));
            }
            let shown =
                self.samples.modified.len() + self.samples.added.len() + self.samples.removed.len();
            if total > shown {
                o.push_str(&format!(
                    "… {} more row difference(s) not shown (use --samples N)\n",
                    total - shown
                ));
            }
        }
        if self.truncated {
            o.push_str("(scan stopped early by --fail-fast; counts are lower bounds)\n");
        }
        if !self.has_differences() {
            o.push_str("\nNo differences found.\n");
        }
        o
    }
}

fn fmt_key(k: &[KeyVal]) -> String {
    k.iter()
        .map(|kv| format!("{}={}", kv.column, kv.value))
        .collect::<Vec<_>>()
        .join(", ")
}
