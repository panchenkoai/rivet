use crate::error::Result;

use super::StateStore;

/// One column in a schema snapshot.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct SchemaColumn {
    pub name: String,
    #[serde(rename = "type")]
    pub data_type: String,
}

/// Diff between two schema snapshots.
#[derive(Debug)]
pub struct SchemaChange {
    pub added: Vec<String>,
    pub removed: Vec<String>,
    pub type_changed: Vec<(String, String, String)>, // (name, old_type, new_type)
}

impl SchemaChange {
    pub fn is_empty(&self) -> bool {
        self.added.is_empty() && self.removed.is_empty() && self.type_changed.is_empty()
    }
}

/// Schema history store — reads and writes `export_schema`.
///
/// Captures a schema snapshot per export on each run and surfaces structural
/// drift (added/removed/retyped columns) by diffing against the stored snapshot.
impl StateStore {
    pub fn get_stored_schema(&self, export_name: &str) -> Result<Option<Vec<SchemaColumn>>> {
        let mut stmt = self
            .conn
            .prepare("SELECT columns_json FROM export_schema WHERE export_name = ?1")?;
        let result = stmt.query_row([export_name], |row| {
            let json_str: String = row.get(0)?;
            Ok(json_str)
        });
        match result {
            Ok(json_str) => {
                let cols: Vec<SchemaColumn> = serde_json::from_str(&json_str)?;
                Ok(Some(cols))
            }
            Err(rusqlite::Error::QueryReturnedNoRows) => Ok(None),
            Err(e) => Err(e.into()),
        }
    }

    pub fn store_schema(&self, export_name: &str, columns: &[SchemaColumn]) -> Result<()> {
        let json = serde_json::to_string(columns)?;
        let now = chrono::Utc::now().to_rfc3339();
        self.conn.execute(
            "INSERT INTO export_schema (export_name, columns_json, updated_at)
             VALUES (?1, ?2, ?3)
             ON CONFLICT(export_name) DO UPDATE SET
                columns_json = excluded.columns_json,
                updated_at = excluded.updated_at",
            rusqlite::params![export_name, json, now],
        )?;
        Ok(())
    }

    pub fn detect_schema_change(
        &self,
        export_name: &str,
        current: &[SchemaColumn],
    ) -> Result<Option<SchemaChange>> {
        let stored = match self.get_stored_schema(export_name)? {
            Some(s) => s,
            None => {
                self.store_schema(export_name, current)?;
                return Ok(None);
            }
        };

        let stored_map: std::collections::HashMap<&str, &str> = stored
            .iter()
            .map(|c| (c.name.as_str(), c.data_type.as_str()))
            .collect();
        let current_map: std::collections::HashMap<&str, &str> = current
            .iter()
            .map(|c| (c.name.as_str(), c.data_type.as_str()))
            .collect();

        let added: Vec<String> = current
            .iter()
            .filter(|c| !stored_map.contains_key(c.name.as_str()))
            .map(|c| format!("{} ({})", c.name, c.data_type))
            .collect();

        let removed: Vec<String> = stored
            .iter()
            .filter(|c| !current_map.contains_key(c.name.as_str()))
            .map(|c| c.name.clone())
            .collect();

        let type_changed: Vec<(String, String, String)> = current
            .iter()
            .filter_map(|c| {
                stored_map.get(c.name.as_str()).and_then(|old_type| {
                    if *old_type != c.data_type.as_str() {
                        Some((c.name.clone(), old_type.to_string(), c.data_type.clone()))
                    } else {
                        None
                    }
                })
            })
            .collect();

        let change = SchemaChange {
            added,
            removed,
            type_changed,
        };

        if !change.is_empty() {
            self.store_schema(export_name, current)?;
            Ok(Some(change))
        } else {
            Ok(None)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn store() -> StateStore {
        StateStore::open_in_memory().expect("in-memory store")
    }

    #[test]
    fn first_schema_stored_no_change() {
        let s = store();
        let cols = vec![
            SchemaColumn {
                name: "id".into(),
                data_type: "Int64".into(),
            },
            SchemaColumn {
                name: "name".into(),
                data_type: "Utf8".into(),
            },
        ];
        let change = s.detect_schema_change("orders", &cols).unwrap();
        assert!(change.is_none(), "first run should detect no change");
        assert!(s.get_stored_schema("orders").unwrap().is_some());
    }

    #[test]
    fn same_schema_no_change() {
        let s = store();
        let cols = vec![SchemaColumn {
            name: "id".into(),
            data_type: "Int64".into(),
        }];
        s.detect_schema_change("t", &cols).unwrap();
        let change = s.detect_schema_change("t", &cols).unwrap();
        assert!(change.is_none());
    }

    #[test]
    fn added_column_detected() {
        let s = store();
        let v1 = vec![SchemaColumn {
            name: "id".into(),
            data_type: "Int64".into(),
        }];
        s.detect_schema_change("t", &v1).unwrap();

        let v2 = vec![
            SchemaColumn {
                name: "id".into(),
                data_type: "Int64".into(),
            },
            SchemaColumn {
                name: "email".into(),
                data_type: "Utf8".into(),
            },
        ];
        let change = s.detect_schema_change("t", &v2).unwrap().unwrap();
        assert_eq!(change.added.len(), 1);
        assert!(change.added[0].contains("email"));
    }

    #[test]
    fn removed_column_detected() {
        let s = store();
        let v1 = vec![
            SchemaColumn {
                name: "id".into(),
                data_type: "Int64".into(),
            },
            SchemaColumn {
                name: "old_field".into(),
                data_type: "Utf8".into(),
            },
        ];
        s.detect_schema_change("t", &v1).unwrap();

        let v2 = vec![SchemaColumn {
            name: "id".into(),
            data_type: "Int64".into(),
        }];
        let change = s.detect_schema_change("t", &v2).unwrap().unwrap();
        assert_eq!(change.removed, vec!["old_field"]);
    }

    #[test]
    fn type_change_detected() {
        let s = store();
        let v1 = vec![SchemaColumn {
            name: "price".into(),
            data_type: "Float64".into(),
        }];
        s.detect_schema_change("t", &v1).unwrap();

        let v2 = vec![SchemaColumn {
            name: "price".into(),
            data_type: "Utf8".into(),
        }];
        let change = s.detect_schema_change("t", &v2).unwrap().unwrap();
        assert_eq!(change.type_changed.len(), 1);
        assert_eq!(
            change.type_changed[0],
            ("price".into(), "Float64".into(), "Utf8".into())
        );
    }
}
