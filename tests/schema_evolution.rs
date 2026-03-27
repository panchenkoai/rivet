use rivet::state::{SchemaColumn, StateStore};

fn store() -> StateStore {
    StateStore::open_in_memory().unwrap()
}

fn cols(pairs: &[(&str, &str)]) -> Vec<SchemaColumn> {
    pairs.iter().map(|(n, t)| SchemaColumn {
        name: n.to_string(),
        data_type: t.to_string(),
    }).collect()
}

// ─── Basic lifecycle ─────────────────────────────────────────

#[test]
fn first_run_stores_schema_silently() {
    let s = store();
    let schema = cols(&[("id", "Int64"), ("name", "Utf8"), ("created_at", "Timestamp(Microsecond, None)")]);
    let change = s.detect_schema_change("orders", &schema).unwrap();
    assert!(change.is_none(), "first run should not report changes");

    let stored = s.get_stored_schema("orders").unwrap().unwrap();
    assert_eq!(stored.len(), 3);
    assert_eq!(stored[0].name, "id");
    assert_eq!(stored[0].data_type, "Int64");
}

#[test]
fn second_run_same_schema_no_change() {
    let s = store();
    let schema = cols(&[("id", "Int64"), ("name", "Utf8")]);
    s.detect_schema_change("t", &schema).unwrap();

    let change = s.detect_schema_change("t", &schema).unwrap();
    assert!(change.is_none(), "identical schema should not report changes");
}

// ─── Column additions ────────────────────────────────────────

#[test]
fn detect_single_column_added() {
    let s = store();
    let v1 = cols(&[("id", "Int64"), ("name", "Utf8")]);
    s.detect_schema_change("t", &v1).unwrap();

    let v2 = cols(&[("id", "Int64"), ("name", "Utf8"), ("email", "Utf8")]);
    let change = s.detect_schema_change("t", &v2).unwrap().unwrap();

    assert_eq!(change.added.len(), 1);
    assert!(change.added[0].contains("email"), "should report email added: {:?}", change.added);
    assert!(change.removed.is_empty());
    assert!(change.type_changed.is_empty());
}

#[test]
fn detect_multiple_columns_added() {
    let s = store();
    let v1 = cols(&[("id", "Int64")]);
    s.detect_schema_change("t", &v1).unwrap();

    let v2 = cols(&[("id", "Int64"), ("name", "Utf8"), ("email", "Utf8"), ("age", "Int32")]);
    let change = s.detect_schema_change("t", &v2).unwrap().unwrap();

    assert_eq!(change.added.len(), 3);
}

// ─── Column removals ─────────────────────────────────────────

#[test]
fn detect_single_column_removed() {
    let s = store();
    let v1 = cols(&[("id", "Int64"), ("name", "Utf8"), ("legacy_field", "Utf8")]);
    s.detect_schema_change("t", &v1).unwrap();

    let v2 = cols(&[("id", "Int64"), ("name", "Utf8")]);
    let change = s.detect_schema_change("t", &v2).unwrap().unwrap();

    assert_eq!(change.removed, vec!["legacy_field"]);
    assert!(change.added.is_empty());
}

#[test]
fn detect_multiple_columns_removed() {
    let s = store();
    let v1 = cols(&[("id", "Int64"), ("a", "Utf8"), ("b", "Utf8"), ("c", "Utf8")]);
    s.detect_schema_change("t", &v1).unwrap();

    let v2 = cols(&[("id", "Int64")]);
    let change = s.detect_schema_change("t", &v2).unwrap().unwrap();

    assert_eq!(change.removed.len(), 3);
    assert!(change.removed.contains(&"a".to_string()));
    assert!(change.removed.contains(&"b".to_string()));
    assert!(change.removed.contains(&"c".to_string()));
}

// ─── Type changes ────────────────────────────────────────────

#[test]
fn detect_column_type_change() {
    let s = store();
    let v1 = cols(&[("id", "Int64"), ("price", "Float64")]);
    s.detect_schema_change("t", &v1).unwrap();

    let v2 = cols(&[("id", "Int64"), ("price", "Utf8")]);
    let change = s.detect_schema_change("t", &v2).unwrap().unwrap();

    assert_eq!(change.type_changed.len(), 1);
    assert_eq!(change.type_changed[0].0, "price");
    assert_eq!(change.type_changed[0].1, "Float64");
    assert_eq!(change.type_changed[0].2, "Utf8");
}

#[test]
fn detect_multiple_type_changes() {
    let s = store();
    let v1 = cols(&[("id", "Int32"), ("ts", "Utf8"), ("flag", "Int16")]);
    s.detect_schema_change("t", &v1).unwrap();

    let v2 = cols(&[("id", "Int64"), ("ts", "Timestamp(Microsecond, None)"), ("flag", "Boolean")]);
    let change = s.detect_schema_change("t", &v2).unwrap().unwrap();

    assert_eq!(change.type_changed.len(), 3);
}

// ─── Combined changes ────────────────────────────────────────

#[test]
fn detect_add_remove_and_type_change_simultaneously() {
    let s = store();
    let v1 = cols(&[("id", "Int64"), ("old_col", "Utf8"), ("price", "Float64")]);
    s.detect_schema_change("t", &v1).unwrap();

    let v2 = cols(&[("id", "Int64"), ("price", "Utf8"), ("new_col", "Boolean")]);
    let change = s.detect_schema_change("t", &v2).unwrap().unwrap();

    assert_eq!(change.added.len(), 1, "new_col added");
    assert!(change.added[0].contains("new_col"));
    assert_eq!(change.removed, vec!["old_col"], "old_col removed");
    assert_eq!(change.type_changed.len(), 1, "price type changed");
    assert_eq!(change.type_changed[0].0, "price");
}

// ─── Schema updates on change ────────────────────────────────

#[test]
fn schema_updated_after_change_detected() {
    let s = store();
    let v1 = cols(&[("id", "Int64")]);
    s.detect_schema_change("t", &v1).unwrap();

    let v2 = cols(&[("id", "Int64"), ("name", "Utf8")]);
    s.detect_schema_change("t", &v2).unwrap();

    // Third run with v2 should show no change (schema was updated)
    let change = s.detect_schema_change("t", &v2).unwrap();
    assert!(change.is_none(), "after update, same schema should show no change");
}

#[test]
fn three_sequential_evolutions() {
    let s = store();

    // v1: initial
    let v1 = cols(&[("id", "Int64"), ("name", "Utf8")]);
    assert!(s.detect_schema_change("t", &v1).unwrap().is_none());

    // v2: add email
    let v2 = cols(&[("id", "Int64"), ("name", "Utf8"), ("email", "Utf8")]);
    let c = s.detect_schema_change("t", &v2).unwrap().unwrap();
    assert_eq!(c.added.len(), 1);

    // v3: remove name, change id type
    let v3 = cols(&[("id", "Int32"), ("email", "Utf8")]);
    let c = s.detect_schema_change("t", &v3).unwrap().unwrap();
    assert_eq!(c.removed, vec!["name"]);
    assert_eq!(c.type_changed.len(), 1);
    assert_eq!(c.type_changed[0].0, "id");
}

// ─── Multiple exports independent ────────────────────────────

#[test]
fn different_exports_tracked_independently() {
    let s = store();

    let schema_a = cols(&[("id", "Int64"), ("name", "Utf8")]);
    let schema_b = cols(&[("id", "Int64"), ("price", "Float64")]);

    s.detect_schema_change("orders", &schema_a).unwrap();
    s.detect_schema_change("products", &schema_b).unwrap();

    // Change only orders
    let schema_a2 = cols(&[("id", "Int64"), ("name", "Utf8"), ("status", "Utf8")]);
    let change_a = s.detect_schema_change("orders", &schema_a2).unwrap();
    let change_b = s.detect_schema_change("products", &schema_b).unwrap();

    assert!(change_a.is_some(), "orders schema changed");
    assert!(change_b.is_none(), "products schema unchanged");
}

// ─── Edge cases ──────────────────────────────────────────────

#[test]
fn empty_schema_to_columns() {
    let s = store();
    let v1 = cols(&[]);
    s.detect_schema_change("t", &v1).unwrap();

    let v2 = cols(&[("id", "Int64")]);
    let c = s.detect_schema_change("t", &v2).unwrap().unwrap();
    assert_eq!(c.added.len(), 1);
}

#[test]
fn columns_to_empty_schema() {
    let s = store();
    let v1 = cols(&[("id", "Int64"), ("name", "Utf8")]);
    s.detect_schema_change("t", &v1).unwrap();

    let v2 = cols(&[]);
    let c = s.detect_schema_change("t", &v2).unwrap().unwrap();
    assert_eq!(c.removed.len(), 2);
}

#[test]
fn column_order_change_not_detected_as_change() {
    let s = store();
    let v1 = cols(&[("id", "Int64"), ("name", "Utf8")]);
    s.detect_schema_change("t", &v1).unwrap();

    let v2 = cols(&[("name", "Utf8"), ("id", "Int64")]);
    let change = s.detect_schema_change("t", &v2).unwrap();
    assert!(change.is_none(), "reordering columns should not be a schema change");
}

// ─── Realistic Arrow type names ──────────────────────────────

#[test]
fn realistic_pg_schema_evolution() {
    let s = store();
    let v1 = cols(&[
        ("id", "Int64"),
        ("name", "Utf8"),
        ("email", "Utf8"),
        ("age", "Int32"),
        ("balance", "Utf8"),
        ("is_active", "Boolean"),
        ("created_at", "Timestamp(Microsecond, None)"),
        ("updated_at", "Timestamp(Microsecond, None)"),
    ]);
    assert!(s.detect_schema_change("users", &v1).unwrap().is_none());

    // ALTER TABLE: add phone, drop age, change balance from text to float
    let v2 = cols(&[
        ("id", "Int64"),
        ("name", "Utf8"),
        ("email", "Utf8"),
        ("phone", "Utf8"),
        ("balance", "Float64"),
        ("is_active", "Boolean"),
        ("created_at", "Timestamp(Microsecond, None)"),
        ("updated_at", "Timestamp(Microsecond, None)"),
    ]);
    let c = s.detect_schema_change("users", &v2).unwrap().unwrap();

    assert!(c.added.iter().any(|a| a.contains("phone")), "phone should be added: {:?}", c.added);
    assert!(c.removed.contains(&"age".to_string()), "age should be removed: {:?}", c.removed);
    assert!(c.type_changed.iter().any(|(n, _, _)| n == "balance"), "balance type should change: {:?}", c.type_changed);
}
