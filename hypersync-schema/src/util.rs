use std::collections::BTreeSet;

use polars_arrow::datatypes::ArrowSchema as Schema;

pub fn project_schema(schema: &Schema, field_selection: &BTreeSet<String>) -> Schema {
    let mut select_indices = Vec::new();
    for col_name in field_selection.iter() {
        if let Some((idx, _)) = schema
            .fields
            .iter()
            .enumerate()
            .find(|(_, f)| &f.name == col_name)
        {
            select_indices.push(idx);
        }
    }

    let schema: Schema = schema
        .fields
        .iter()
        .filter(|f| field_selection.contains(&f.name))
        .cloned()
        .collect::<Vec<_>>()
        .into();

    schema
}
