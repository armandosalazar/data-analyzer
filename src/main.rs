use polars::lazy::dsl::*;
use polars::prelude::*;

#[allow(dead_code)]
fn change_level(level: Expr) -> Expr {
    level.map(
        |s: Series| -> PolarsResult<Option<Series>> {
            let chunks: StringChunked = s.str()?.apply_generic(|value| match value? {
                "I" => Some("ING".to_string()),
                "T" => Some("TEC".to_string()),
                _ => None,
            });

            Ok(Some(chunks.into_series()))
        },
        GetOutput::default(),
    )
}

fn main() -> Result<(), Box<dyn std::error::Error>>{
    let path: &str = "/Users/armando/Downloads/Base de datos.csv";

    let mut schema: Schema = Schema::new();
    schema.with_column("registro".into(), DataType::UInt32);

    let df: LazyFrame = LazyCsvReader::new(path)
    .with_dtype_overwrite(Some(Arc::new(schema)))
    .finish()?;

    println!("{:?}", df.collect()?);

    Ok(())
}
