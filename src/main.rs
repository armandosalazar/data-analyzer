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

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let path: &str = "/Users/armando/Downloads/Base de datos.csv";

    let mut schema: Schema = Schema::new();
    schema.with_column("registro".into(), DataType::UInt32);
    schema.with_column("especialidad".into(), DataType::UInt32);

    let df: LazyFrame = LazyCsvReader::new(path)
        .with_dtype_overwrite(Some(Arc::new(schema)))
        .finish()?;

    let results = get_subjets(&df)?;

    println!("{:?}", results.get_columns());
    // println!("{:?}", results);

    Ok(())
}

#[allow(dead_code)]
fn get_students(df: &LazyFrame) -> PolarsResult<DataFrame> {
    df.clone()
        .lazy()
        .limit(10)
        .select(&[col("registro"), col("nombre_completo")])
        .group_by([col("registro")])
        .agg([len().alias("cantidad"), col("nombre_completo").first()])
        .sort(["registro"], Default::default())
        .collect()
}

#[allow(dead_code)]
fn get_specialties(df: &LazyFrame) -> PolarsResult<DataFrame> {
    df.clone()
        .lazy()
        .select(&[col("especialidad"), col("nombre")])
        .group_by([col("especialidad")])
        .agg([col("nombre").unique()])
        .sort(["especialidad"], Default::default())
        .collect()
}

fn get_subjets(df: &LazyFrame) -> PolarsResult<DataFrame> {
    df.clone()
        .lazy()
        //.limit(5)
        .select(&[col("clave"), col("nombre_duplicated_0")])
        .group_by([col("clave")])
        .agg([
            col("nombre_duplicated_0").unique().alias("nombre"),
            col("nombre_duplicated_0").unique().len().alias("cantidad"),
        ])
        .filter(col("cantidad").gt(lit(1)))
        .collect()
}

#[allow(dead_code)]
fn get_teachers(df: &LazyFrame) -> PolarsResult<DataFrame> {
    df.clone().lazy().select(&[]).collect()
}
