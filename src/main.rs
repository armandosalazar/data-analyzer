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

#[allow(unused_variables)]
fn main() -> Result<(), Box<dyn std::error::Error>> {
    let path: &str = "/Users/armando/Downloads/Base de datos.csv";

    let mut schema: Schema = Schema::new();
    schema.with_column("registro".into(), DataType::UInt32);
    schema.with_column("especialidad".into(), DataType::UInt32);

    let df: LazyFrame = LazyCsvReader::new(path)
        .with_dtype_overwrite(Some(Arc::new(schema)))
        .finish()?;

    // let results = get_subjets(&df)?;
    // let columns = results.get_columns();

    // for i in 0..columns[0].len() {
    //     println!(
    //         "{:?} {:?}",
    //         columns[0].get(i)?.to_string().replace("\"", ""),
    //         columns[1].get(i)?.to_string().replace("\"", "")
    //     );
    // }

    let results = get_students(&df)?;
    // let results = get_specialties(&df)?;
    // let results = get_subjets(&df)?;
    println!("{:?}", results);

    Ok(())
}

#[allow(dead_code)]
fn get_students(df: &LazyFrame) -> PolarsResult<DataFrame> {
    df.clone()
        .lazy()
        // .limit(10)
        .select(&[
            col("registro"),
            col("nombre_completo"),
            col("tipo"),
            col("estado"),
            col("semestre"),
            col("grupo"),
            col("turno"),
            col("nivel"),
            col("nombre").alias("nombre_especialidad"),
            col("clave"),
            col("nombre_duplicated_0").alias("nombre_materia"),
            col("estatus_materia"),
        ])
        .group_by([col("registro"), col("clave")])
        .agg([
            // len().alias("cantidad"),
            // col("nombre_completo").unique().first(),
            // col("tipo").unique().first(),
            // col("estado").unique().first(),
            // col("semestre").unique().first(),
            // col("grupo").unique().first(),
            // col("turno").unique().first(),
            // col("nivel").unique().first(),
            // col("nombre_especialidad").unique().first(),
            // col("clave"),
            col("nombre_materia").unique().first(),
            col("estatus_materia").unique().first(),
        ])
        .sort(["registro"], Default::default())
        // .filter(col("clave").neq(col("nombre_materia")))
        // .filter(col("estatus_materia").neq(lit(1)))
        // .filter(col("nombre_materia").neq(lit(1)))
        .collect()
}

#[allow(dead_code)]
fn get_specialties(df: &LazyFrame) -> PolarsResult<DataFrame> {
    df.clone()
        .lazy()
        .select(&[
            col("especialidad"),
            col("nombre").alias("nombre_especialidad"),
        ])
        .group_by([col("especialidad")])
        .agg([col("nombre_especialidad").unique()])
        .sort(["especialidad"], Default::default())
        .collect()
}

#[allow(dead_code)]
fn get_subjets(df: &LazyFrame) -> PolarsResult<DataFrame> {
    df.clone()
        .lazy()
        // .limit(5)
        .select(&[col("clave"), col("nombre_duplicated_0").alias("nombre")])
        .group_by([col("clave")])
        .agg([
            col("nombre").unique().first(),
            // col("nombre_duplicated_0").unique().len().alias("cantidad"),
        ])
        // .filter(col("cantidad").gt(lit(1)))
        .collect()
}

#[allow(dead_code)]
fn get_teachers(df: &LazyFrame) -> PolarsResult<DataFrame> {
    df.clone().lazy().select(&[]).collect()
}
