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
    schema.with_column("division".into(), DataType::UInt32);

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
    println!("{:?}", results);
    // let results = get_specialties(&df)?;
    // println!("{:?}", results);
    // let results = get_subjets(&df)?;
    // println!("{:?}", results);
    // let results = get_teachers(&df)?;
    // println!("{:?}", results);
    // let results = get_divisions(&df)?;
    // println!("{:?}", results);
    // for i in 0..results.height() {
    //     let division = results.column("division")?.u32()?.get(i).unwrap();
    //     let name = results.column("nombre")?.list()?.get(i).unwrap();
    // }

    Ok(())
}

#[allow(dead_code)]
fn get_students(df: &LazyFrame) -> PolarsResult<DataFrame> {
    df.clone()
        .lazy()
        // .limit(10)
        .select(&[
            col("registro"),
            col("nombre_completo").alias("nombre"),
            col("tipo"),
            col("estado"),
            col("semestre"),
            col("grupo"),
            col("turno"),
            col("nivel"),
            col("especialidad"),
            col("nombre").alias("nombre_especialidad"),
        ])
        .group_by([col("registro")])
        .agg([
            col("nombre").unique().first(),
            col("tipo").unique().first(),
            col("estado").unique().first(),
            col("semestre").unique().first(),
            col("grupo").unique().first(),
            col("turno").unique().first(),
            col("nivel").unique().first(),
            col("especialidad").unique().first(),
            col("nombre_especialidad").unique().first(),
        ])
        .sort(["registro"], Default::default())
        .collect()
}

#[allow(dead_code)]
fn get_specialties(df: &LazyFrame) -> PolarsResult<DataFrame> {
    df.clone()
        .lazy()
        .select(&[col("especialidad"), col("nombre")])
        .group_by([col("especialidad")])
        .agg([
            col("nombre").unique(),
            // col("nombre").unique().len().alias("cantidad"),
        ])
        .sort(["especialidad"], Default::default())
        // .filter(col("cantidad").gt(lit(1)))
        .explode(["nombre"])
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
            // col("nombre").unique().len().alias("cantidad"),
        ])
        // .filter(col("cantidad").gt(lit(1)))
        .sort(["clave"], Default::default())
        // .explode(["nombre"])
        .collect()
}

#[allow(dead_code)]
fn get_divisions(df: &LazyFrame) -> PolarsResult<DataFrame> {
    df.clone()
        .lazy()
        .select(&[col("division"), col("academia")])
        .group_by([col("division")])
        .agg([col("academia").unique()])
        .explode(["academia"])
        .sort(["division"], Default::default())
        .collect()
}
#[allow(dead_code)]
fn get_teachers(df: &LazyFrame) -> PolarsResult<DataFrame> {
    df.clone()
        .lazy()
        .select(&[col("nomina"), col("nombre_duplicated_1").alias("nombre")])
        .group_by([col("nomina")])
        .agg([col("nombre").unique().first()])
        // .agg([col("nombre").unique().len()])
        // .filter(col("nombre").gt(lit(1)))
        .sort(["nomina"], Default::default())
        .collect()
}
