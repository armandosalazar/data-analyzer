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

fn change_grade(grade: Expr) -> Expr {
    grade.map(
        |s: Series| -> PolarsResult<Option<Series>> {
            let chunks: Float32Chunked = s.f32()?.apply(|value| match value? {
                value if value > 100.0 => Some(value / 100.0),
                value if value <= 100.0 => Some(value / 1.0),
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
    schema.with_column("registro".into(), DataType::Int32);
    schema.with_column("especialidad".into(), DataType::Int32);
    schema.with_column("division".into(), DataType::Int32);
    schema.with_column("nomina".into(), DataType::Int32);
    schema.with_column("calificacion3".into(), DataType::Int32);

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

    let grades = get_grades(&df)?;
    println!("{:?}", grades);
    // let results = get_students(&df)?;
    // println!("{:?}", results);
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
    //     let name = results.column("academia")?.str()?.get(i).unwrap();
    //     println!("{:?} {:?}", division, name);
    // }

    Ok(())
}

#[allow(dead_code)]
fn get_grades(df: &LazyFrame) -> PolarsResult<DataFrame> {
    if df.clone().schema()?.index_of("calificacion1").is_some() & df.clone().schema()?.index_of("calificacion2").is_some() & df.clone().schema()?.index_of("calificacion3").is_some() {
        let result = df.clone()
            .lazy()
            .select(
                &[
                    col("registro"),
                    col("nombre_completo").alias("nombre_alumno"),
                    col("clave"),
                    col("nombre_duplicated_0").alias("nombre_materia"),
                    col("estatus_materia"),
                    col("nomina").alias("nomina_maestro"),
                    col("calificacion1").alias("calificacion_primer_parcial"),
                    col("faltas1").alias("faltas_primer_parcial"),
                    col("ponderacion1").alias("ponderacion_primer_parcial"),
                    col("calificacion2").alias("calificacion_segundo_parcial"),
                    col("faltas2").alias("faltas_segundo_parcial"),
                    col("ponderacion2").alias("ponderacion_segundo_parcial"),
                    col("calificacion3").alias("calificacion_tercer_parcial"),
                    col("faltas3").alias("faltas_tercer_parcial"),
                    col("ponderacion3").alias("ponderacion_tercer_parcial"),
                ]
            )
            .group_by([col("registro"), col("clave")])
            .agg(
                [
                    col("nombre_alumno").unique().first(),
                    col("nombre_materia").unique().first(),
                    //col("estatus_materia").unique().first(),
                    //col("nomina_maestro").unique().first(),
                    //col("calificacion_primer_parcial").unique().first(),
                    //col("faltas_primer_parcial").unique().first(),
                    //col("ponderacion_primer_parcial").unique().first(),
                    //col("calificacion_segundo_parcial").unique().first(),
                    //col("faltas_segundo_parcial").unique().first(),
                    //col("ponderacion_segundo_parcial").unique().first(),
                    //col("calificacion_tercer_parcial").unique().first(),
                    //col("faltas_tercer_parcial").unique().first(),
                    //col("ponderacion_tercer_parcial").unique().first(),
                    change_grade(col("calificacion_primer_parcial")).unique().first().alias("OK"),
                ]
            )
            .filter(col("registro").eq(lit(21110110)))
            //.sort(["registro"], Default::default())
            .collect()?;

        return Ok(result);
    }
    if df.clone().schema()?.index_of("calificacion1").is_some() & df.clone().schema()?.index_of("calificacion2").is_some() {
        let result = df.clone()
            .lazy()
            .select(
                &[
                    col("registro"),
                    col("nombre_completo").alias("nombre_alumno"),
                    col("clave"),
                    col("nombre_duplicated_0").alias("nombre_materia"),
                    col("estatus_materia"),
                    col("nomina").alias("nomina_maestro"),
                    col("calificacion1").alias("calificacion_primer_parcial").cast(DataType::Float32),
                    col("faltas1").alias("faltas_primer_parcial"),
                    col("ponderacion1").alias("ponderacion_primer_parcial"),
                    col("calificacion2").alias("calificacion_segundo_parcial"),
                    col("faltas2").alias("faltas_segundo_parcial"),
                    col("ponderacion2").alias("ponderacion_segundo_parcial"),
                ]
            )
            .group_by([col("registro"), col("clave")])
            .agg(
                [
                    col("nombre_alumno").unique().first(),
                    col("nombre_materia").unique().first(),
                    col("estatus_materia").unique().first(),
                    col("nomina_maestro").unique().first(),
                    col("calificacion_primer_parcial").unique().first(),
                    col("faltas_primer_parcial").unique().first(),
                    col("ponderacion_primer_parcial").unique().first(),
                    col("calificacion_segundo_parcial").unique().first(),
                    col("faltas_segundo_parcial").unique().first(),
                    col("ponderacion_segundo_parcial").unique().first(),
                    change_grade(col("calificacion_primer_parcial")).unique().first().alias("OK"),
                ]
            )
            .filter(col("registro").eq(lit(21110110)))
            //.sort(["registro"], Default::default())
            .collect()?;

        return Ok(result);
    }
    if df.clone().schema()?.index_of("calificacion1").is_some() {
        let result = df.clone()
            .lazy()
            .select(
                &[
                    col("registro"),
                    col("nombre_completo").alias("nombre_alumno"),
                    col("clave"),
                    col("nombre_duplicated_0").alias("nombre_materia"),
                    col("estatus_materia"),
                    col("nomina").alias("nomina_maestro"),
                    col("calificacion1").alias("calificacion_primer_parcial"),
                    col("faltas1").alias("faltas_primer_parcial"),
                    col("ponderacion1").alias("ponderacion_primer_parcial"),
                ]
            )
            .group_by([col("registro"), col("clave")])
            .agg(
                [
                    col("nombre_alumno").unique().first(),
                    col("nombre_materia").unique().first(),
                    col("estatus_materia").unique().first(),
                    col("nomina_maestro").unique().first(),
                    col("calificacion_primer_parcial").unique().first(),
                    col("faltas_primer_parcial").unique().first(),
                    col("ponderacion_primer_parcial").unique().first(),
                ]
            )
            .filter(col("registro").eq(lit(21110110)))
            //.sort(["registro"], Default::default())
            .collect()?;

        return Ok(result);
    } else {
        let result = df.clone()
            .lazy()
            .select(
                &[
                    col("registro"),
                    col("nombre_completo").alias("nombre_alumno"),
                    col("clave"),
                    col("nombre_duplicated_0").alias("nombre_materia"),
                    col("estatus_materia"),
                    col("nomina").alias("nomina_maestro"),
                ]
            )
            .group_by([col("registro"), col("clave")])
            .agg(
                [
                    col("nombre_alumno").unique().first(),
                    col("nombre_materia").unique().first(),
                    col("estatus_materia").unique().first(),
                    col("nomina_maestro").unique().first(),
                ]
            )
            .filter(col("registro").eq(lit(21110110)))
            //.sort(["registro"], Default::default())
            .collect()?;


        return Ok(result);
    }
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
            // col("nombre").unique().first(),
            // col("tipo").unique().first(),
            // col("estado").unique().first(),
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
        .select(&[
            col("clave"),
            col("nombre_duplicated_0").alias("nombre"),
            col("division"),
            col("academia"),
            col("nomina"),
            col("nombre_duplicated_1").alias("nombre_maestro"),
        ])
        .group_by([col("clave"), col("nombre_maestro")])
        .agg([
            col("nombre").unique().first(),
            col("division").unique().first(),
            col("academia").unique().first(),
            // col("nombre").unique().len().alias("cantidad"),
            col("nomina").unique().first(),
        ])
        // .filter(col("cantidad").gt(lit(1)))
        .sort(["clave"], Default::default())
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
