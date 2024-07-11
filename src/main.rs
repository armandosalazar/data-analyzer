
use polars::prelude::*;

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

fn main() {
    let path = "/Users/armando/Downloads/Base de datos original 080724.csv";

    // let df = CsvReadOptions::default()
    //     .try_into_reader_with_file_path(Some(path.into()))
    //     .unwrap()
    //     .finish()
    //     .unwrap();

    let schema = Schema::new();

    let df: LazyFrame = LazyCsvReader::new(path).finish()?;

    // let users = df
    //     .clone()
    //     .lazy()
    //     .group_by([col("registro")])
    //     .agg([
    //          col("nombre_completo").first(),
    //          col("tipo").first(),
    //          col("estado").first(),
    //          col("semestre").first(),
    //          col("grupo").first(),
    //          col("turno").first(),
    //          change_level(col("nivel").first()).alias("nivel"),
    //     ])
    //     .sort(
    //         ["registro"],
    //         SortMultipleOptions::default()
    //         )
    //     .limit(5)
    //     .collect()
    //     .unwrap();

    // println!("{:?}", users);

    // let out = df
    //     .clone()
    //     .lazy()
    //     .filter(col("registro").eq(lit(21110110)))
    //     .group_by([col("registro")])
    //     .agg([
    //          len().alias("count"),
    //          col("nombre_completo").first(),
    //          col("especialidad").unique().first(),
    //          col("clave").unique().alias("claves"),
    //     ])
    //     .sort(
    //         ["registro"],
    //         SortMultipleOptions::default()
    //         .with_order_descending(false)
    //         .with_nulls_last(true),
    //         )
    //     .limit(10)
    //     .collect()
    //     .unwrap();

    // println!("{:?}", out);

    // let subjects = df
    //     .clone()
    //     .lazy()
    //     .group_by([col("clave")])
    //     .agg([
    //         col("nombre").unique().first()
    //     ]
    //     ).collect()
    //     .unwrap();

    // print!("{:?}", subjects);

    // let divisions = df
    //     .clone()
    //     .lazy()
    //     .group_by(["academia"])
    //     .agg([
    //          col("division").unique()
    //     ]).collect()
    //     .unwrap();

    // println!("{}", divisions);

    // let teachers = df
    //     .clone()
    //     .lazy()
    //     .group_by(["nomina"])
    //     .agg([
    //          col("nombre").unique()
    //     ])
    //     .sort(
    //         ["nomina"],
    //         SortMultipleOptions::default()
    //         )
    //     .collect()
    //     .unwrap();

    // print!("{}", teachers)
}
