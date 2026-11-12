"""Profile the IMDb dumps, then write a cleaned Parquet copy.

Run from the repo root:

    python -m src.prepare
"""

import argparse
import csv
import os
import sys
import time

import pyspark.sql.functions as F
from pyspark.sql import SparkSession

from src.clean import (
    clean_akas,
    clean_basics,
    clean_crew,
    clean_episodes,
    clean_names,
    clean_principals,
    clean_ratings,
)
from src.load_imdb import TABLES, assert_samples, load_tables
from src.quality import find_issues

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def build_spark():
    tmp = os.path.join(ROOT, "data", "tmp")
    os.makedirs(os.path.join(tmp, "derby"), exist_ok=True)
    os.makedirs(os.path.join(tmp, "spark"), exist_ok=True)
    os.environ["PYSPARK_PYTHON"] = sys.executable
    os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable
    os.environ["SPARK_LOCAL_IP"] = "127.0.0.1"
    spark = (
        SparkSession.builder.appName("imdb-data-preparation")
        .master("local[*]")
        .config("spark.driver.memory", "16g")
        .config("spark.driver.maxResultSize", "2g")
        .config("spark.sql.shuffle.partitions", "64")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.autoBroadcastJoinThreshold", "-1")
        .config("spark.ui.enabled", "false")
        .config("spark.local.dir", os.path.join(tmp, "spark"))
        .config("spark.sql.warehouse.dir", os.path.join(tmp, "warehouse"))
        .config(
            "spark.driver.extraJavaOptions",
            "-Dderby.system.home=" + os.path.join(tmp, "derby"),
        )
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


def null_profile(frames):
    rows = []
    by_table = {}
    for spec in TABLES:
        name = spec["name"]
        df = frames[name]
        print(f"nulls {name}", flush=True)
        aggs = [F.count(F.lit(1)).alias("_rows")]
        key = df.columns[0]
        aggs.append(F.sum(F.when(F.col(key) == F.lit(key), 1).otherwise(0)).alias("_header"))
        for column in df.columns:
            aggs.append(
                F.sum(F.when(F.col(column).isNull(), 1).otherwise(0)).alias("nulls_" + column)
            )
        rec = df.agg(*aggs).collect()[0]
        total = int(rec["_rows"])
        if int(rec["_header"] or 0) > 0:
            raise SystemExit(f"{name}: header row was read as data")
        if total < spec["min_rows"]:
            raise SystemExit(f"{name}: only {total} rows, expected at least {spec['min_rows']}")
        by_table[name] = total
        for column in df.columns:
            nulls = int(rec["nulls_" + column] or 0)
            rows.append(
                {
                    "table": name,
                    "column": column,
                    "rows": total,
                    "nulls": nulls,
                    "null_pct": round(100.0 * nulls / total, 4) if total else 0.0,
                }
            )
    return rows, by_table


def _write_csv(path, rows, fields):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)


def _save(spark, df, clean_dir, name, rows_in):
    path = os.path.join(clean_dir, name.replace(".", "_"))
    print(f"write {name}", flush=True)
    df.write.mode("overwrite").parquet(path)
    out = spark.read.parquet(path)
    rows_out = out.count()
    print(f"{name}: {rows_in} -> {rows_out}", flush=True)
    return out, {
        "table": name,
        "rows_in": rows_in,
        "rows_out": rows_out,
        "rows_dropped": rows_in - rows_out,
    }


def clean_and_write(spark, frames, rows_in, clean_dir):
    os.makedirs(clean_dir, exist_ok=True)
    summary = []

    basics, row = _save(
        spark, clean_basics(frames["title.basics"]), clean_dir, "title.basics", rows_in["title.basics"]
    )
    summary.append(row)
    title_ids = basics.select("tconst").persist()
    title_ids.count()

    names, row = _save(
        spark,
        clean_names(frames["name.basics"], title_ids),
        clean_dir,
        "name.basics",
        rows_in["name.basics"],
    )
    summary.append(row)
    name_ids = names.select("nconst").persist()
    name_ids.count()

    for name, df in (
        ("title.ratings", clean_ratings(frames["title.ratings"], title_ids)),
        ("title.crew", clean_crew(frames["title.crew"], title_ids, name_ids)),
        ("title.episode", clean_episodes(frames["title.episode"], title_ids)),
        ("title.principals", clean_principals(frames["title.principals"], title_ids, name_ids)),
        ("title.akas", clean_akas(frames["title.akas"], title_ids)),
    ):
        _, row = _save(spark, df, clean_dir, name, rows_in[name])
        summary.append(row)

    _assert_cleaned(spark, clean_dir)
    title_ids.unpersist()
    name_ids.unpersist()
    return summary


def _assert_cleaned(spark, clean_dir):
    basics = spark.read.parquet(os.path.join(clean_dir, "title_basics"))
    row = basics.where(F.col("tconst") == "tt0000001").collect()
    if len(row) != 1:
        raise SystemExit("cleaned title.basics lost tt0000001")
    genres = list(row[0]["genres"])
    if genres != ["Documentary", "Short"] or row[0]["startYear"] != 1894 or row[0]["endYear"] is not None:
        raise SystemExit(f"cleaned Carmencita looks wrong: {row[0]}")

    crew = spark.read.parquet(os.path.join(clean_dir, "title_crew"))
    directed = crew.where(F.col("tconst") == "tt0000001").collect()
    if len(directed) != 1 or list(directed[0]["directors"]) != ["nm0005690"]:
        raise SystemExit(f"cleaned crew for tt0000001 looks wrong: {directed}")

    people = spark.read.parquet(os.path.join(clean_dir, "name_basics"))
    person = people.where(F.col("nconst") == "nm0000001").collect()
    if (
        len(person) != 1
        or person[0]["birthYear"] != 1899
        or person[0]["deathYear"] != 1987
        or not person[0]["knownForTitles"]
    ):
        raise SystemExit(f"cleaned nm0000001 looks wrong: {person}")

    ratings = spark.read.parquet(os.path.join(clean_dir, "title_ratings"))
    rating = ratings.where(F.col("tconst") == "tt0000001").collect()
    if len(rating) != 1 or abs(rating[0]["averageRating"] - 5.7) > 1e-6:
        raise SystemExit(f"cleaned rating looks wrong: {rating}")
    print("cleaned samples ok", flush=True)


def write_reports(report_dir, nulls, issues, ranges, summary, type_counts):
    os.makedirs(report_dir, exist_ok=True)
    _write_csv(
        os.path.join(report_dir, "null_percentages.csv"),
        nulls,
        ["table", "column", "rows", "nulls", "null_pct"],
    )
    top = sorted(nulls, key=lambda item: (-item["null_pct"], -item["nulls"], item["table"], item["column"]))[:5]
    _write_csv(
        os.path.join(report_dir, "top5_null_columns.csv"),
        top,
        ["table", "column", "rows", "nulls", "null_pct"],
    )
    _write_csv(
        os.path.join(report_dir, "quality_issues.csv"),
        issues,
        ["check", "table", "affected", "unit", "handling"],
    )
    _write_csv(
        os.path.join(report_dir, "numeric_ranges.csv"),
        ranges,
        ["table", "column", "min", "max"],
    )
    _write_csv(
        os.path.join(report_dir, "cleaning_summary.csv"),
        summary,
        ["table", "rows_in", "rows_out", "rows_dropped"],
    )
    _write_csv(
        os.path.join(report_dir, "title_types.csv"),
        type_counts,
        ["titleType", "titles"],
    )
    print("top 5 null columns:", flush=True)
    for item in top:
        print(f"  {item['table']}.{item['column']}: {item['null_pct']}%", flush=True)


def main():
    parser = argparse.ArgumentParser(description="IMDb data preparation")
    parser.add_argument("--rebuild", action="store_true", help="reparse the TSV dumps")
    args = parser.parse_args()

    os.chdir(ROOT)
    started = time.time()
    spark = build_spark()
    try:
        frames = load_tables(
            spark,
            os.path.join(ROOT, "data", "raw"),
            os.path.join(ROOT, "data", "tmp", "normalized"),
            rebuild=args.rebuild,
        )
        assert_samples(frames)
        nulls, rows_in = null_profile(frames)
        issues, ranges, type_counts = find_issues(frames)
        summary = clean_and_write(spark, frames, rows_in, os.path.join(ROOT, "data", "clean"))
        write_reports(
            os.path.join(ROOT, "output", "part01"),
            nulls,
            issues,
            ranges,
            summary,
            type_counts,
        )
    finally:
        spark.stop()
    print(f"done in {time.time() - started:.0f}s", flush=True)


if __name__ == "__main__":
    main()
