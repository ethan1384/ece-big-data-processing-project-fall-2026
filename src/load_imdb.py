"""Read the seven IMDb TSV dumps and rewrite them as Parquet.

IMDb marks missing cells with the two characters \\N. Empty strings are
treated the same way. Quoting is turned off: fields are separated by tabs
and `characters` contains raw JSON with double quotes.
"""

import os

import pyspark.sql.functions as F
from pyspark.sql.types import StringType, StructField, StructType

TABLES = [
    {
        "name": "name.basics",
        "file": "name.basics.tsv.gz",
        "columns": [
            "nconst",
            "primaryName",
            "birthYear",
            "deathYear",
            "primaryProfession",
            "knownForTitles",
        ],
        "partitions": 16,
        "min_rows": 5_000_000,
    },
    {
        "name": "title.akas",
        "file": "title.akas.tsv.gz",
        "columns": [
            "titleId",
            "ordering",
            "title",
            "region",
            "language",
            "types",
            "attributes",
            "isOriginalTitle",
        ],
        "partitions": 32,
        "min_rows": 10_000_000,
    },
    {
        "name": "title.basics",
        "file": "title.basics.tsv.gz",
        "columns": [
            "tconst",
            "titleType",
            "primaryTitle",
            "originalTitle",
            "isAdult",
            "startYear",
            "endYear",
            "runtimeMinutes",
            "genres",
        ],
        "partitions": 16,
        "min_rows": 5_000_000,
    },
    {
        "name": "title.crew",
        "file": "title.crew.tsv.gz",
        "columns": ["tconst", "directors", "writers"],
        "partitions": 8,
        "min_rows": 5_000_000,
    },
    {
        "name": "title.episode",
        "file": "title.episode.tsv.gz",
        "columns": ["tconst", "parentTconst", "seasonNumber", "episodeNumber"],
        "partitions": 8,
        "min_rows": 1_000_000,
    },
    {
        "name": "title.principals",
        "file": "title.principals.tsv.gz",
        "columns": ["tconst", "ordering", "nconst", "category", "job", "characters"],
        "partitions": 64,
        "min_rows": 20_000_000,
    },
    {
        "name": "title.ratings",
        "file": "title.ratings.tsv.gz",
        "columns": ["tconst", "averageRating", "numVotes"],
        "partitions": 4,
        "min_rows": 500_000,
    },
]


def _blank_to_null(df):
    for name in df.columns:
        text = F.trim(F.col(name))
        df = df.withColumn(
            name,
            F.when(text.isNull() | (text == "") | (text == r"\N"), None).otherwise(text),
        )
    return df


def _read_tsv(spark, path, columns):
    schema = StructType([StructField(name, StringType(), True) for name in columns])
    return (
        spark.read.option("sep", "\t")
        .option("header", True)
        .option("quote", "\u0000")
        .option("escape", "\u0000")
        .option("mode", "PERMISSIVE")
        .schema(schema)
        .csv(path)
    )


def _ready(path):
    return os.path.isfile(os.path.join(path, "_SUCCESS"))


def load_tables(spark, raw_dir, tmp_dir, rebuild=False):
    missing = []
    for spec in TABLES:
        path = os.path.join(raw_dir, spec["file"])
        if not os.path.isfile(path) or os.path.getsize(path) == 0:
            missing.append(spec["file"])
    if missing:
        raise SystemExit(
            "missing IMDb files in " + raw_dir + ": " + ", ".join(missing)
            + "\nrun: bash scripts/download_imdb.sh"
        )

    os.makedirs(tmp_dir, exist_ok=True)
    frames = {}
    for spec in TABLES:
        name = spec["name"]
        dest = os.path.join(tmp_dir, name.replace(".", "_"))
        if rebuild or not _ready(dest):
            print(f"parse {spec['file']}", flush=True)
            raw = _blank_to_null(_read_tsv(spark, os.path.join(raw_dir, spec["file"]), spec["columns"]))
            raw.repartition(spec["partitions"]).write.mode("overwrite").parquet(dest)
        frames[name] = spark.read.parquet(dest)
    return frames


def assert_samples(frames):
    """A few cells we know from the public files. Catches a bad TSV parse."""
    basics = frames["title.basics"].where(F.col("tconst") == "tt0000001").collect()
    if len(basics) != 1:
        raise SystemExit(f"tt0000001 parsed {len(basics)} rows, expected 1")
    row = basics[0]
    expected = {
        "titleType": "short",
        "primaryTitle": "Carmencita",
        "originalTitle": "Carmencita",
        "isAdult": "0",
        "startYear": "1894",
        "endYear": None,
        "runtimeMinutes": "1",
        "genres": "Documentary,Short",
    }
    for key, value in expected.items():
        if row[key] != value:
            raise SystemExit(f"title.basics.{key}: got {row[key]!r}, expected {value!r}")

    principal = frames["title.principals"].where(
        (F.col("tconst") == "tt0000001") & (F.col("ordering") == "1")
    ).collect()
    if len(principal) != 1 or principal[0]["characters"] != '["Self"]' or principal[0]["job"] is not None:
        raise SystemExit(f"principals sample looks wrong: {principal}")

    rating = frames["title.ratings"].where(F.col("tconst") == "tt0000001").collect()
    if len(rating) != 1 or rating[0]["averageRating"] != "5.7":
        raise SystemExit(f"ratings sample looks wrong: {rating}")

    aka = frames["title.akas"].where(
        (F.col("titleId") == "tt0000001") & (F.col("ordering") == "1")
    ).collect()
    if len(aka) != 1 or aka[0]["region"] is not None or aka[0]["isOriginalTitle"] != "1":
        raise SystemExit(f"akas sample looks wrong: {aka}")

    print("sample rows ok", flush=True)
