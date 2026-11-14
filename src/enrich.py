"""Part 3: enrich the top films of each genre and year with Wikipedia text.

Run from the repo root, once `python -m src.prepare` has written data/clean/:

    python -m src.enrich              # select, fetch, build
    python -m src.enrich select       # Spark: which films and which people
    python -m src.enrich fetch        # Python: Wikidata + Wikipedia (resumable)
    python -m src.enrich build        # Spark: clean, enforce schema, write outputs

Scope is set by the constants below (top 10 films per genre and year, 1980-2025,
3 principals per film). Nothing in the code depends on that scope: change the
constants (or set TOP_N_FILMS to None) and the same code runs on the full dataset.

Files:
    data/enrich/selection/*             Spark selection (parquet)
    data/enrich/cache/*.jsonl           raw Wikipedia/Wikidata answers
    data/enriched/{films,principals}    final tables (parquet)
    output/part03/*.csv                 coverage summaries and samples (committed)
"""

import argparse
import csv
import os
import sys
import time

import pyspark.sql.functions as F
from pyspark.sql import Window
from pyspark.sql.types import (
    ArrayType,
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
)

from src import wikipedia
from src.explore import FIRST_YEAR, LAST_YEAR, feature_movies, load_tables
from src.prepare import ROOT, build_spark

ENRICH_DIR = os.path.join(ROOT, "data", "enrich")
SELECT_DIR = os.path.join(ENRICH_DIR, "selection")
CACHE_DIR = os.path.join(ENRICH_DIR, "cache")
FINAL_DIR = os.path.join(ROOT, "data", "enriched")
OUT_DIR = os.path.join(ROOT, "output", "part03")

# Films per (year, genre), ranked by votes (the popularity measure of part 2).
# None means "every film", which is the full-dataset run.
TOP_N_FILMS = 10
# People per film, taken in IMDb billing order (title.principals.ordering).
N_PRINCIPALS = 3
# A synopsis shorter than this is a stub, not a plot.
MIN_SYNOPSIS_CHARS = 200
MIN_BIO_CHARS = 50

WIKIDATA_CACHE = os.path.join(CACHE_DIR, "wikidata.jsonl")
FILM_CACHE = os.path.join(CACHE_DIR, "film_pages.jsonl")
PERSON_CACHE = os.path.join(CACHE_DIR, "person_pages.jsonl")

WIKIDATA_SCHEMA = StructType(
    [
        StructField("imdb_id", StringType(), False),
        StructField("wikidata_uri", StringType(), True),
        StructField("wikipedia_title", StringType(), True),
        StructField("resolved_at", StringType(), True),
    ]
)
PAGE_SCHEMA = StructType(
    [
        StructField("imdb_id", StringType(), False),
        StructField("kind", StringType(), True),
        StructField("page_title", StringType(), True),
        StructField("revision_id", LongType(), True),
        StructField("source_url", StringType(), True),
        StructField("lead", StringType(), True),
        StructField("plot", StringType(), True),
        StructField("retrieved_at", StringType(), True),
    ]
)


# --- select -----------------------------------------------------------------

def select_targets(spark, t):
    """Top films per (year, genre) and their first principals."""
    movies = (
        feature_movies(t)
        .where(F.col("year").between(FIRST_YEAR, LAST_YEAR))
        .join(t["title_ratings"], "tconst")
        .where(F.col("genres").isNotNull())
        .withColumn("genre", F.explode("genres"))
    )
    order = Window.partitionBy("year", "genre").orderBy(
        F.desc("numVotes"), F.desc("averageRating"), "tconst"
    )
    ranked = movies.withColumn("genre_rank", F.row_number().over(order))
    if TOP_N_FILMS is not None:
        ranked = ranked.where(F.col("genre_rank") <= TOP_N_FILMS)

    # One row per film; the genres where it made the cut stay as an array.
    films = ranked.groupBy(
        "tconst", "primaryTitle", "year", "runtimeMinutes", "genres", "averageRating", "numVotes"
    ).agg(
        F.sort_array(
            F.collect_list(F.concat(F.col("genre"), F.lit(" #"), F.col("genre_rank").cast("string")))
        ).alias("top_in")
    )

    by_billing = Window.partitionBy("tconst").orderBy("ordering", "nconst")
    credits = (
        t["title_principals"]
        .join(films.select("tconst"), "tconst")
        .withColumn("billing", F.row_number().over(by_billing))
        .where(F.col("billing") <= N_PRINCIPALS)
        .select("tconst", "nconst", "ordering", "category", "billing")
    )
    people = (
        credits.join(
            t["name_basics"].select("nconst", "primaryName", "birthYear", "deathYear"), "nconst"
        )
    )

    films.write.mode("overwrite").parquet(os.path.join(SELECT_DIR, "films"))
    people.write.mode("overwrite").parquet(os.path.join(SELECT_DIR, "principals"))
    print(
        f"  selected {spark.read.parquet(os.path.join(SELECT_DIR, 'films')).count()} films, "
        f"{spark.read.parquet(os.path.join(SELECT_DIR, 'principals')).count()} credits",
        flush=True,
    )


# --- fetch ------------------------------------------------------------------

def read_ids(spark, name, column):
    path = os.path.join(SELECT_DIR, name)
    if not os.path.isdir(path):
        raise SystemExit(f"{path} not found, run: python -m src.enrich select")
    return [r[0] for r in spark.read.parquet(path).select(column).distinct().collect()]


def fetch_all(film_ids, person_ids):
    client = wikipedia.Client()
    print("fetch", flush=True)
    wikipedia.resolve_ids(client, film_ids + person_ids, WIKIDATA_CACHE)
    resolved = {r["imdb_id"]: r for r in wikipedia.read_jsonl(WIKIDATA_CACHE)}
    for kind, ids, path in (
        ("film", film_ids, FILM_CACHE),
        ("person", person_ids, PERSON_CACHE),
    ):
        targets = [resolved[i] for i in sorted(ids) if i in resolved]
        wikipedia.fetch_pages(client, kind, targets, path)
    print(f"  {client.requests} HTTP requests this run", flush=True)


# --- build ------------------------------------------------------------------

def tidy(column):
    """Collapse whitespace, drop citation marks, and turn empty text into null."""
    text = F.regexp_replace(column, r"\[(\d+|citation needed|[a-z])\]", "")
    text = F.regexp_replace(text, r"[ \t ]+", " ")
    text = F.regexp_replace(text, r" ?\n ?", "\n")
    text = F.regexp_replace(text, r"\n{3,}", "\n\n")
    text = F.trim(text)
    return F.when(F.length(text) > 0, text)


def notes(*pairs):
    """Array of the labels whose condition is true; null conditions count as false."""
    flags = [F.when(cond, F.lit(label)) for label, cond in pairs]
    return F.filter(F.array(*flags), lambda x: x.isNotNull())


def read_cache(spark, path, schema):
    if not os.path.exists(path):
        return spark.createDataFrame([], schema)
    return spark.read.schema(schema).option("mode", "FAILFAST").json(path)


def build_tables(spark):
    wikidata = read_cache(spark, WIKIDATA_CACHE, WIKIDATA_SCHEMA).dropDuplicates(["imdb_id"])
    film_pages = read_cache(spark, FILM_CACHE, PAGE_SCHEMA).dropDuplicates(["imdb_id"])
    person_pages = read_cache(spark, PERSON_CACHE, PAGE_SCHEMA).dropDuplicates(["imdb_id"])

    def attach(df, key, pages, plot):
        """Join Wikidata and page text onto df[key]; plot=True also keeps the Plot section."""
        joined = (
            df.join(wikidata.withColumnRenamed("imdb_id", key), key, "left")
            .join(pages.drop("kind").withColumnRenamed("imdb_id", key), key, "left")
        )
        return joined

    films_in = spark.read.parquet(os.path.join(SELECT_DIR, "films"))
    film = attach(films_in, "tconst", film_pages, True)
    plot = tidy(F.col("plot"))
    lead = tidy(F.col("lead"))
    has_plot = F.coalesce(F.length(plot) >= MIN_SYNOPSIS_CHARS, F.lit(False))
    has_lead = F.coalesce(F.length(lead) >= MIN_SYNOPSIS_CHARS, F.lit(False))
    films = film.select(
        "tconst",
        F.col("primaryTitle").alias("title"),
        F.col("year").cast(IntegerType()),
        F.col("runtimeMinutes").cast(IntegerType()),
        F.col("genres"),
        F.col("top_in").alias("top10_in"),
        F.col("averageRating").cast(DoubleType()).alias("average_rating"),
        F.col("numVotes").cast(LongType()).alias("num_votes"),
        F.when(has_plot, plot).when(has_lead, lead).alias("synopsis"),
        F.when(has_plot, F.lit("plot_section")).when(has_lead, F.lit("lead_paragraph")).alias(
            "synopsis_kind"
        ),
        F.col("page_title").alias("wikipedia_title"),
        F.col("source_url"),
        F.col("wikidata_uri"),
        F.col("revision_id").cast(LongType()),
        F.col("retrieved_at"),
        notes(
            ("no_wikipedia_page", F.col("wikipedia_title").isNull()),
            ("wikipedia_page_not_found", F.col("wikipedia_title").isNotNull() & F.col("page_title").isNull()),
            ("no_plot_section_used_lead", ~has_plot & has_lead),
            ("no_usable_synopsis", ~has_plot & ~has_lead & F.col("page_title").isNotNull()),
        ).alias("notes"),
    )

    people_in = spark.read.parquet(os.path.join(SELECT_DIR, "principals"))
    person = attach(people_in, "nconst", person_pages, False)
    bio = tidy(F.col("lead"))
    has_bio = F.coalesce(F.length(bio) >= MIN_BIO_CHARS, F.lit(False))
    principals = person.select(
        "tconst",
        "nconst",
        F.col("billing").cast(IntegerType()),
        "category",
        F.col("primaryName").alias("name"),
        F.col("birthYear").cast(IntegerType()).alias("birth_year"),
        F.col("deathYear").cast(IntegerType()).alias("death_year"),
        F.when(has_bio, bio).alias("biography"),
        F.col("page_title").alias("wikipedia_title"),
        F.col("source_url"),
        F.col("wikidata_uri"),
        F.col("revision_id").cast(LongType()),
        F.col("retrieved_at"),
        notes(
            ("no_wikipedia_page", F.col("wikipedia_title").isNull()),
            ("wikipedia_page_not_found", F.col("wikipedia_title").isNotNull() & F.col("page_title").isNull()),
            ("no_usable_biography", ~has_bio & F.col("page_title").isNotNull()),
        ).alias("notes"),
    )

    films.write.mode("overwrite").parquet(os.path.join(FINAL_DIR, "films"))
    principals.write.mode("overwrite").parquet(os.path.join(FINAL_DIR, "principals"))
    return (
        spark.read.parquet(os.path.join(FINAL_DIR, "films")),
        spark.read.parquet(os.path.join(FINAL_DIR, "principals")),
    )


# --- outputs ----------------------------------------------------------------

def _cell(value):
    if value is None:
        return ""
    if isinstance(value, float):
        return round(value, 3)
    if isinstance(value, list):
        return "|".join(str(v) for v in value)
    return value


def save(df, name):
    rows = df.collect()
    os.makedirs(OUT_DIR, exist_ok=True)
    path = os.path.join(OUT_DIR, name + ".csv")
    with open(path, "w", newline="", encoding="utf-8") as handle:
        writer = csv.writer(handle)
        writer.writerow(df.columns)
        for row in rows:
            writer.writerow([_cell(v) for v in row])
    print(f"  {name}.csv: {len(rows)} rows", flush=True)


def write_outputs(films, principals):
    note_film = F.explode_outer("notes")
    # coverage, overall
    f_total = films.count()
    p_total = principals.count()

    def pct(n, d):
        return round(100 * n / d, 2) if d else None

    f_page = films.where(F.col("wikipedia_title").isNotNull()).count()
    f_plot = films.where(F.col("synopsis_kind") == "plot_section").count()
    f_lead = films.where(F.col("synopsis_kind") == "lead_paragraph").count()
    p_page = principals.where(F.col("wikipedia_title").isNotNull()).count()
    p_bio = principals.where(F.col("biography").isNotNull()).count()
    per_film = principals.groupBy("tconst").agg(
        F.count("*").alias("principals"),
        F.sum(F.when(F.col("biography").isNotNull(), 1).otherwise(0)).alias("with_bio"),
    )
    three_bios = per_film.where(F.col("with_bio") >= 3).count()
    p_distinct = principals.select("nconst").distinct().count()
    spark = films.sparkSession
    summary = spark.createDataFrame(
        [
            ("films selected", f_total, 100.0),
            ("films with a Wikipedia page", f_page, pct(f_page, f_total)),
            ("films with a plot-section synopsis", f_plot, pct(f_plot, f_total)),
            ("films with a lead-paragraph synopsis only", f_lead, pct(f_lead, f_total)),
            ("films with no synopsis", f_total - f_plot - f_lead, pct(f_total - f_plot - f_lead, f_total)),
            ("principal credits selected", p_total, 100.0),
            ("distinct people", p_distinct, None),
            ("credits whose person has a Wikipedia page", p_page, pct(p_page, p_total)),
            ("credits with a biography", p_bio, pct(p_bio, p_total)),
            ("films with 3 or more biographies", three_bios, pct(three_bios, f_total)),
        ],
        "metric string, count long, percent double",
    )
    save(summary, "coverage_summary")

    save(
        films.groupBy("year")
        .agg(
            F.count("*").alias("films"),
            F.sum(F.when(F.col("synopsis").isNotNull(), 1).otherwise(0)).alias("with_synopsis"),
            F.sum(F.when(F.col("synopsis_kind") == "plot_section", 1).otherwise(0)).alias("with_plot"),
        )
        .withColumn("synopsis_pct", F.round(100 * F.col("with_synopsis") / F.col("films"), 2))
        .orderBy("year"),
        "coverage_films_by_year",
    )

    save(
        films.select("tconst", note_film.alias("note"))
        .groupBy("note").count().withColumn("table", F.lit("films"))
        .unionByName(
            principals.select("tconst", F.explode_outer("notes").alias("note"))
            .groupBy("note").count().withColumn("table", F.lit("principals"))
        )
        .select("table", F.coalesce("note", F.lit("(none)")).alias("note"), "count")
        .orderBy("table", F.desc("count")),
        "coverage_notes",
    )

    # A small readable sample: the 15 most voted films with a plot, and their people.
    sample = (
        films.where(F.col("synopsis_kind") == "plot_section")
        .orderBy(F.desc("num_votes"))
        .limit(15)
    )
    save(
        sample.select(
            "tconst", "title", "year", "top10_in", "wikipedia_title", "source_url",
            "synopsis_kind", F.substring("synopsis", 1, 400).alias("synopsis_start"), "notes",
        ),
        "sample_films",
    )
    save(
        principals.join(sample.select("tconst"), "tconst")
        .select(
            "tconst", "billing", "category", "name", "wikipedia_title", "source_url",
            F.substring("biography", 1, 300).alias("biography_start"), "notes",
        )
        .orderBy("tconst", "billing"),
        "sample_principals",
    )


def main():
    parser = argparse.ArgumentParser(description="IMDb enrichment (part 3)")
    parser.add_argument("steps", nargs="*", help="select, fetch, build; default: all")
    args = parser.parse_args()
    steps = args.steps or ["select", "fetch", "build"]
    unknown = [s for s in steps if s not in ("select", "fetch", "build")]
    if unknown:
        sys.exit("unknown step: " + ", ".join(unknown))

    os.chdir(ROOT)
    started = time.time()
    spark = build_spark()
    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", str(64 * 1024 * 1024))
    try:
        if "select" in steps:
            print("select", flush=True)
            select_targets(spark, load_tables(spark))
        if "fetch" in steps:
            fetch_all(read_ids(spark, "films", "tconst"), read_ids(spark, "principals", "nconst"))
        if "build" in steps:
            print("build", flush=True)
            films, principals = build_tables(spark)
            write_outputs(films, principals)
    finally:
        spark.stop()
    print(f"done in {time.time() - started:.0f}s", flush=True)


if __name__ == "__main__":
    main()
