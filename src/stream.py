"""Part 4: Wikipedia edits -> Kafka -> Spark Structured Streaming, matched to IMDb.

Run from the repo root, with Kafka up and src/wiki_producer.py feeding it:

    python -m src.stream lookup               # batch, once: IMDb lookup tables
    python -m src.stream run --minutes 25     # the streaming job
    python -m src.stream report               # batch: CSV summaries in output/part04/

Filter: English Wikipedia article edits (enwiki, namespace 0, edit or new page)
whose page is a film or a person from the IMDb dataset. A page matches on the
first of these that works:

1. wikidata_page   the exact page title was resolved from an IMDb id in part 3
2. film_title_year "Title (2010 film)": title + year of a feature film
3. film_title      "Title (film)": title of a feature film, most voted one wins
4. person_name     "Name" or "Name (actor)": an actor, actress, director or writer

Films and people only count if they have a feature film with at least
LOOKUP_MIN_VOTES votes, so a match is something people actually know.

Late data: event time is the edit time written by Wikipedia. The windowed
counts use a watermark of WATERMARK_SECONDS; a later event is dropped from the
counts (Spark reports how many), but it is still written to the matches table
with its lateness, so nothing is lost silently.
"""

import argparse
import csv
import json
import os
import shutil
import sys
import time

import pyspark.sql.functions as F
from pyspark.sql import Window
from pyspark.sql.types import BooleanType, IntegerType, LongType, StringType, StructField, StructType

from src.enrich import WIKIDATA_CACHE, WIKIDATA_SCHEMA
from src.explore import feature_movies, load_tables, norm_title
from src.prepare import ROOT, build_spark
from src.wiki_producer import KAFKA_BOOTSTRAP, TOPIC

KAFKA_PACKAGE = "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.4"

STREAM_DIR = os.path.join(ROOT, "data", "stream")
LOOKUP_DIR = os.path.join(STREAM_DIR, "lookup")
MATCHES_DIR = os.path.join(STREAM_DIR, "matches")
WINDOWS_DIR = os.path.join(STREAM_DIR, "windows")
CHECKPOINT_DIR = os.path.join(STREAM_DIR, "checkpoints")
PROGRESS_FILE = os.path.join(STREAM_DIR, "progress.jsonl")
OUT_DIR = os.path.join(ROOT, "output", "part04")

LOOKUP_MIN_VOTES = 1000
PERSON_CATEGORIES = ("actor", "actress", "director", "writer")
# Wikipedia disambiguation that still means "this person"
PERSON_QUALIFIERS = (
    "actor", "actress", "director", "film director", "filmmaker", "screenwriter",
    "writer", "producer", "film producer", "comedian", "entertainer",
)
# "film", "2010 film", "American film", "2014 American film", ...
FILM_QUALIFIER_RE = r"^(\d{4} )?([A-Za-z' -]+ )?film$"

WINDOW = "2 minutes"
WATERMARK_SECONDS = 300
TRIGGER = "30 seconds"
MAX_OFFSETS_PER_TRIGGER = 20000

EVENT_SCHEMA = StructType(
    [
        StructField(
            "meta",
            StructType(
                [
                    StructField("id", StringType()),
                    StructField("dt", StringType()),
                    StructField("uri", StringType()),
                    StructField("domain", StringType()),
                ]
            ),
        ),
        StructField("type", StringType()),
        StructField("namespace", IntegerType()),
        StructField("title", StringType()),
        StructField("timestamp", LongType()),
        StructField("user", StringType()),
        StructField("bot", BooleanType()),
        StructField("wiki", StringType()),
        StructField("length", StructType([StructField("old", LongType()), StructField("new", LongType())])),
        StructField("revision", StructType([StructField("old", LongType()), StructField("new", LongType())])),
    ]
)


def spark_session():
    return build_spark(
        {
            "spark.app.name": "imdb-wikipedia-streaming",
            "spark.jars.packages": KAFKA_PACKAGE,
            # micro-batches are small; 64 shuffle partitions would be mostly empty
            "spark.sql.shuffle.partitions": "8",
            "spark.sql.autoBroadcastJoinThreshold": str(64 * 1024 * 1024),
        }
    )


# --- lookup (batch) -----------------------------------------------------------

def build_lookup(spark):
    t = load_tables(spark)
    movies = (
        feature_movies(t)
        .join(t["title_ratings"].where(F.col("numVotes") >= LOOKUP_MIN_VOTES), "tconst")
        .select("tconst", "primaryTitle", "year", "numVotes")
        .persist()
    )
    films = movies.withColumn("key", norm_title("primaryTitle")).where(F.col("key") != "")
    by_year = Window.partitionBy("key", "year").orderBy(F.desc("numVotes"), "tconst")
    by_title = Window.partitionBy("key").orderBy(F.desc("numVotes"), "tconst")
    films_by_year = (
        films.withColumn("rn", F.row_number().over(by_year))
        .where(F.col("rn") == 1)
        .select("key", "year", "tconst", F.col("primaryTitle").alias("label"))
    )
    films_any = (
        films.withColumn("rn", F.row_number().over(by_title))
        .where(F.col("rn") == 1)
        .select("key", "tconst", F.col("primaryTitle").alias("label"))
    )

    credits = (
        t["title_principals"]
        .where(F.col("category").isin(*PERSON_CATEGORIES))
        .join(movies.select("tconst", "numVotes"), "tconst")
        .dropDuplicates(["tconst", "nconst"])
        .groupBy("nconst")
        .agg(F.sum("numVotes").alias("votes"))
    )
    # One-word names ("Madonna", but also "Paris") collide with too many pages.
    named = (
        credits.join(t["name_basics"].select("nconst", "primaryName"), "nconst")
        .where(F.col("primaryName").contains(" "))
        .withColumn("key", norm_title("primaryName"))
        .where(F.col("key") != "")
    )
    by_name = Window.partitionBy("key")
    people = (
        named.withColumn("candidates", F.count("*").over(by_name))
        .withColumn("rn", F.row_number().over(by_name.orderBy(F.desc("votes"), "nconst")))
        .where(F.col("rn") == 1)
        .select("key", "nconst", F.col("primaryName").alias("label"), "candidates")
    )

    if os.path.exists(WIKIDATA_CACHE):
        labels = (
            t["title_basics"].select(F.col("tconst").alias("imdb_id"), F.col("primaryTitle").alias("label"))
            .unionByName(t["name_basics"].select(F.col("nconst").alias("imdb_id"), F.col("primaryName").alias("label")))
        )
        pages = (
            spark.read.schema(WIKIDATA_SCHEMA).json(WIKIDATA_CACHE)
            .where(F.col("wikipedia_title").isNotNull())
            .select(F.col("wikipedia_title").alias("page_title"), "imdb_id")
            .dropDuplicates(["page_title"])
            .join(labels, "imdb_id", "left")
        )
    else:
        print(f"  {WIKIDATA_CACHE} not found (run python -m src.enrich): no wikidata_page matches", flush=True)
        pages = spark.createDataFrame([], "page_title string, imdb_id string, label string")

    for name, df in (
        ("pages", pages),
        ("films_by_year", films_by_year),
        ("films_any", films_any),
        ("people", people),
    ):
        path = os.path.join(LOOKUP_DIR, name)
        df.write.mode("overwrite").parquet(path)
        print(f"  lookup {name}: {spark.read.parquet(path).count()} rows", flush=True)
    movies.unpersist()


# --- streaming job --------------------------------------------------------------

def read_lookup(spark, name, prefix):
    path = os.path.join(LOOKUP_DIR, name)
    if not os.path.isdir(path):
        raise SystemExit(f"{path} not found, run: python -m src.stream lookup")
    df = spark.read.parquet(path)
    return F.broadcast(df.select([F.col(c).alias(prefix + c) for c in df.columns]).cache())


def article_edits(spark, starting_offsets):
    raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", TOPIC)
        .option("startingOffsets", starting_offsets)
        .option("maxOffsetsPerTrigger", MAX_OFFSETS_PER_TRIGGER)
        .option("failOnDataLoss", "false")
        .load()
    )
    events = raw.select(
        F.from_json(F.col("value").cast("string"), EVENT_SCHEMA).alias("e"),
        F.col("timestamp").alias("kafka_time"),
    )
    edits = events.select(
        F.col("e.meta.id").alias("event_id"),
        # edit time from Wikipedia; meta.dt is the fallback when timestamp is missing
        F.coalesce(F.timestamp_seconds("e.timestamp"), F.to_timestamp("e.meta.dt")).alias("event_time"),
        "kafka_time",
        F.col("e.wiki").alias("wiki"),
        F.col("e.namespace").alias("namespace"),
        F.col("e.type").alias("type"),
        F.col("e.title").alias("title"),
        F.col("e.user").alias("user"),
        F.col("e.bot").alias("bot"),
        (F.col("e.length.new") - F.col("e.length.old")).alias("size_delta"),
        F.col("e.revision.old").alias("rev_old"),
        F.col("e.revision.new").alias("rev_new"),
        F.col("e.meta.uri").alias("uri"),
    ).where(
        (F.col("wiki") == "enwiki")
        & (F.col("namespace") == 0)
        & F.col("type").isin("edit", "new")
        & F.col("event_time").isNotNull()
    )
    qualifier = F.regexp_extract("title", r"^.*? \(([^()]*)\)$", 1)
    return (
        edits.withColumn("base", F.regexp_extract("title", r"^(.*?)(?: \([^()]*\))?$", 1))
        .withColumn("qualifier", F.when(qualifier != "", qualifier))
        .withColumn("film_page", F.coalesce(F.col("qualifier").rlike(FILM_QUALIFIER_RE), F.lit(False)))
        .withColumn(
            "page_year",
            F.when(F.col("film_page"), F.regexp_extract("qualifier", r"^(\d{4}) ", 1).cast("int")),
        )
        .withColumn(
            "person_page",
            F.col("qualifier").isNull() | F.lower(F.col("qualifier")).isin(*PERSON_QUALIFIERS),
        )
        .withColumn("key", norm_title("base"))
        .withColumn("lateness_s", F.col("kafka_time").cast("long") - F.col("event_time").cast("long"))
    )


def match_imdb(spark, edits):
    pages = read_lookup(spark, "pages", "pg_")
    fy = read_lookup(spark, "films_by_year", "fy_")
    fa = read_lookup(spark, "films_any", "fa_")
    pp = read_lookup(spark, "people", "pp_")
    key = F.col("key")
    joined = (
        edits.join(pages, F.col("title") == F.col("pg_page_title"), "left")
        .join(
            fy,
            F.col("film_page") & (key == F.col("fy_key")) & (F.col("page_year") == F.col("fy_year")),
            "left",
        )
        .join(fa, F.col("film_page") & F.col("page_year").isNull() & (key == F.col("fa_key")), "left")
        .join(pp, F.col("person_page") & (key == F.col("pp_key")), "left")
    )
    match_type = (
        F.when(F.col("pg_imdb_id").isNotNull(), "wikidata_page")
        .when(F.col("fy_tconst").isNotNull(), "film_title_year")
        .when(F.col("fa_tconst").isNotNull(), "film_title")
        .when(F.col("pp_nconst").isNotNull(), "person_name")
    )
    return joined.select(
        "event_id",
        "event_time",
        "kafka_time",
        "lateness_s",
        (F.col("lateness_s") > WATERMARK_SECONDS).alias("after_watermark"),
        "title",
        "type",
        "user",
        "bot",
        "size_delta",
        "rev_new",
        "uri",
        F.when(
            F.col("rev_old").isNotNull(),
            F.format_string("https://en.wikipedia.org/w/index.php?diff=%d&oldid=%d", "rev_new", "rev_old"),
        ).alias("diff_url"),
        match_type.alias("match_type"),
        F.coalesce("pg_imdb_id", "fy_tconst", "fa_tconst", "pp_nconst").alias("imdb_id"),
        F.coalesce("pg_label", "fy_label", "fa_label", "pp_label").alias("imdb_label"),
        F.when(match_type == "person_name", F.col("pp_candidates")).alias("name_candidates"),
    )


def _progress_dict(p):
    return p if isinstance(p, dict) else json.loads(p.json)


def log_progress(queries, seen):
    with open(PROGRESS_FILE, "a", encoding="utf-8") as handle:
        for q in queries:
            for p in q.recentProgress:
                p = _progress_dict(p)
                key = (p.get("name"), p.get("batchId"))
                if key in seen:
                    continue
                seen.add(key)
                dropped = sum(op.get("numRowsDroppedByWatermark", 0) for op in p.get("stateOperators", []))
                row = {
                    "query": p.get("name"),
                    "batch": p.get("batchId"),
                    "time": p.get("timestamp"),
                    "input_rows": p.get("numInputRows", 0),
                    "rows_per_s": p.get("processedRowsPerSecond"),
                    "watermark": p.get("eventTime", {}).get("watermark"),
                    "dropped_by_watermark": dropped,
                    "batch_ms": p.get("durationMs", {}).get("triggerExecution"),
                }
                handle.write(json.dumps(row) + "\n")
                if row["query"] == "windows":
                    print(
                        f"  batch {row['batch']}: {row['input_rows']} kafka rows, "
                        f"watermark {row['watermark']}, dropped late {dropped}",
                        flush=True,
                    )


def run_stream(spark, minutes, starting_offsets, fresh):
    if fresh:
        for path in (MATCHES_DIR, WINDOWS_DIR, CHECKPOINT_DIR, PROGRESS_FILE):
            if os.path.isdir(path):
                shutil.rmtree(path)
            elif os.path.exists(path):
                os.remove(path)
    os.makedirs(STREAM_DIR, exist_ok=True)

    matched = match_imdb(spark, article_edits(spark, starting_offsets))

    matches_query = (
        matched.where(F.col("match_type").isNotNull())
        .writeStream.queryName("matches")
        .format("parquet")
        .option("path", MATCHES_DIR)
        .option("checkpointLocation", os.path.join(CHECKPOINT_DIR, "matches"))
        .outputMode("append")
        .trigger(processingTime=TRIGGER)
        .start()
    )
    windows = (
        matched.withWatermark("event_time", f"{WATERMARK_SECONDS} seconds")
        .groupBy(
            F.window("event_time", WINDOW),
            F.coalesce("match_type", F.lit("no_match")).alias("match_type"),
        )
        .agg(
            F.count("*").alias("edits"),
            F.approx_count_distinct("title").alias("pages"),
            F.avg("lateness_s").alias("avg_lateness_s"),
            F.max("lateness_s").alias("max_lateness_s"),
        )
        .select(
            F.col("window.start").alias("window_start"),
            F.col("window.end").alias("window_end"),
            "match_type",
            "edits",
            "pages",
            "avg_lateness_s",
            "max_lateness_s",
        )
    )
    windows_query = (
        windows.writeStream.queryName("windows")
        .format("parquet")
        .option("path", WINDOWS_DIR)
        .option("checkpointLocation", os.path.join(CHECKPOINT_DIR, "windows"))
        .outputMode("append")
        .trigger(processingTime=TRIGGER)
        .start()
    )

    queries = (matches_query, windows_query)
    seen = set()
    deadline = time.time() + minutes * 60
    print(f"streaming for {minutes} min from {TOPIC} (Ctrl+C stops early)", flush=True)
    try:
        while time.time() < deadline:
            time.sleep(10)
            for q in queries:
                if not q.isActive:
                    raise SystemExit(f"query {q.name} stopped: {q.exception()}")
            log_progress(queries, seen)
    except KeyboardInterrupt:
        print("stopping", flush=True)
    finally:
        for q in queries:
            if q.isActive:
                q.stop()
        log_progress(queries, seen)


# --- report (batch) -------------------------------------------------------------

def _cell(value):
    if value is None:
        return ""
    if isinstance(value, float):
        return round(value, 3)
    return value


def save(df, name):
    rows = df.collect()
    os.makedirs(OUT_DIR, exist_ok=True)
    with open(os.path.join(OUT_DIR, name + ".csv"), "w", newline="", encoding="utf-8") as handle:
        writer = csv.writer(handle)
        writer.writerow(df.columns)
        for row in rows:
            writer.writerow([_cell(v) for v in row])
    print(f"  {name}.csv: {len(rows)} rows", flush=True)


def report(spark):
    if not os.path.isdir(MATCHES_DIR):
        raise SystemExit(f"{MATCHES_DIR} not found, run: python -m src.stream run")
    # A restarted query can write an event twice; the event id is unique.
    matches = spark.read.parquet(MATCHES_DIR).dropDuplicates(["event_id"]).persist()
    has_windows = os.path.isdir(WINDOWS_DIR) and any(
        f.endswith(".parquet") for f in os.listdir(WINDOWS_DIR)
    )
    windows = spark.read.parquet(WINDOWS_DIR) if has_windows else None

    progress = []
    if os.path.exists(PROGRESS_FILE):
        with open(PROGRESS_FILE, encoding="utf-8") as handle:
            progress = [json.loads(line) for line in handle if line.strip()]
    kafka_rows = sum(p["input_rows"] for p in progress if p["query"] == "windows")
    dropped = sum(p["dropped_by_watermark"] for p in progress if p["query"] == "windows")
    batches = len([p for p in progress if p["query"] == "windows" and p["input_rows"] > 0])

    stats = matches.agg(
        F.count("*").alias("n"),
        F.countDistinct("title").alias("pages"),
        F.countDistinct("imdb_id").alias("imdb_ids"),
        F.min("event_time").alias("first"),
        F.max("event_time").alias("last"),
        F.expr("percentile_approx(lateness_s, 0.5)").alias("p50"),
        F.expr("percentile_approx(lateness_s, 0.95)").alias("p95"),
        F.max("lateness_s").alias("max_late"),
        F.sum(F.when(F.col("after_watermark"), 1).otherwise(0)).alias("after_wm"),
        F.sum(F.when(F.col("bot"), 1).otherwise(0)).alias("bots"),
    ).collect()[0]
    window_edits = window_matched = 0
    if windows is not None:
        w = windows.agg(
            F.sum("edits").alias("all"),
            F.sum(F.when(F.col("match_type") != "no_match", F.col("edits")).otherwise(0)).alias("matched"),
        ).collect()[0]
        window_edits, window_matched = int(w["all"] or 0), int(w["matched"] or 0)
    span = (stats["last"] - stats["first"]).total_seconds() / 60 if stats["n"] else 0

    summary = [
        ("kafka events read (all wikis)", kafka_rows),
        ("micro-batches with data", batches),
        ("enwiki article edits in closed windows", window_edits),
        ("matched edits in closed windows", window_matched),
        ("matched edits (all, deduplicated)", stats["n"]),
        ("matched edits per minute", round(stats["n"] / span, 2) if span else None),
        ("distinct pages matched", stats["pages"]),
        ("distinct IMDb ids matched", stats["imdb_ids"]),
        ("matched edits made by bots", stats["bots"]),
        ("event time span (minutes)", round(span, 1)),
        ("lateness p50 (s)", stats["p50"]),
        ("lateness p95 (s)", stats["p95"]),
        ("lateness max (s)", stats["max_late"]),
        (f"matched edits later than the {WATERMARK_SECONDS}s watermark", stats["after_wm"]),
        ("late partial aggregates dropped by the watermark (Spark metric, not events)", dropped),
    ]
    save(spark.createDataFrame([(k, "" if v is None else str(v)) for k, v in summary], "metric string, value string"), "run_summary")

    save(
        matches.groupBy("match_type")
        .agg(F.count("*").alias("edits"), F.countDistinct("title").alias("pages"))
        .orderBy(F.desc("edits")),
        "matches_by_type",
    )
    save(
        matches.groupBy("title", "match_type", "imdb_id", "imdb_label")
        .agg(F.count("*").alias("edits"), F.max("event_time").alias("last_edit"))
        .orderBy(F.desc("edits"), "title")
        .limit(30),
        "top_pages",
    )
    save(
        matches.orderBy("event_time")
        .select(
            "event_time", "lateness_s", "title", "match_type", "imdb_id", "imdb_label",
            "user", "bot", "size_delta", "diff_url",
        )
        .limit(40),
        "sample_matches",
    )
    bucket = (
        F.when(F.col("lateness_s") < 10, "0-10 s")
        .when(F.col("lateness_s") < 60, "10-60 s")
        .when(F.col("lateness_s") < WATERMARK_SECONDS, f"60-{WATERMARK_SECONDS} s")
        .when(F.col("lateness_s") < 900, f"{WATERMARK_SECONDS}-900 s")
        .otherwise("900 s or more")
    )
    order = F.min("lateness_s")
    save(
        matches.groupBy(bucket.alias("lateness")).agg(F.count("*").alias("edits"), order.alias("_o"))
        .orderBy("_o").drop("_o"),
        "lateness_buckets",
    )
    if windows is not None:
        save(
            windows.groupBy("window_start", "window_end")
            .pivot("match_type")
            .agg(F.sum("edits"))
            .fillna(0)
            .orderBy("window_start"),
            "window_counts",
        )
    matches.unpersist()


def main():
    parser = argparse.ArgumentParser(description="Wikipedia streaming matched to IMDb (part 4)")
    parser.add_argument("step", choices=["lookup", "run", "report"])
    parser.add_argument("--minutes", type=float, default=25, help="run: how long to stream")
    parser.add_argument(
        "--from", dest="starting", choices=["earliest", "latest"], default="earliest",
        help="run: where to start reading the topic (first run only, then the checkpoint decides)",
    )
    parser.add_argument("--fresh", action="store_true", help="run: delete previous outputs and checkpoints")
    args = parser.parse_args()

    os.chdir(ROOT)
    started = time.time()
    spark = spark_session()
    try:
        if args.step == "lookup":
            build_lookup(spark)
        elif args.step == "run":
            run_stream(spark, args.minutes, args.starting, args.fresh)
        else:
            report(spark)
    finally:
        spark.stop()
    print(f"done in {time.time() - started:.0f}s", flush=True)


if __name__ == "__main__":
    sys.exit(main())
