"""Part 5 (open question): do film sequels hold up against the film before
them, and does keeping the director or the lead actor make a difference?

Run from the repo root, once `python -m src.prepare` has written data/clean/:

    python -m src.open_question             # fetch (if not cached), then analyze
    python -m src.open_question fetch       # Wikidata: which films belong to which series
    python -m src.open_question analyze     # Spark: joins and CSVs in output/part05/

Extra data: Wikidata (CC0, edited by people). Property P179 "part of the
series" on films that carry an IMDb id (P345) says which films form one
franchise. That is two SPARQL queries in total, sent through the rate-limited
client of part 3 (src/wikipedia.py). Every franchise row keeps the Wikidata
URIs of the film and of the series as lineage.

A sequel only gets made when the first film did well, so sequels are expected
to score lower even if nothing about them is worse (regression to the mean).
The baseline for that: a director's next film that is not part of any series,
compared at the same starting rating.
"""

import argparse
import csv
import json
import os
import sys
import time

import pyspark.sql.functions as F
from pyspark.sql import Window
from pyspark.sql.types import StringType, StructField, StructType

from src import wikipedia
from src.explore import ACTING, feature_movies, load_tables
from src.prepare import ROOT, build_spark

OPEN_DIR = os.path.join(ROOT, "data", "open")
SERIES_CACHE = os.path.join(OPEN_DIR, "wikidata_series.jsonl")
OUT_DIR = os.path.join(ROOT, "output", "part05")

FILM_TYPES = {"Q11424": "film", "Q202866": "animated film"}
# Ratings below this many votes are too noisy to compare two films.
MIN_VOTES = 1000
# A "series" with more films than this is a shared universe or a loose
# collection, not a line of sequels.
MAX_INSTALLMENTS = 30
MIN_GROUP = 30

SERIES_SCHEMA = StructType(
    [
        StructField("film_uri", StringType()),
        StructField("imdb_id", StringType()),
        StructField("series_uri", StringType()),
        StructField("series_label", StringType()),
        StructField("retrieved_at", StringType()),
    ]
)


# --- fetch (plain Python) --------------------------------------------------------

def sparql(film_type):
    return (
        "SELECT ?film ?imdb ?series ?seriesLabel WHERE { "
        f"?film wdt:P31 wd:{film_type} ; wdt:P179 ?series ; wdt:P345 ?imdb . "
        'FILTER(STRSTARTS(?imdb, "tt")) '
        'OPTIONAL { ?series rdfs:label ?seriesLabel . FILTER(LANG(?seriesLabel) = "en") } }'
    )


def fetch(refetch=False):
    if os.path.exists(SERIES_CACHE) and not refetch:
        print(f"  {SERIES_CACHE} already there (use --refetch to download again)", flush=True)
        return
    client = wikipedia.Client()
    records = []
    for qid, name in FILM_TYPES.items():
        print(f"  wikidata query: {name} ({qid}) in a series", flush=True)
        data = client.get_json(
            wikipedia.SPARQL_URL,
            {"query": sparql(qid), "format": "json"},
            accept="application/sparql-results+json",
            post=True,
            interval=wikipedia.SPARQL_INTERVAL,
        )
        stamp = wikipedia.now()
        rows = data["results"]["bindings"]
        for row in rows:
            records.append(
                {
                    "film_uri": row["film"]["value"],
                    "imdb_id": row["imdb"]["value"],
                    "series_uri": row["series"]["value"],
                    "series_label": row.get("seriesLabel", {}).get("value"),
                    "retrieved_at": stamp,
                }
            )
        print(f"    {len(rows)} rows", flush=True)
    os.makedirs(OPEN_DIR, exist_ok=True)
    # written in one go, so a failed run never leaves half a cache behind
    tmp = SERIES_CACHE + ".tmp"
    with open(tmp, "w", encoding="utf-8") as handle:
        for record in records:
            handle.write(json.dumps(record, ensure_ascii=False) + "\n")
    os.replace(tmp, SERIES_CACHE)
    print(f"  {len(records)} rows saved, {client.requests} HTTP requests", flush=True)


# --- analyze (Spark) ---------------------------------------------------------------

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
    with open(os.path.join(OUT_DIR, name + ".csv"), "w", newline="", encoding="utf-8") as handle:
        writer = csv.writer(handle)
        writer.writerow(df.columns)
        for row in rows:
            writer.writerow([_cell(v) for v in row])
    print(f"  {name}.csv: {len(rows)} rows", flush=True)


def diff_stats(diff="diff"):
    d = F.col(diff)
    return [
        F.count("*").alias("pairs"),
        F.round(F.avg(d), 3).alias("mean_diff"),
        F.expr(f"percentile_approx({diff}, 0.5)").alias("median_diff"),
        F.round(100 * F.avg(F.when(d < 0, 1).otherwise(0)), 1).alias("pct_lower"),
        F.round(100 * F.avg(F.when(d > 0, 1).otherwise(0)), 1).alias("pct_higher"),
    ]


def rating_bucket(col):
    return (
        F.when(col < 6, "below 6")
        .when(col < 7, "6 to 7")
        .when(col < 8, "7 to 8")
        .otherwise("8 and up")
    )


def analyze(spark):
    if not os.path.exists(SERIES_CACHE):
        raise SystemExit(f"{SERIES_CACHE} not found, run: python -m src.open_question fetch")
    t = load_tables(spark)
    wikidata = spark.read.schema(SERIES_SCHEMA).json(SERIES_CACHE).dropDuplicates(["imdb_id", "series_uri"])
    movies = (
        feature_movies(t)
        .join(t["title_ratings"].where(F.col("numVotes") >= MIN_VOTES), "tconst")
        .select("tconst", "primaryTitle", "year", "genres", "averageRating", "numVotes")
        .persist()
    )

    members = wikidata.join(movies, wikidata["imdb_id"] == movies["tconst"]).drop("imdb_id")
    in_series = Window.partitionBy("series_uri")
    by_release = in_series.orderBy("year", "tconst")
    franchise = (
        members.withColumn("installments", F.count("*").over(in_series))
        .where(F.col("installments").between(2, MAX_INSTALLMENTS))
        .withColumn("n", F.row_number().over(by_release))
        .withColumn("prev_tconst", F.lag("tconst").over(by_release))
        .withColumn("prev_rating", F.lag("averageRating").over(by_release))
        .withColumn("prev_votes", F.lag("numVotes").over(by_release))
        .withColumn("prev_year", F.lag("year").over(by_release))
        .withColumn("first_rating", F.first("averageRating").over(by_release))
        # some series have no English label on Wikidata: name them after the first film
        .withColumn(
            "series_label",
            F.coalesce(
                "series_label",
                F.concat(F.first("primaryTitle").over(by_release), F.lit(" (series)")),
            ),
        )
        .persist()
    )

    ids = franchise.select("tconst").distinct()
    directors = t["title_crew"].join(ids, "tconst", "left_semi").select("tconst", "directors")
    cast = (
        t["title_principals"]
        .where(F.col("category").isin(*ACTING))
        .join(ids, "tconst", "left_semi")
        .groupBy("tconst")
        .agg(
            F.min_by("nconst", "ordering").alias("lead"),
            F.collect_set("nconst").alias("cast"),
        )
    )
    people = directors.join(cast, "tconst", "full")
    prev_people = people.select(
        F.col("tconst").alias("prev_tconst"),
        F.col("directors").alias("prev_directors"),
        F.col("lead").alias("prev_lead"),
    )
    sequels = (
        franchise.where(F.col("n") >= 2)
        .join(people, "tconst", "left")
        .join(prev_people, "prev_tconst", "left")
        .withColumn(
            "same_director",
            F.coalesce(F.size(F.array_intersect("directors", "prev_directors")) > 0, F.lit(False)),
        )
        .withColumn("lead_returns", F.coalesce(F.array_contains("cast", F.col("prev_lead")), F.lit(False)))
        .withColumn("diff", F.col("averageRating") - F.col("prev_rating"))
        .withColumn("gap", F.col("year") - F.col("prev_year"))
        .withColumn("vote_ratio", F.col("numVotes") / F.col("prev_votes"))
    )
    # a film listed in two series would count twice; one row per (film, previous film)
    pairs = sequels.dropDuplicates(["tconst", "prev_tconst"]).persist()

    # baseline: a director's next film that is not in any series
    series_ids = members.select("tconst").distinct()
    directed = (
        t["title_crew"]
        .select("tconst", F.explode("directors").alias("director"))
        .join(movies.select("tconst", "year", "averageRating"), "tconst")
    )
    by_director = Window.partitionBy("director").orderBy("year", "tconst")
    baseline = (
        directed.withColumn("prev_tconst", F.lag("tconst").over(by_director))
        .withColumn("prev_rating", F.lag("averageRating").over(by_director))
        .where(F.col("prev_tconst").isNotNull())
        .join(series_ids, "tconst", "left_anti")
        .dropDuplicates(["tconst", "prev_tconst"])
        .withColumn("diff", F.col("averageRating") - F.col("prev_rating"))
    )

    n_rows = wikidata.count()
    coverage = [
        ("wikidata rows (film, series)", n_rows),
        ("distinct series on wikidata", wikidata.select("series_uri").distinct().count()),
        (f"films matched to IMDb feature films with {MIN_VOTES}+ votes", members.select("tconst").distinct().count()),
        (f"series kept (2 to {MAX_INSTALLMENTS} matched films)", franchise.select("series_uri").distinct().count()),
        ("films in kept series", franchise.select("tconst").distinct().count()),
        ("sequel pairs (film, previous film)", pairs.count()),
        ("baseline pairs (director's next non-series film)", baseline.count()),
    ]
    save(spark.createDataFrame(coverage, "metric string, value long"), "coverage")

    save(pairs.agg(*diff_stats(), F.round(F.expr("percentile_approx(vote_ratio, 0.5)"), 3).alias("median_vote_ratio")),
         "sequel_vs_previous")

    by_n = (
        franchise.withColumn("installment", F.when(F.col("n") >= 6, "6+").otherwise(F.col("n").cast("string")))
        .withColumn("diff_first", F.col("averageRating") - F.col("first_rating"))
        .groupBy("installment")
        .agg(
            F.count("*").alias("films"),
            F.round(F.avg("averageRating"), 3).alias("avg_rating"),
            F.round(F.expr("percentile_approx(numVotes, 0.5)"), 0).alias("median_votes"),
            F.round(F.avg("diff_first"), 3).alias("mean_diff_vs_first"),
            F.round(100 * F.avg(F.when(F.col("diff_first") < 0, 1).otherwise(0)), 1).alias("pct_below_first"),
        )
        .orderBy("installment")
    )
    save(by_n, "by_installment")

    kind_rows = (
        pairs.select(F.lit("sequel").alias("kind"), "prev_rating", "diff")
        .unionByName(baseline.select(F.lit("director next film, not a sequel").alias("kind"), "prev_rating", "diff"))
        .withColumn("previous_film_rating", rating_bucket(F.col("prev_rating")))
    )
    save(
        kind_rows.groupBy("previous_film_rating", "kind").agg(*diff_stats()).orderBy("previous_film_rating", "kind"),
        "sequel_vs_baseline",
    )
    save(kind_rows.groupBy("kind").agg(*diff_stats()).orderBy("kind"), "sequel_vs_baseline_overall")

    save(
        pairs.groupBy("same_director", "lead_returns").agg(*diff_stats()).orderBy("same_director", "lead_returns"),
        "continuity",
    )
    gap_bucket = (
        F.when(F.col("gap") <= 2, "0-2 years")
        .when(F.col("gap") <= 5, "3-5 years")
        .when(F.col("gap") <= 10, "6-10 years")
        .otherwise("more than 10 years")
    )
    save(pairs.groupBy(gap_bucket.alias("gap")).agg(*diff_stats()).orderBy("gap"), "by_gap")
    save(
        pairs.groupBy(((F.col("year") / 10).cast("int") * 10).alias("decade")).agg(*diff_stats()).orderBy("decade"),
        "by_decade",
    )
    save(
        pairs.withColumn("genre", F.explode("genres"))
        .groupBy("genre").agg(*diff_stats())
        .where(F.col("pairs") >= MIN_GROUP)
        .orderBy("mean_diff"),
        "by_genre",
    )

    # franchises with 3+ films: first vs last, and where the best one sits
    best = Window.partitionBy("series_uri").orderBy(F.desc("averageRating"), "n")
    last = Window.partitionBy("series_uri").orderBy(F.desc("n"))
    franchises = (
        franchise.where(F.col("installments") >= 3)
        .withColumn("best_rank", F.row_number().over(best))
        .withColumn("last_rank", F.row_number().over(last))
        .groupBy("series_uri", "series_label", "installments")
        .agg(
            F.max(F.when(F.col("n") == 1, F.struct("primaryTitle", "year", "averageRating"))).alias("first"),
            F.max(F.when(F.col("last_rank") == 1, F.struct("primaryTitle", "year", "averageRating"))).alias("last"),
            F.max(F.when(F.col("best_rank") == 1, F.col("n"))).alias("best_installment"),
            F.round(F.avg("averageRating"), 2).alias("avg_rating"),
        )
        .select(
            "series_label",
            "installments",
            F.col("first.primaryTitle").alias("first_title"),
            F.col("first.year").alias("first_year"),
            F.col("first.averageRating").alias("first_rating"),
            F.col("last.primaryTitle").alias("last_title"),
            F.col("last.year").alias("last_year"),
            F.col("last.averageRating").alias("last_rating"),
            F.round(F.col("last.averageRating") - F.col("first.averageRating"), 2).alias("change"),
            "best_installment",
            "avg_rating",
            "series_uri",
        )
        .orderBy("change", "series_label")
    )
    save(franchises, "franchises")

    save(
        franchise.join(people.select("tconst", "lead"), "tconst", "left")
        .join(pairs.select("tconst", "prev_tconst", "same_director", "lead_returns", "diff"), ["tconst", "prev_tconst"], "left")
        .select(
            "series_label", "n", "tconst", "primaryTitle", "year", "averageRating", "numVotes",
            F.round("diff", 2).alias("diff_vs_previous"), "same_director", "lead_returns",
            "film_uri", "series_uri", "retrieved_at",
        )
        .orderBy("series_label", "series_uri", "n"),
        "franchise_films",
    )
    for df in (pairs, franchise, movies):
        df.unpersist()


def main():
    parser = argparse.ArgumentParser(description="Open question: do sequels hold up? (part 5)")
    parser.add_argument("steps", nargs="*", help="fetch, analyze; default: both")
    parser.add_argument("--refetch", action="store_true", help="download the Wikidata series again")
    args = parser.parse_args()
    steps = args.steps or ["fetch", "analyze"]
    unknown = [s for s in steps if s not in ("fetch", "analyze")]
    if unknown:
        sys.exit("unknown step: " + ", ".join(unknown))

    os.chdir(ROOT)
    started = time.time()
    if "fetch" in steps:
        print("fetch", flush=True)
        fetch(args.refetch)
    if "analyze" in steps:
        print("analyze", flush=True)
        spark = build_spark({"spark.sql.autoBroadcastJoinThreshold": str(64 * 1024 * 1024)})
        try:
            analyze(spark)
        finally:
            spark.stop()
    print(f"done in {time.time() - started:.0f}s", flush=True)


if __name__ == "__main__":
    main()
