"""Part 2: exploration queries on the cleaned IMDb tables.

Run from the repo root, once `python -m src.prepare` has written data/clean/:

    python -m src.explore            # all eight questions
    python -m src.explore q2 q3      # only some of them

Each question writes its CSV files to output/part02/.
"""

import argparse
import csv
import os
import sys
import time
import unicodedata

import pyspark.sql.functions as F
from pyspark.sql import Window

from src.common import SERIES_TYPES
from src.prepare import ROOT, build_spark

CLEAN_DIR = os.path.join(ROOT, "data", "clean")
OUT_DIR = os.path.join(ROOT, "output", "part02")

# Feature film: titleType movie, 40 minutes or more (AMPAS / BFI rule), not adult.
# A movie with no runtime cannot be called feature-length, so it is left out.
FEATURE_MIN_RUNTIME = 40
# Below this many votes a rating is noise (a 9.4 from 12 votes).
MIN_VOTES = 100

FIRST_YEAR = 1980
LAST_YEAR = 2025

# q1: z-score of log(runtime) inside each genre. Runtimes are skewed to the
# right, so a plain z-score can never flag a short film (mean - 3 sd < 40 min).
Q1_MIN_Z = 3.0
Q1_MIN_GENRE_SIZE = 30

Q4_HIT = 8.0
Q4_FLOP_AVG = 6.0

Q5_MIN_SEASONS = 3
Q5_MIN_EPISODES = 10
Q5_MIN_EPISODE_VOTES = 20

# q6: a "successful" film
HIT_RATING = 7.0
HIT_VOTES = 1000

Q7_MIN_SPAN = 20
Q7_MIN_MOVIES = 15
# best-rated movie inside the first third of the filmography (by movie count)
Q7_EARLY_SHARE = 1 / 3
# first half of the movies must beat the second half by at least this much
Q7_MIN_DROP = 0.5

# q8: a role counts for a person once they have this many movies in it
Q8_MIN_CREDITS = 3

ACTING = ("actor", "actress")

# Language of an aka row that has a region but no language. Countries with
# two or more main languages (IN, CA, BE, CH) are left out on purpose.
REGION_LANGUAGE = {
    "US": "en", "GB": "en", "AU": "en", "NZ": "en", "IE": "en",
    "FR": "fr", "DE": "de", "AT": "de", "IT": "it",
    "ES": "es", "MX": "es", "AR": "es", "CO": "es", "CL": "es", "PE": "es",
    "VE": "es", "UY": "es", "CU": "es",
    "PT": "pt", "BR": "pt", "JP": "ja", "KR": "ko",
    "CN": "cmn", "TW": "cmn", "HK": "yue",
    "RU": "ru", "UA": "uk", "SE": "sv", "NO": "no", "DK": "da", "FI": "fi",
    "IS": "is", "NL": "nl", "PL": "pl", "CZ": "cs", "HU": "hu", "RO": "ro",
    "BG": "bg", "RS": "sr", "HR": "hr", "GR": "el", "TR": "tr", "IR": "fa",
    "IL": "he", "EG": "ar", "TH": "th", "ID": "id",
}


def _accent_map():
    """Lowercase Latin letters with a diacritic -> plain letter, for F.translate."""
    special = {"ł": "l", "ø": "o", "đ": "d", "ı": "i", "ß": "s", "æ": "a", "œ": "o", "ħ": "h", "ŧ": "t"}
    src, dst = [], []
    for code in range(0xC0, 0x250):
        ch = chr(code)
        if ch != ch.lower():
            continue
        base = special.get(ch) or unicodedata.normalize("NFKD", ch)[0]
        if base != ch and base.isascii() and base.isalpha():
            src.append(ch)
            dst.append(base)
    return "".join(src), "".join(dst)


ACCENTS_FROM, ACCENTS_TO = _accent_map()


def norm_title(name):
    """IMDb drops some accents in originalTitle (Róza vs Róża), so compare loosely."""
    folded = F.translate(F.lower(F.trim(F.col(name))), ACCENTS_FROM, ACCENTS_TO)
    return F.regexp_replace(folded, r"[^\p{L}\p{N}]", "")


def load_tables(spark):
    tables = {}
    for name in (
        "title_basics",
        "title_ratings",
        "title_crew",
        "title_episode",
        "title_principals",
        "title_akas",
        "name_basics",
    ):
        path = os.path.join(CLEAN_DIR, name)
        if not os.path.isdir(path):
            raise SystemExit(f"{path} not found, run: python -m src.prepare")
        tables[name] = spark.read.parquet(path)
    return tables


def feature_movies(t):
    return t["title_basics"].where(
        (F.col("titleType") == "movie")
        & (F.col("isAdult") == 0)
        & (F.col("runtimeMinutes") >= FEATURE_MIN_RUNTIME)
        & F.col("startYear").isNotNull()
    ).select(
        "tconst",
        "primaryTitle",
        F.col("startYear").alias("year"),
        "runtimeMinutes",
        "genres",
    )


def rated_movies(t, min_votes=MIN_VOTES):
    ratings = t["title_ratings"].where(F.col("numVotes") >= min_votes)
    return feature_movies(t).join(ratings, "tconst")


def actors(t):
    return (
        t["title_principals"]
        .where(F.col("category").isin(*ACTING))
        .select("tconst", "nconst")
        .dropDuplicates()
    )


def names(t):
    return t["name_basics"].select("nconst", "primaryName")


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


# --- q1 ---------------------------------------------------------------------

def q1_runtime_outliers(t):
    movies = (
        feature_movies(t)
        .withColumn("genre", F.explode("genres"))
        .withColumn("log_rt", F.log("runtimeMinutes"))
    )
    stats = (
        movies.groupBy("genre")
        .agg(
            F.count("*").alias("movies"),
            F.avg("runtimeMinutes").alias("avg_runtime"),
            F.expr("percentile_approx(runtimeMinutes, 0.5)").alias("median_runtime"),
            F.avg("log_rt").alias("log_mean"),
            F.stddev_samp("log_rt").alias("log_sd"),
        )
        .where(F.col("movies") >= Q1_MIN_GENRE_SIZE)
    )
    outliers = (
        movies.join(stats, "genre")
        .withColumn("z", (F.col("log_rt") - F.col("log_mean")) / F.col("log_sd"))
        .where(F.abs(F.col("z")) >= Q1_MIN_Z)
        .withColumn("direction", F.when(F.col("z") > 0, "longer").otherwise("shorter"))
        .persist()
    )

    save(
        outliers.select(
            "genre",
            "tconst",
            "primaryTitle",
            "year",
            "runtimeMinutes",
            F.round("avg_runtime", 1).alias("genre_avg_runtime"),
            F.round(100 * (F.col("runtimeMinutes") / F.col("avg_runtime") - 1), 1).alias("pct_vs_avg"),
            F.round("z", 2).alias("z"),
            "direction",
        ).orderBy("genre", F.desc(F.abs(F.col("z"))), "tconst"),
        "q1_runtime_outliers",
    )

    counts = outliers.groupBy("genre").agg(
        F.sum(F.when(F.col("direction") == "shorter", 1).otherwise(0)).alias("shorter"),
        F.sum(F.when(F.col("direction") == "longer", 1).otherwise(0)).alias("longer"),
    )
    summary = (
        stats.join(counts, "genre", "left")
        .fillna(0, ["shorter", "longer"])
        .select(
            "genre",
            "movies",
            F.round("avg_runtime", 1).alias("avg_runtime"),
            "median_runtime",
            # runtimes outside [short_cutoff, long_cutoff] are flagged
            F.round(F.exp(F.col("log_mean") - Q1_MIN_Z * F.col("log_sd")), 0).alias("short_cutoff"),
            F.round(F.exp(F.col("log_mean") + Q1_MIN_Z * F.col("log_sd")), 0).alias("long_cutoff"),
            "shorter",
            "longer",
        )
        .orderBy("genre")
    )
    save(summary, "q1_genre_runtime_summary")
    outliers.unpersist()


# --- q2 / q3 ----------------------------------------------------------------

def genre_year_ranked(t):
    """Votes per (year, genre), ranked inside each year. Popularity = total votes."""
    movies = feature_movies(t).where(F.col("year").between(FIRST_YEAR, LAST_YEAR))
    per_genre = (
        movies.join(t["title_ratings"], "tconst")
        .withColumn("genre", F.explode("genres"))
        .groupBy("year", "genre")
        .agg(
            F.sum(F.coalesce(F.col("numVotes"), F.lit(0))).alias("votes"),
            F.count("*").alias("movies"),
            F.sum("averageRating").alias("rating_sum"),
        )
    )
    by_rank = Window.partitionBy("year").orderBy(F.desc("votes"), F.desc("movies"), "genre")
    year = Window.partitionBy("year")
    return (
        per_genre.withColumn("rank", F.row_number().over(by_rank))
        .withColumn("year_votes", F.sum("votes").over(year))
        .withColumn("genres_in_year", F.count("*").over(year))
    )


def q2_top_genres(t):
    ranked = genre_year_ranked(t)
    save(
        ranked.where(F.col("rank") <= 3)
        .select(
            "year",
            "rank",
            "genre",
            "votes",
            "movies",
            F.round(100 * F.col("votes") / F.col("year_votes"), 2).alias("vote_share_pct"),
            F.round(F.col("rating_sum") / F.col("movies"), 2).alias("avg_rating"),
        )
        .orderBy("year", "rank"),
        "q2_top3_genres_by_year",
    )


def q3_top_vs_rest(t):
    ranked = genre_year_ranked(t).persist()
    top = F.col("rank") <= 3

    def part(condition, column):
        return F.sum(F.when(condition, F.col(column)).otherwise(0))

    # A movie with three genres is counted once in each, so "tags" here are
    # (movie, genre) pairs, not distinct movies.
    per_year = ranked.groupBy("year").agg(
        F.first("genres_in_year").alias("genres"),
        part(top, "votes").alias("top3_votes"),
        part(~top, "votes").alias("rest_votes"),
        part(top, "movies").alias("top3_tags"),
        part(~top, "movies").alias("rest_tags"),
        part(top, "rating_sum").alias("top3_rating_sum"),
        part(~top, "rating_sum").alias("rest_rating_sum"),
        F.max(F.when(F.col("rank") == 3, F.col("votes"))).alias("third_votes"),
        F.max(F.when(F.col("rank") == 4, F.col("votes"))).alias("fourth_votes"),
    )
    total = F.col("top3_votes") + F.col("rest_votes")
    save(
        per_year.select(
            "year",
            "genres",
            F.round(100 * F.col("top3_votes") / total, 1).alias("top3_vote_share_pct"),
            F.round(100 * F.col("top3_tags") / (F.col("top3_tags") + F.col("rest_tags")), 1).alias(
                "top3_tag_share_pct"
            ),
            F.round(F.col("top3_votes") / 3, 0).alias("votes_per_top3_genre"),
            F.round(F.col("rest_votes") / (F.col("genres") - 3), 0).alias("votes_per_other_genre"),
            F.round(F.col("top3_votes") / F.col("top3_tags"), 0).alias("top3_votes_per_movie"),
            F.round(F.col("rest_votes") / F.col("rest_tags"), 0).alias("rest_votes_per_movie"),
            F.round(F.col("top3_rating_sum") / F.col("top3_tags"), 2).alias("top3_avg_rating"),
            F.round(F.col("rest_rating_sum") / F.col("rest_tags"), 2).alias("rest_avg_rating"),
            F.round(F.col("third_votes") / F.col("fourth_votes"), 2).alias("third_vs_fourth"),
        ).orderBy("year"),
        "q3_top3_vs_rest_by_year",
    )

    save(
        ranked.where(top)
        .groupBy("genre")
        .agg(
            F.count("*").alias("years_in_top3"),
            F.sum(F.when(F.col("rank") == 1, 1).otherwise(0)).alias("years_ranked_1"),
            F.min("year").alias("first_year"),
            F.max("year").alias("last_year"),
        )
        .orderBy(F.desc("years_in_top3"), "genre"),
        "q3_genre_top3_frequency",
    )
    ranked.unpersist()


# --- q4 ---------------------------------------------------------------------

def q4_one_hit_directors(t):
    directed = (
        t["title_crew"]
        .select("tconst", F.explode("directors").alias("nconst"))
        .join(rated_movies(t), "tconst")
        .dropDuplicates(["nconst", "tconst"])
    )
    hit = F.col("averageRating") >= Q4_HIT
    per_director = directed.groupBy("nconst").agg(
        F.sum(F.when(hit, 1).otherwise(0)).alias("hits"),
        F.max(F.when(hit, F.struct("numVotes", "primaryTitle", "year", "averageRating"))).alias("hit"),
        F.sum(F.when(~hit, 1).otherwise(0)).alias("other_movies"),
        F.avg(F.when(~hit, F.col("averageRating"))).alias("other_avg"),
        F.max(F.when(~hit, F.col("averageRating"))).alias("other_max"),
    )
    found = per_director.where(
        (F.col("hits") == 1) & (F.col("other_movies") >= 2) & (F.col("other_avg") <= Q4_FLOP_AVG)
    )
    save(
        found.join(names(t), "nconst", "left")
        .select(
            "nconst",
            "primaryName",
            F.col("hit.primaryTitle").alias("hit_title"),
            F.col("hit.year").alias("hit_year"),
            F.col("hit.averageRating").alias("hit_rating"),
            F.col("hit.numVotes").alias("hit_votes"),
            "other_movies",
            F.round("other_avg", 2).alias("other_avg_rating"),
            F.col("other_max").alias("other_best_rating"),
        )
        .orderBy(F.desc("hit_votes"), "nconst"),
        "q4_one_hit_directors",
    )


# --- q5 ---------------------------------------------------------------------

def q5_declining_series(t):
    series = t["title_basics"].where(F.col("titleType").isin(*SERIES_TYPES)).select(
        F.col("tconst").alias("parentTconst"),
        F.col("primaryTitle").alias("series"),
        F.col("startYear").alias("series_start"),
    )
    episodes = (
        t["title_episode"]
        .where(F.col("seasonNumber").isNotNull())
        .join(series.select("parentTconst"), "parentTconst", "left_semi")
    )
    # Season size counts every listed episode, rated or not.
    shape = (
        episodes.groupBy("parentTconst", "seasonNumber")
        .agg(F.count("*").alias("episodes"))
        .groupBy("parentTconst")
        .agg(
            F.count("*").alias("seasons"),
            F.min("episodes").alias("min_episodes"),
            F.sort_array(F.collect_list("seasonNumber")).alias("season_list"),
        )
        .where((F.col("seasons") >= Q5_MIN_SEASONS) & (F.col("min_episodes") >= Q5_MIN_EPISODES))
        .select(
            "parentTconst",
            "seasons",
            "min_episodes",
            F.col("season_list")[1].alias("second_season"),
            F.element_at("season_list", -1).alias("last_season"),
        )
    )
    ratings = t["title_ratings"].where(F.col("numVotes") >= Q5_MIN_EPISODE_VOTES)
    rated = episodes.join(shape, "parentTconst").join(ratings, "tconst").persist()

    # Strict comparisons: a tie between the last season and an earlier one
    # does not count as "the worst episode is in the last season".
    last = F.col("seasonNumber") == F.col("last_season")
    early = F.col("seasonNumber") <= F.col("second_season")
    rating = F.col("averageRating")
    keep = (
        rated.groupBy("parentTconst")
        .agg(
            F.min(F.when(last, rating)).alias("worst_last"),
            F.min(F.when(~last, rating)).alias("worst_before"),
            F.max(F.when(early, rating)).alias("best_first_two"),
            F.max(F.when(~early, rating)).alias("best_after"),
        )
        .where(
            (F.col("worst_last") < F.col("worst_before"))
            & (F.col("best_first_two") > F.col("best_after"))
        )
        .select("parentTconst")
    )

    # One episode to show for each side (most voted when several share the rating).
    titles = t["title_basics"].select("tconst", F.col("primaryTitle").alias("episode"))
    picked = rated.join(keep, "parentTconst", "left_semi").join(titles, "tconst", "left")

    def pick(order, prefix):
        w = Window.partitionBy("parentTconst").orderBy(order, F.desc("numVotes"), "tconst")
        return (
            picked.withColumn("rn", F.row_number().over(w))
            .where(F.col("rn") == 1)
            .select(
                "parentTconst",
                F.col("episode").alias(prefix + "_episode"),
                F.col("seasonNumber").alias(prefix + "_season"),
                F.col("episodeNumber").alias(prefix + "_number"),
                F.col("averageRating").alias(prefix + "_rating"),
            )
        )

    out = (
        keep.join(series, "parentTconst")
        .join(shape, "parentTconst")
        .join(pick(F.desc("averageRating"), "best"), "parentTconst")
        .join(pick(F.asc("averageRating"), "worst"), "parentTconst")
        .select(
            F.col("parentTconst").alias("tconst"),
            "series",
            "series_start",
            "seasons",
            "min_episodes",
            "best_episode",
            "best_season",
            "best_number",
            "best_rating",
            "worst_episode",
            "worst_season",
            "worst_number",
            "worst_rating",
        )
        .orderBy(F.desc(F.col("best_rating") - F.col("worst_rating")), "tconst")
    )
    save(out, "q5_series_best_early_worst_last")
    rated.unpersist()


# --- q6 ---------------------------------------------------------------------

def title_languages(t, movie_ids):
    """Candidate original languages per movie.

    An aka row is a candidate when its title is the original title. Many
    countries keep the original title too (Kler is also "Kler" in GB, AU, IE),
    so a movie usually has several candidates. Pseudo-regions (XWW, XEU, ...)
    are skipped.
    """
    region_lang = F.create_map(*[F.lit(x) for pair in REGION_LANGUAGE.items() for x in pair])
    originals = (
        t["title_basics"]
        .join(movie_ids, "tconst", "left_semi")
        .select("tconst", norm_title("originalTitle").alias("orig"))
        .where(F.col("orig") != "")
    )
    return (
        t["title_akas"]
        .where(F.col("region").isNotNull() & ~F.col("region").startswith("X"))
        .select(
            F.col("titleId").alias("tconst"),
            norm_title("title").alias("aka"),
            F.lower(F.trim("language")).alias("language"),
            "region",
        )
        .join(originals, "tconst")
        .where(F.col("aka") == F.col("orig"))
        .select("tconst", F.coalesce(F.col("language"), region_lang[F.col("region")]).alias("lang"), "region")
        .where(F.col("lang").isNotNull())
        .groupBy("tconst", "lang")
        .agg(F.countDistinct("region").alias("regions"))
    )


def original_language(t, movie_ids, candidates):
    """Pick one candidate language per movie, using the director's other movies.

    The home country shows up on almost every movie of a director, the
    countries that kept the title change from one movie to the next. So the
    candidate found on the most movies of the same director(s) wins.
    """
    directed = t["title_crew"].select("tconst", F.explode("directors").alias("nconst"))
    director_lang = (
        candidates.join(directed, "tconst")
        .groupBy("nconst", "lang")
        .agg(F.countDistinct("tconst").alias("director_movies"))
    )
    scored = (
        candidates.join(movie_ids, "tconst", "left_semi")
        .join(directed, "tconst", "left")
        .join(director_lang, ["nconst", "lang"], "left")
        .groupBy("tconst", "lang", "regions")
        .agg(F.coalesce(F.sum("director_movies"), F.lit(0)).alias("score"))
    )
    w = Window.partitionBy("tconst").orderBy(F.desc("score"), F.desc("regions"), "lang")
    return (
        scored.withColumn("rn", F.row_number().over(w))
        .where(F.col("rn") == 1)
        .select("tconst", F.col("lang").alias("language"))
    )


def q6_multi_language_actors(t):
    hits = rated_movies(t, HIT_VOTES).where(F.col("averageRating") >= HIT_RATING).select("tconst")
    hits = hits.persist()
    candidates = title_languages(t, feature_movies(t).select("tconst")).persist()
    languages = original_language(t, hits, candidates).persist()

    total = hits.count()
    known = languages.count()
    candidates.unpersist()
    spark = hits.sparkSession
    save(
        spark.createDataFrame(
            [
                ("successful movies", total),
                ("original language found", known),
                ("language unknown (left out)", total - known),
            ],
            ["what", "movies"],
        ),
        "q6_language_coverage",
    )
    save(
        languages.groupBy("language").agg(F.count("*").alias("hit_movies")).orderBy(F.desc("hit_movies")),
        "q6_hit_movies_by_language",
    )

    per_actor = (
        actors(t)
        .join(hits, "tconst", "left_semi")
        .join(languages, "tconst")
        .groupBy("nconst")
        .agg(
            F.countDistinct("language").alias("languages"),
            F.sort_array(F.collect_set("language")).alias("language_list"),
            F.countDistinct("tconst").alias("hit_movies"),
        )
        .where(F.col("languages") >= 2)
        .join(names(t), "nconst", "left")
        .select("nconst", "primaryName", "languages", "language_list", "hit_movies")
        .persist()
    )
    order = [F.desc("languages"), F.desc("hit_movies"), "nconst"]
    save(per_actor.orderBy(*order), "q6_hits_in_2plus_languages")
    save(per_actor.where(F.col("languages") >= 3).orderBy(*order), "q6_hits_in_3plus_languages")
    save(
        per_actor.groupBy("languages").agg(F.count("*").alias("actors")).orderBy("languages"),
        "q6_actors_by_language_count",
    )
    per_actor.unpersist()
    languages.unpersist()
    hits.unpersist()


# --- q7 ---------------------------------------------------------------------

def q7_early_peak(t):
    rated = rated_movies(t).select("tconst", "primaryTitle", "year", "averageRating", "numVotes")
    person = Window.partitionBy("nconst")
    in_order = Window.partitionBy("nconst").orderBy("year", "tconst")
    # Early / late is split on the movie count, not on the calendar: with a
    # calendar split, one stray late movie becomes the whole "late career".
    acted = (
        actors(t)
        .join(rated, "tconst")
        .withColumn("first_year", F.min("year").over(person))
        .withColumn("last_year", F.max("year").over(person))
        .withColumn("movies", F.count("*").over(person))
        .where(
            (F.col("last_year") - F.col("first_year") >= Q7_MIN_SPAN)
            & (F.col("movies") >= Q7_MIN_MOVIES)
        )
        .withColumn("pos", F.row_number().over(in_order))
        .withColumn("early", F.col("pos") <= F.col("movies") / 2)
        .persist()
    )

    best_order = Window.partitionBy("nconst").orderBy(
        F.desc("averageRating"), F.desc("numVotes"), "year", "tconst"
    )
    best = (
        acted.withColumn("rn", F.row_number().over(best_order))
        .where(F.col("rn") == 1)
        .select(
            "nconst",
            F.col("primaryTitle").alias("best_movie"),
            F.col("year").alias("best_year"),
            F.col("averageRating").alias("best_rating"),
            F.col("pos").alias("best_pos"),
        )
    )
    early = F.col("early")
    rating = F.col("averageRating")
    careers = acted.groupBy("nconst", "first_year", "last_year", "movies").agg(
        F.sum(F.when(early, 1).otherwise(0)).alias("early_movies"),
        F.avg(F.when(early, rating)).alias("early_avg"),
        F.sum(F.when(~early, 1).otherwise(0)).alias("late_movies"),
        F.avg(F.when(~early, rating)).alias("late_avg"),
    )
    out = (
        careers.join(best, "nconst")
        .withColumn("peak_position", (F.col("best_pos") - 1) / (F.col("movies") - 1))
        .withColumn("drop", F.col("early_avg") - F.col("late_avg"))
        .where((F.col("peak_position") <= Q7_EARLY_SHARE) & (F.col("drop") >= Q7_MIN_DROP))
        .join(names(t), "nconst", "left")
        .select(
            "nconst",
            "primaryName",
            "first_year",
            "last_year",
            "movies",
            "best_movie",
            "best_year",
            "best_rating",
            F.round("peak_position", 2).alias("peak_position"),
            "early_movies",
            F.round("early_avg", 2).alias("early_avg"),
            "late_movies",
            F.round("late_avg", 2).alias("late_avg"),
            F.round("drop", 2).alias("drop"),
        )
        .orderBy(F.desc("drop"), "nconst")
    )
    save(out, "q7_early_peak_actors")
    acted.unpersist()


# --- q8 ---------------------------------------------------------------------

def q8_multi_talent(t):
    movie_ids = feature_movies(t).select("tconst")
    crew = t["title_crew"]
    principals = t["title_principals"].where(F.col("category").isin("director", "writer", *ACTING))
    credits = (
        crew.select("tconst", F.explode("directors").alias("nconst"), F.lit("director").alias("role"))
        .unionByName(
            crew.select("tconst", F.explode("writers").alias("nconst"), F.lit("writer").alias("role"))
        )
        .unionByName(
            principals.select(
                "tconst",
                "nconst",
                F.when(F.col("category").isin(*ACTING), "actor").otherwise(F.col("category")).alias("role"),
            )
        )
        .join(movie_ids, "tconst", "left_semi")
        .dropDuplicates()
        .persist()
    )

    talent = (
        credits.groupBy("nconst", "role")
        .agg(F.countDistinct("tconst").alias("n"))
        .where(F.col("n") >= Q8_MIN_CREDITS)
        .groupBy("nconst")
        .agg(F.sort_array(F.collect_list("role")).alias("roles"))
        .where(F.size("roles") >= 2)
        .withColumn("roles", F.array_join("roles", "+"))
        .persist()
    )
    save(
        talent.groupBy("roles").agg(F.count("*").alias("people")).orderBy(F.desc("people")),
        "q8_multi_talent_counts",
    )

    ratings = t["title_ratings"].where(F.col("numVotes") >= MIN_VOTES)
    per_movie = (
        credits.join(talent.select("nconst"), "nconst", "left_semi")
        .groupBy("nconst", "tconst")
        .agg(F.sort_array(F.collect_set("role")).alias("movie_roles"))
        .withColumn("n_roles", F.size("movie_roles"))
        .join(ratings, "tconst")
        .persist()
    )
    both = F.col("n_roles") >= 2
    rating = F.col("averageRating")
    people = (
        per_movie.groupBy("nconst")
        .agg(
            F.sum(F.when(both, 1).otherwise(0)).alias("combined_movies"),
            F.avg(F.when(both, rating)).alias("combined_avg"),
            F.avg(F.when(both, F.col("numVotes"))).alias("combined_avg_votes"),
            F.sum(F.when(~both, 1).otherwise(0)).alias("single_movies"),
            F.avg(F.when(~both, rating)).alias("single_avg"),
            F.avg(F.when(~both, F.col("numVotes"))).alias("single_avg_votes"),
        )
        .where((F.col("combined_movies") >= 1) & (F.col("single_movies") >= 1))
        .persist()
    )
    save(
        people.join(talent, "nconst")
        .join(names(t), "nconst", "left")
        .select(
            "nconst",
            "primaryName",
            "roles",
            "combined_movies",
            F.round("combined_avg", 2).alias("combined_avg"),
            "single_movies",
            F.round("single_avg", 2).alias("single_avg"),
            F.round(F.col("combined_avg") - F.col("single_avg"), 2).alias("diff"),
            F.round("combined_avg_votes", 0).alias("combined_avg_votes"),
            F.round("single_avg_votes", 0).alias("single_avg_votes"),
        )
        .orderBy(F.desc("combined_movies"), "nconst"),
        "q8_multi_talent_people",
    )

    # Same comparison, split by which roles were held on the same movie.
    # Each person is compared with their own single-role average.
    by_combo = (
        per_movie.where(both)
        .withColumn("combo", F.array_join("movie_roles", "+"))
        .groupBy("nconst", "combo")
        .agg(F.count("*").alias("movies"), F.avg("averageRating").alias("combo_avg"))
        .join(people.select("nconst", "single_avg"), "nconst")
        .withColumn("diff", F.col("combo_avg") - F.col("single_avg"))
    )
    overall = people.select(
        "nconst",
        F.lit("any").alias("combo"),
        F.col("combined_movies").alias("movies"),
        F.col("combined_avg").alias("combo_avg"),
        "single_avg",
        (F.col("combined_avg") - F.col("single_avg")).alias("diff"),
    )
    save(
        by_combo.unionByName(overall)
        .groupBy("combo")
        .agg(
            F.count("*").alias("people"),
            F.sum("movies").alias("movies"),
            F.round(F.avg("combo_avg"), 2).alias("avg_rating_combined"),
            F.round(F.avg("single_avg"), 2).alias("avg_rating_single"),
            F.round(F.avg("diff"), 3).alias("mean_diff"),
            F.round(F.expr("percentile_approx(diff, 0.5)"), 3).alias("median_diff"),
            F.round(100 * F.avg(F.when(F.col("diff") > 0, 1).otherwise(0)), 1).alias("pct_better_combined"),
        )
        .orderBy(F.desc("people")),
        "q8_combined_vs_single_by_roles",
    )
    for df in (people, per_movie, talent, credits):
        df.unpersist()


QUESTIONS = {
    "q1": q1_runtime_outliers,
    "q2": q2_top_genres,
    "q3": q3_top_vs_rest,
    "q4": q4_one_hit_directors,
    "q5": q5_declining_series,
    "q6": q6_multi_language_actors,
    "q7": q7_early_peak,
    "q8": q8_multi_talent,
}


def main():
    parser = argparse.ArgumentParser(description="IMDb exploration queries (part 2)")
    parser.add_argument("questions", nargs="*", help="q1 ... q8, default: all")
    args = parser.parse_args()
    wanted = args.questions or list(QUESTIONS)
    unknown = [q for q in wanted if q not in QUESTIONS]
    if unknown:
        sys.exit("unknown question: " + ", ".join(unknown))

    os.chdir(ROOT)
    started = time.time()
    spark = build_spark()
    # prepare.py turns broadcast joins off for the big cleaning joins.
    # Here most joins hit small filtered tables, so let Spark broadcast them.
    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", str(64 * 1024 * 1024))
    try:
        tables = load_tables(spark)
        for key in wanted:
            print(key, flush=True)
            QUESTIONS[key](tables)
    finally:
        spark.stop()
    print(f"done in {time.time() - started:.0f}s", flush=True)


if __name__ == "__main__":
    main()
