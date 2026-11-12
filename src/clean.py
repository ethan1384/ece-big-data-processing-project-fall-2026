"""Cleaning rules for the IMDb dumps.

Three cases, handled differently on purpose:
- A missing \\N (living person, movie end year, job, characters, region, ...)
  stays null. Filling it would invent a fact.
- A value that is present but impossible is cleared. The row stays.
- A row we cannot join (broken id, duplicate key, foreign key with no parent)
  is dropped. Inside director / writer / known-for lists, only the bad id goes.
"""

import pyspark.sql.functions as F

from src.common import (
    BC_YEAR_CUTOFF,
    CURRENT_YEAR,
    MAX_END_YEAR,
    MAX_EPISODE,
    MAX_RUNTIME,
    MAX_SEASON,
    MAX_START_YEAR,
    MIN_EPISODE,
    MIN_PERSON_YEAR,
    MIN_TITLE_YEAR,
    NCONST_RE,
    SERIES_TYPES,
    TCONST_RE,
    as_double,
    as_int,
    token_array,
)


def _filter_id_array(df, key, column, pattern, valid_df, valid_col):
    """Keep ids that match `pattern` and exist in `valid_df`, in original order."""
    parts = (
        df.select(F.col(key), F.posexplode(F.split(F.col(column), ",")).alias("pos", "token"))
        .withColumn("token", F.trim(F.col("token")))
        .where(F.col("token").rlike(pattern))
    )
    valid = valid_df.select(F.col(valid_col).alias("token")).dropDuplicates(["token"])
    parts = parts.join(valid, "token", "left_semi")
    first = parts.groupBy(key, "token").agg(F.min("pos").alias("pos"))
    ordered = first.groupBy(key).agg(
        F.expr("transform(sort_array(collect_list(struct(pos, token))), x -> x.token)").alias(column)
    )
    return df.drop(column).join(ordered, key, "left")


def _between_or_null(df, name, low, high):
    value = F.col(name)
    return df.withColumn(name, F.when(value.between(low, high), value))


def clean_basics(df):
    start = as_int("startYear")
    end = as_int("endYear")
    parsed = df.select(
        "tconst",
        "titleType",
        "primaryTitle",
        "originalTitle",
        as_int("isAdult").alias("isAdult"),
        start.alias("_start"),
        end.alias("_end"),
        as_int("runtimeMinutes").alias("runtimeMinutes"),
        "genres",
    )
    parsed = parsed.withColumn("isAdult", F.when(F.col("isAdult").isin(0, 1), F.col("isAdult")))
    parsed = parsed.withColumn(
        "startYear",
        F.when(F.col("_start").between(MIN_TITLE_YEAR, MAX_START_YEAR), F.col("_start")),
    )
    # Use the cleaned start year. A garbage start (already nulled) should not
    # wipe an end year that is fine on its own. In-range inversions still drop end.
    end_ok = (
        F.col("_end").between(MIN_TITLE_YEAR, MAX_END_YEAR)
        & F.col("titleType").isin(*SERIES_TYPES)
        & (F.col("startYear").isNull() | (F.col("_end") >= F.col("startYear")))
    )
    parsed = parsed.withColumn("endYear", F.when(end_ok, F.col("_end")))
    parsed = _between_or_null(parsed, "runtimeMinutes", 1, MAX_RUNTIME)
    parsed = parsed.withColumn(
        "originalTitle",
        F.coalesce(F.col("originalTitle"), F.col("primaryTitle")),
    )
    parsed = parsed.withColumn("genres", token_array("genres"))
    parsed = parsed.where(
        F.col("tconst").rlike(TCONST_RE)
        & F.col("primaryTitle").isNotNull()
        & F.col("titleType").isNotNull()
    )
    return parsed.dropDuplicates(["tconst"]).select(
        "tconst",
        "titleType",
        "primaryTitle",
        "originalTitle",
        "isAdult",
        "startYear",
        "endYear",
        "runtimeMinutes",
        "genres",
    )


def clean_names(df, title_ids):
    parsed = df.select(
        "nconst",
        "primaryName",
        as_int("birthYear").alias("_birth"),
        as_int("deathYear").alias("_death"),
        "primaryProfession",
        "knownForTitles",
    )
    parsed = parsed.withColumn(
        "birthYear",
        F.when(F.col("_birth").between(MIN_PERSON_YEAR, CURRENT_YEAR), F.col("_birth")),
    )
    # Common-era contradiction: we cannot tell which year is wrong, so both go.
    # Older pairs (Homer, Plato, ...) are almost certainly BC years stored
    # without a sign, and those we leave alone.
    # Store the flag. If we recompute it after birthYear is cleared, the
    # comparison sees a null birth and the death year would survive.
    contradiction = (
        F.col("birthYear").isNotNull()
        & F.col("_death").isNotNull()
        & F.col("_death").between(MIN_PERSON_YEAR, CURRENT_YEAR)
        & (F.col("_death") < F.col("birthYear"))
        & (F.col("birthYear") >= F.lit(BC_YEAR_CUTOFF))
    )
    parsed = parsed.withColumn("_bad_years", contradiction)
    parsed = parsed.withColumn("birthYear", F.when(~F.col("_bad_years"), F.col("birthYear")))
    parsed = parsed.withColumn(
        "deathYear",
        F.when(
            F.col("_death").between(MIN_PERSON_YEAR, CURRENT_YEAR) & ~F.col("_bad_years"),
            F.col("_death"),
        ),
    )
    parsed = parsed.withColumn("primaryProfession", token_array("primaryProfession"))
    parsed = parsed.where(F.col("nconst").rlike(NCONST_RE) & F.col("primaryName").isNotNull())
    parsed = parsed.dropDuplicates(["nconst"]).drop("_birth", "_death", "_bad_years")
    return _filter_id_array(
        parsed, "nconst", "knownForTitles", TCONST_RE, title_ids, "tconst"
    )


def clean_ratings(df, title_ids):
    parsed = df.select(
        "tconst",
        as_double("averageRating").alias("averageRating"),
        as_int("numVotes").alias("numVotes"),
    )
    parsed = parsed.withColumn(
        "averageRating",
        F.when(F.col("averageRating").between(1.0, 10.0), F.col("averageRating")),
    )
    parsed = parsed.withColumn("numVotes", F.when(F.col("numVotes") >= 1, F.col("numVotes")))
    parsed = parsed.where(F.col("tconst").rlike(TCONST_RE) & F.col("averageRating").isNotNull())
    parsed = parsed.dropDuplicates(["tconst"])
    return parsed.join(title_ids, "tconst", "left_semi")


def clean_crew(df, title_ids, name_ids):
    parsed = df.where(F.col("tconst").rlike(TCONST_RE)).dropDuplicates(["tconst"])
    parsed = parsed.join(title_ids, "tconst", "left_semi")
    parsed = _filter_id_array(parsed, "tconst", "directors", NCONST_RE, name_ids, "nconst")
    return _filter_id_array(parsed, "tconst", "writers", NCONST_RE, name_ids, "nconst")


def clean_episodes(df, title_ids):
    parsed = df.select(
        "tconst",
        "parentTconst",
        as_int("seasonNumber").alias("seasonNumber"),
        as_int("episodeNumber").alias("episodeNumber"),
    )
    parsed = _between_or_null(parsed, "seasonNumber", 1, MAX_SEASON)
    parsed = _between_or_null(parsed, "episodeNumber", MIN_EPISODE, MAX_EPISODE)
    parsed = parsed.where(F.col("tconst").rlike(TCONST_RE) & F.col("parentTconst").rlike(TCONST_RE))
    parsed = parsed.dropDuplicates(["tconst"])
    parsed = parsed.join(title_ids, "tconst", "left_semi")
    parents = title_ids.select(F.col("tconst").alias("parentTconst"))
    return parsed.join(parents, "parentTconst", "left_semi")


def clean_principals(df, title_ids, name_ids):
    parsed = df.select(
        "tconst",
        as_int("ordering").alias("ordering"),
        "nconst",
        "category",
        "job",
        "characters",
    )
    parsed = parsed.withColumn(
        "characters",
        F.when(
            F.col("characters").isNull() | F.col("characters").rlike(r"^\[.*\]$"),
            F.col("characters"),
        ),
    )
    parsed = parsed.where(
        F.col("tconst").rlike(TCONST_RE)
        & F.col("nconst").rlike(NCONST_RE)
        & F.col("ordering").isNotNull()
        & (F.col("ordering") >= 1)
        & F.col("category").isNotNull()
    )
    parsed = parsed.dropDuplicates(["tconst", "ordering"])
    parsed = parsed.join(title_ids, "tconst", "left_semi")
    return parsed.join(name_ids, "nconst", "left_semi")


def clean_akas(df, title_ids):
    parsed = df.select(
        "titleId",
        as_int("ordering").alias("ordering"),
        "title",
        "region",
        "language",
        "types",
        "attributes",
        as_int("isOriginalTitle").alias("isOriginalTitle"),
    )
    parsed = parsed.withColumn(
        "isOriginalTitle",
        F.when(F.col("isOriginalTitle").isin(0, 1), F.col("isOriginalTitle")),
    )
    parsed = parsed.where(
        F.col("titleId").rlike(TCONST_RE)
        & F.col("ordering").isNotNull()
        & (F.col("ordering") >= 1)
        & F.col("title").isNotNull()
    )
    parsed = parsed.dropDuplicates(["titleId", "ordering"])
    parents = title_ids.select(F.col("tconst").alias("titleId"))
    return parsed.join(parents, "titleId", "left_semi")
