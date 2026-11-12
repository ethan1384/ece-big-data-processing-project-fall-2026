"""Quality checks on the raw (normalized) IMDb tables.

Null percentages are computed separately, before any row is dropped.
The counts here are the other problems: impossible values and broken keys.
"""

import pyspark.sql.functions as F

from src.common import (
    BC_YEAR_CUTOFF,
    CURRENT_YEAR,
    MAX_AGE,
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
    count_when,
    present_but_not_double,
    present_but_not_int,
)


def _issue(check, table, affected, unit, handling):
    return {
        "check": check,
        "table": table,
        "affected": int(affected or 0),
        "unit": unit,
        "handling": handling,
    }


def _counts(df, named):
    row = df.agg(
        *[count_when(condition).alias(name) for name, condition in named]
    ).collect()[0]
    return {name: int(row[name] or 0) for name, _ in named}


def _extra_duplicates(df, keys, patterns):
    scoped = df
    for key, pattern in zip(keys, patterns):
        scoped = scoped.where(F.col(key).rlike(pattern))
    grouped = scoped.groupBy(*keys).count()
    extra = (
        grouped.where(F.col("count") > 1)
        .select(F.sum(F.col("count") - F.lit(1)))
        .collect()[0][0]
    )
    return int(extra or 0)


def _broken_ids(df, column, pattern, valid_df, valid_col):
    tokens = (
        df.select(F.explode(F.split(F.col(column), ",")).alias("token"))
        .withColumn("token", F.trim(F.col("token")))
        .where(F.col("token").isNotNull() & (F.col("token") != ""))
        .persist()
    )
    malformed = tokens.where(~F.col("token").rlike(pattern)).count()
    dangling = (
        tokens.where(F.col("token").rlike(pattern))
        .join(valid_df.select(F.col(valid_col).alias("token")), "token", "left_anti")
        .count()
    )
    tokens.unpersist()
    return int(malformed), int(dangling)


def _out_of_range(parsed, low, high):
    return parsed.isNotNull() & ~parsed.between(low, high)


def find_issues(frames):
    basics = frames["title.basics"]
    names = frames["name.basics"]
    ratings = frames["title.ratings"]
    crew = frames["title.crew"]
    episodes = frames["title.episode"]
    principals = frames["title.principals"]
    akas = frames["title.akas"]

    print("index title and name ids", flush=True)
    titles = (
        basics.where(F.col("tconst").rlike(TCONST_RE))
        .select("tconst", "titleType")
        .dropDuplicates(["tconst"])
        .persist()
    )
    people = (
        names.where(F.col("nconst").rlike(NCONST_RE))
        .select("nconst")
        .dropDuplicates(["nconst"])
        .persist()
    )
    titles.count()
    people.count()

    issues = []
    ranges = []

    print("check title.basics", flush=True)
    start_i = as_int("startYear")
    end_i = as_int("endYear")
    run_i = as_int("runtimeMinutes")
    genre_n = F.expr(
        "size(filter(transform(split(genres, ','), x -> trim(x)), x -> x <> ''))"
    )
    basics_x = basics.select(
        "*",
        start_i.alias("start_i"),
        end_i.alias("end_i"),
        run_i.alias("run_i"),
    )
    start_ok = F.col("start_i").between(MIN_TITLE_YEAR, MAX_START_YEAR)
    end_ok = F.col("end_i").between(MIN_TITLE_YEAR, MAX_END_YEAR)
    counted = _counts(
        basics_x,
        [
            ("bad_id", F.col("tconst").isNull() | ~F.col("tconst").rlike(TCONST_RE)),
            ("null_title", F.col("primaryTitle").isNull()),
            ("null_type", F.col("titleType").isNull()),
            (
                "bad_adult",
                F.col("isAdult").isNotNull() & ~F.col("isAdult").isin("0", "1"),
            ),
            ("start_nan", present_but_not_int("startYear")),
            ("start_range", _out_of_range(F.col("start_i"), MIN_TITLE_YEAR, MAX_START_YEAR)),
            ("end_nan", present_but_not_int("endYear")),
            ("end_range", _out_of_range(F.col("end_i"), MIN_TITLE_YEAR, MAX_END_YEAR)),
            ("end_before_start", start_ok & end_ok & (F.col("end_i") < F.col("start_i"))),
            (
                "end_on_nonseries",
                F.col("end_i").isNotNull()
                & (F.col("titleType").isNull() | ~F.col("titleType").isin(*SERIES_TYPES)),
            ),
            ("run_nan", present_but_not_int("runtimeMinutes")),
            ("run_range", _out_of_range(F.col("run_i"), 1, MAX_RUNTIME)),
            ("genres_long", F.col("genres").isNotNull() & (genre_n > 3)),
        ],
    )
    span = basics_x.agg(
        F.min("start_i").alias("start_min"),
        F.max("start_i").alias("start_max"),
        F.min("run_i").alias("run_min"),
        F.max("run_i").alias("run_max"),
    ).collect()[0]
    ranges.append(_range("title.basics", "startYear", span["start_min"], span["start_max"]))
    ranges.append(_range("title.basics", "runtimeMinutes", span["run_min"], span["run_max"]))

    issues.append(_issue("null or malformed tconst", "title.basics", counted["bad_id"], "rows", "drop row"))
    issues.append(_issue("null primaryTitle", "title.basics", counted["null_title"], "rows", "drop row"))
    issues.append(_issue("null titleType", "title.basics", counted["null_type"], "rows", "drop row"))
    issues.append(
        _issue(
            "duplicate tconst",
            "title.basics",
            _extra_duplicates(basics, ["tconst"], [TCONST_RE]),
            "extra rows",
            "keep one row",
        )
    )
    issues.append(_issue("isAdult other than 0 or 1", "title.basics", counted["bad_adult"], "rows", "set isAdult to null"))
    issues.append(_issue("startYear not an integer", "title.basics", counted["start_nan"], "rows", "set startYear to null"))
    issues.append(
        _issue(
            f"startYear outside {MIN_TITLE_YEAR}-{MAX_START_YEAR}",
            "title.basics",
            counted["start_range"],
            "rows",
            "set startYear to null",
        )
    )
    issues.append(_issue("endYear not an integer", "title.basics", counted["end_nan"], "rows", "set endYear to null"))
    issues.append(
        _issue(
            f"endYear outside {MIN_TITLE_YEAR}-{MAX_END_YEAR}",
            "title.basics",
            counted["end_range"],
            "rows",
            "set endYear to null",
        )
    )
    issues.append(
        _issue(
            "endYear before startYear",
            "title.basics",
            counted["end_before_start"],
            "rows",
            "set endYear to null",
        )
    )
    issues.append(
        _issue(
            "endYear set on a title that is not a series",
            "title.basics",
            counted["end_on_nonseries"],
            "rows",
            "set endYear to null",
        )
    )
    issues.append(_issue("runtimeMinutes not an integer", "title.basics", counted["run_nan"], "rows", "set runtimeMinutes to null"))
    issues.append(
        _issue(
            f"runtimeMinutes outside 1-{MAX_RUNTIME}",
            "title.basics",
            counted["run_range"],
            "rows",
            "set runtimeMinutes to null",
        )
    )
    issues.append(
        _issue(
            "more than 3 genres",
            "title.basics",
            counted["genres_long"],
            "rows",
            "kept (do not truncate a real list)",
        )
    )

    print("check name.basics", flush=True)
    birth_i = as_int("birthYear")
    death_i = as_int("deathYear")
    names_x = names.select("*", birth_i.alias("birth_i"), death_i.alias("death_i"))
    birth_ok = F.col("birth_i").between(MIN_PERSON_YEAR, CURRENT_YEAR)
    death_ok = F.col("death_i").between(MIN_PERSON_YEAR, CURRENT_YEAR)
    counted = _counts(
        names_x,
        [
            ("bad_id", F.col("nconst").isNull() | ~F.col("nconst").rlike(NCONST_RE)),
            ("null_name", F.col("primaryName").isNull()),
            ("birth_nan", present_but_not_int("birthYear")),
            ("birth_range", _out_of_range(F.col("birth_i"), MIN_PERSON_YEAR, CURRENT_YEAR)),
            ("death_nan", present_but_not_int("deathYear")),
            ("death_range", _out_of_range(F.col("death_i"), MIN_PERSON_YEAR, CURRENT_YEAR)),
            (
                "died_before_born",
                birth_ok
                & death_ok
                & (F.col("death_i") < F.col("birth_i"))
                & (F.col("birth_i") >= BC_YEAR_CUTOFF),
            ),
            (
                "bc_years",
                birth_ok
                & death_ok
                & (F.col("death_i") < F.col("birth_i"))
                & (F.col("birth_i") < BC_YEAR_CUTOFF),
            ),
            (
                "age_over_max",
                birth_ok
                & death_ok
                & (F.col("death_i") >= F.col("birth_i"))
                & ((F.col("death_i") - F.col("birth_i")) > MAX_AGE),
            ),
        ],
    )
    span = names_x.agg(
        F.min("birth_i").alias("birth_min"),
        F.max("birth_i").alias("birth_max"),
        F.min("death_i").alias("death_min"),
        F.max("death_i").alias("death_max"),
    ).collect()[0]
    ranges.append(_range("name.basics", "birthYear", span["birth_min"], span["birth_max"]))
    ranges.append(_range("name.basics", "deathYear", span["death_min"], span["death_max"]))

    issues.append(_issue("null or malformed nconst", "name.basics", counted["bad_id"], "rows", "drop row"))
    issues.append(_issue("null primaryName", "name.basics", counted["null_name"], "rows", "drop row"))
    issues.append(
        _issue(
            "duplicate nconst",
            "name.basics",
            _extra_duplicates(names, ["nconst"], [NCONST_RE]),
            "extra rows",
            "keep one row",
        )
    )
    issues.append(_issue("birthYear not an integer", "name.basics", counted["birth_nan"], "rows", "set birthYear to null"))
    issues.append(
        _issue(
            f"birthYear outside {MIN_PERSON_YEAR}-{CURRENT_YEAR}",
            "name.basics",
            counted["birth_range"],
            "rows",
            "set birthYear to null",
        )
    )
    issues.append(_issue("deathYear not an integer", "name.basics", counted["death_nan"], "rows", "set deathYear to null"))
    issues.append(
        _issue(
            f"deathYear outside {MIN_PERSON_YEAR}-{CURRENT_YEAR}",
            "name.basics",
            counted["death_range"],
            "rows",
            "set deathYear to null",
        )
    )
    issues.append(
        _issue(
            f"death year before birth year, birth year >= {BC_YEAR_CUTOFF}",
            "name.basics",
            counted["died_before_born"],
            "rows",
            "set birthYear and deathYear to null",
        )
    )
    issues.append(
        _issue(
            f"death year before birth year, birth year < {BC_YEAR_CUTOFF}",
            "name.basics",
            counted["bc_years"],
            "rows",
            "kept (unsigned years, probably BC)",
        )
    )
    issues.append(
        _issue(
            f"age at death over {MAX_AGE}",
            "name.basics",
            counted["age_over_max"],
            "rows",
            "kept (cannot tell which year is wrong)",
        )
    )

    bad_kft, dangling_kft = _broken_ids(names, "knownForTitles", TCONST_RE, titles, "tconst")
    issues.append(_issue("malformed knownForTitles id", "name.basics", bad_kft, "ids", "remove that id"))
    issues.append(_issue("knownForTitles id missing from title.basics", "name.basics", dangling_kft, "ids", "remove that id"))

    print("check title.ratings", flush=True)
    rating_i = as_double("averageRating")
    votes_i = as_int("numVotes")
    ratings_x = ratings.select("*", rating_i.alias("rating_i"), votes_i.alias("votes_i"))
    counted = _counts(
        ratings_x,
        [
            ("bad_id", F.col("tconst").isNull() | ~F.col("tconst").rlike(TCONST_RE)),
            ("rating_nan", present_but_not_double("averageRating")),
            ("rating_range", F.col("rating_i").isNotNull() & ~F.col("rating_i").between(1.0, 10.0)),
            ("votes_nan", present_but_not_int("numVotes")),
            ("votes_low", F.col("votes_i").isNotNull() & (F.col("votes_i") < 1)),
        ],
    )
    orphan_ratings = (
        ratings.where(F.col("tconst").rlike(TCONST_RE))
        .join(titles.select("tconst"), "tconst", "left_anti")
        .count()
    )
    issues.append(_issue("null or malformed tconst", "title.ratings", counted["bad_id"], "rows", "drop row"))
    issues.append(
        _issue(
            "duplicate tconst",
            "title.ratings",
            _extra_duplicates(ratings, ["tconst"], [TCONST_RE]),
            "extra rows",
            "keep one row",
        )
    )
    issues.append(_issue("tconst missing from title.basics", "title.ratings", orphan_ratings, "rows", "drop row"))
    issues.append(_issue("averageRating not a number", "title.ratings", counted["rating_nan"], "rows", "drop row"))
    issues.append(_issue("averageRating outside 1-10", "title.ratings", counted["rating_range"], "rows", "drop row"))
    issues.append(_issue("numVotes not an integer", "title.ratings", counted["votes_nan"], "rows", "set numVotes to null"))
    issues.append(_issue("numVotes < 1", "title.ratings", counted["votes_low"], "rows", "set numVotes to null"))

    print("check title.crew", flush=True)
    orphan_crew = (
        crew.where(F.col("tconst").rlike(TCONST_RE))
        .join(titles.select("tconst"), "tconst", "left_anti")
        .count()
    )
    issues.append(
        _issue(
            "null or malformed tconst",
            "title.crew",
            _counts(crew, [("bad_id", F.col("tconst").isNull() | ~F.col("tconst").rlike(TCONST_RE))])["bad_id"],
            "rows",
            "drop row",
        )
    )
    issues.append(
        _issue(
            "duplicate tconst",
            "title.crew",
            _extra_duplicates(crew, ["tconst"], [TCONST_RE]),
            "extra rows",
            "keep one row",
        )
    )
    issues.append(_issue("tconst missing from title.basics", "title.crew", orphan_crew, "rows", "drop row"))
    bad_dir, dangling_dir = _broken_ids(crew, "directors", NCONST_RE, people, "nconst")
    bad_wri, dangling_wri = _broken_ids(crew, "writers", NCONST_RE, people, "nconst")
    issues.append(_issue("malformed director id", "title.crew", bad_dir, "ids", "remove that id"))
    issues.append(_issue("director id missing from name.basics", "title.crew", dangling_dir, "ids", "remove that id"))
    issues.append(_issue("malformed writer id", "title.crew", bad_wri, "ids", "remove that id"))
    issues.append(_issue("writer id missing from name.basics", "title.crew", dangling_wri, "ids", "remove that id"))

    print("check title.episode", flush=True)
    season_i = as_int("seasonNumber")
    episode_i = as_int("episodeNumber")
    episodes_x = episodes.select("*", season_i.alias("season_i"), episode_i.alias("episode_i"))
    counted = _counts(
        episodes_x,
        [
            ("bad_id", F.col("tconst").isNull() | ~F.col("tconst").rlike(TCONST_RE)),
            (
                "bad_parent",
                F.col("parentTconst").isNull() | ~F.col("parentTconst").rlike(TCONST_RE),
            ),
            ("season_nan", present_but_not_int("seasonNumber")),
            ("season_range", _out_of_range(F.col("season_i"), 1, MAX_SEASON)),
            ("episode_nan", present_but_not_int("episodeNumber")),
            ("episode_range", _out_of_range(F.col("episode_i"), MIN_EPISODE, MAX_EPISODE)),
            ("episode_zero", F.col("episode_i") == 0),
        ],
    )
    orphan_episode = (
        episodes.where(F.col("tconst").rlike(TCONST_RE))
        .join(titles.select("tconst"), "tconst", "left_anti")
        .count()
    )
    orphan_parent = (
        episodes.where(F.col("parentTconst").rlike(TCONST_RE))
        .join(titles.select(F.col("tconst").alias("parentTconst")), "parentTconst", "left_anti")
        .count()
    )
    parents = titles.select(F.col("tconst").alias("parentTconst"), F.col("titleType").alias("parentType"))
    parent_not_series = (
        episodes.join(parents, "parentTconst", "inner")
        .where(~F.col("parentType").isin(*SERIES_TYPES))
        .count()
    )
    self_type = titles.select("tconst", F.col("titleType").alias("selfType"))
    not_episode = (
        episodes.join(self_type, "tconst", "inner")
        .where(F.col("selfType") != "tvEpisode")
        .count()
    )
    issues.append(_issue("null or malformed tconst", "title.episode", counted["bad_id"], "rows", "drop row"))
    issues.append(
        _issue(
            "duplicate tconst",
            "title.episode",
            _extra_duplicates(episodes, ["tconst"], [TCONST_RE]),
            "extra rows",
            "keep one row",
        )
    )
    issues.append(_issue("tconst missing from title.basics", "title.episode", orphan_episode, "rows", "drop row"))
    issues.append(
        _issue(
            "null or malformed parentTconst",
            "title.episode",
            counted["bad_parent"],
            "rows",
            "drop row",
        )
    )
    issues.append(
        _issue(
            "parentTconst missing from title.basics",
            "title.episode",
            orphan_parent,
            "rows",
            "drop row",
        )
    )
    issues.append(
        _issue(
            "parent title is not tvSeries or tvMiniSeries",
            "title.episode",
            parent_not_series,
            "rows",
            "kept (parent row exists)",
        )
    )
    issues.append(
        _issue(
            "title.basics type is not tvEpisode",
            "title.episode",
            not_episode,
            "rows",
            "kept (id exists)",
        )
    )
    issues.append(_issue("seasonNumber not an integer", "title.episode", counted["season_nan"], "rows", "set seasonNumber to null"))
    issues.append(
        _issue(
            f"seasonNumber outside 1-{MAX_SEASON}",
            "title.episode",
            counted["season_range"],
            "rows",
            "set seasonNumber to null",
        )
    )
    issues.append(_issue("episodeNumber not an integer", "title.episode", counted["episode_nan"], "rows", "set episodeNumber to null"))
    issues.append(
        _issue(
            f"episodeNumber outside {MIN_EPISODE}-{MAX_EPISODE}",
            "title.episode",
            counted["episode_range"],
            "rows",
            "set episodeNumber to null",
        )
    )
    issues.append(
        _issue(
            "episodeNumber is 0",
            "title.episode",
            counted["episode_zero"],
            "rows",
            "kept (some series use episode 0)",
        )
    )

    print("check title.principals", flush=True)
    ordering_i = as_int("ordering")
    principals_x = principals.select("*", ordering_i.alias("ordering_i"))
    counted = _counts(
        principals_x,
        [
            ("bad_title", F.col("tconst").isNull() | ~F.col("tconst").rlike(TCONST_RE)),
            ("bad_name", F.col("nconst").isNull() | ~F.col("nconst").rlike(NCONST_RE)),
            ("bad_ordering", F.col("ordering_i").isNull() | (F.col("ordering_i") < 1)),
            ("null_category", F.col("category").isNull()),
            (
                "bad_characters",
                F.col("characters").isNotNull() & ~F.col("characters").rlike(r"^\[.*\]$"),
            ),
        ],
    )
    orphan_principal_title = (
        principals.where(F.col("tconst").rlike(TCONST_RE))
        .join(titles.select("tconst"), "tconst", "left_anti")
        .count()
    )
    orphan_principal_name = (
        principals.where(F.col("nconst").rlike(NCONST_RE))
        .join(people, "nconst", "left_anti")
        .count()
    )
    issues.append(_issue("null or malformed tconst", "title.principals", counted["bad_title"], "rows", "drop row"))
    issues.append(_issue("null or malformed nconst", "title.principals", counted["bad_name"], "rows", "drop row"))
    issues.append(
        _issue(
            "ordering missing, non-numeric, or < 1",
            "title.principals",
            counted["bad_ordering"],
            "rows",
            "drop row",
        )
    )
    issues.append(
        _issue(
            "duplicate (tconst, ordering)",
            "title.principals",
            _extra_duplicates(principals, ["tconst", "ordering"], [TCONST_RE, r"^\d+$"]),
            "extra rows",
            "keep one row",
        )
    )
    issues.append(
        _issue(
            "tconst missing from title.basics",
            "title.principals",
            orphan_principal_title,
            "rows",
            "drop row",
        )
    )
    issues.append(
        _issue(
            "nconst missing from name.basics",
            "title.principals",
            orphan_principal_name,
            "rows",
            "drop row",
        )
    )
    issues.append(_issue("null category", "title.principals", counted["null_category"], "rows", "drop row"))
    issues.append(
        _issue(
            "characters is not a JSON list",
            "title.principals",
            counted["bad_characters"],
            "rows",
            "set characters to null",
        )
    )

    print("check title.akas", flush=True)
    aka_ordering = as_int("ordering")
    akas_x = akas.select("*", aka_ordering.alias("ordering_i"))
    counted = _counts(
        akas_x,
        [
            ("bad_id", F.col("titleId").isNull() | ~F.col("titleId").rlike(TCONST_RE)),
            ("bad_ordering", F.col("ordering_i").isNull() | (F.col("ordering_i") < 1)),
            ("null_title", F.col("title").isNull()),
            (
                "bad_original",
                F.col("isOriginalTitle").isNotNull() & ~F.col("isOriginalTitle").isin("0", "1"),
            ),
        ],
    )
    orphan_aka = (
        akas.where(F.col("titleId").rlike(TCONST_RE))
        .join(titles.select(F.col("tconst").alias("titleId")), "titleId", "left_anti")
        .count()
    )
    multi_original = (
        akas.where(F.col("isOriginalTitle") == "1")
        .groupBy("titleId")
        .count()
        .where(F.col("count") > 1)
        .count()
    )
    issues.append(_issue("null or malformed titleId", "title.akas", counted["bad_id"], "rows", "drop row"))
    issues.append(
        _issue(
            "ordering missing, non-numeric, or < 1",
            "title.akas",
            counted["bad_ordering"],
            "rows",
            "drop row",
        )
    )
    issues.append(
        _issue(
            "duplicate (titleId, ordering)",
            "title.akas",
            _extra_duplicates(akas, ["titleId", "ordering"], [TCONST_RE, r"^\d+$"]),
            "extra rows",
            "keep one row",
        )
    )
    issues.append(_issue("titleId missing from title.basics", "title.akas", orphan_aka, "rows", "drop row"))
    issues.append(_issue("null title", "title.akas", counted["null_title"], "rows", "drop row"))
    issues.append(
        _issue(
            "isOriginalTitle other than 0 or 1",
            "title.akas",
            counted["bad_original"],
            "rows",
            "set isOriginalTitle to null",
        )
    )
    issues.append(
        _issue(
            "more than one isOriginalTitle = 1",
            "title.akas",
            multi_original,
            "titles",
            "kept (no way to pick the right row)",
        )
    )

    type_counts = [
        {"titleType": row["titleType"], "titles": int(row["count"])}
        for row in basics.groupBy("titleType").count().orderBy(F.desc("count")).collect()
    ]

    titles.unpersist()
    people.unpersist()
    return issues, ranges, type_counts


def _range(table, column, low, high):
    return {
        "table": table,
        "column": column,
        "min": None if low is None else int(low),
        "max": None if high is None else int(high),
    }
