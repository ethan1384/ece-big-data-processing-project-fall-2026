# REPORT

Numbers below come from `python -m src.prepare`. The CSV copies are in `output/part01/`.

The dumps used are the public IMDb files from `datasets.imdbws.com` (daily refresh). `\N` and blank cells are counted as null. Percentages use the row count of that table, before any row is dropped.

> The methodology section is a working draft. Rewrite it in your own words before submitting. The course does not allow an AI-written explanation in this file.

## Null, missing, and empty values

### name.basics

15,808,370 rows.

| Column | Nulls | Null % |
| --- | ---: | ---: |
| nconst | 0 | 0.00 |
| primaryName | 89 | 0.00 |
| birthYear | 15,122,576 | 95.66 |
| deathYear | 15,542,928 | 98.32 |
| primaryProfession | 3,218,607 | 20.36 |
| knownForTitles | 1,916,507 | 12.12 |

### title.akas

60,353,877 rows.

| Column | Nulls | Null % |
| --- | ---: | ---: |
| titleId | 0 | 0.00 |
| ordering | 0 | 0.00 |
| title | 0 | 0.00 |
| region | 12,993,626 | 21.53 |
| language | 19,649,615 | 32.56 |
| types | 40,349,110 | 66.85 |
| attributes | 60,036,655 | 99.47 |
| isOriginalTitle | 0 | 0.00 |

### title.basics

12,953,566 rows.

| Column | Nulls | Null % |
| --- | ---: | ---: |
| tconst | 0 | 0.00 |
| titleType | 0 | 0.00 |
| primaryTitle | 0 | 0.00 |
| originalTitle | 0 | 0.00 |
| isAdult | 0 | 0.00 |
| startYear | 1,487,454 | 11.48 |
| endYear | 12,789,170 | 98.73 |
| runtimeMinutes | 8,053,410 | 62.17 |
| genres | 547,893 | 4.23 |

### title.crew

12,953,566 rows.

| Column | Nulls | Null % |
| --- | ---: | ---: |
| tconst | 0 | 0.00 |
| directors | 5,785,700 | 44.66 |
| writers | 6,426,788 | 49.61 |

### title.episode

10,026,289 rows.

| Column | Nulls | Null % |
| --- | ---: | ---: |
| tconst | 0 | 0.00 |
| parentTconst | 0 | 0.00 |
| seasonNumber | 2,120,813 | 21.15 |
| episodeNumber | 2,120,813 | 21.15 |

### title.principals

102,713,417 rows.

| Column | Nulls | Null % |
| --- | ---: | ---: |
| tconst | 0 | 0.00 |
| ordering | 0 | 0.00 |
| nconst | 0 | 0.00 |
| category | 0 | 0.00 |
| job | 82,849,061 | 80.66 |
| characters | 52,669,159 | 51.28 |

### title.ratings

1,731,454 rows. `tconst`, `averageRating`, and `numVotes` are all filled (0% null).

## Five columns with the most nulls

| Rank | Column | Null % |
| --- | --- | ---: |
| 1 | title.akas.attributes | 99.47 |
| 2 | title.basics.endYear | 98.73 |
| 3 | name.basics.deathYear | 98.32 |
| 4 | name.basics.birthYear | 95.66 |
| 5 | title.principals.job | 80.66 |

`endYear` is empty for anything that is not a series (about 12.6 million of 13.0 million titles are not `tvSeries` or `tvMiniSeries`). `deathYear` is empty for people who are alive or whose death was never recorded. `attributes` and `job` are optional labels, not broken keys. `birthYear` is simply missing for most names. None of these were filled in.

## Other quality issues

Checks that came back empty are listed in `output/part01/quality_issues.csv` (duplicate keys, ratings outside 1–10, orphan ratings, `endYear` on a non-series, genres longer than 3, and so on). The hits are below.

| Issue | Affected | What the job does |
| --- | ---: | --- |
| startYear after 2036 (the only value is 2115) | 1 title | `startYear` set to null |
| runtimeMinutes is 0, or above 100,000 | 7 titles | `runtimeMinutes` set to null |
| primaryName null | 89 people | row dropped |
| death year before birth year, and birth year is 1000 or later | 10 people | both years set to null |
| death year before birth year, birth year before 1000 | 18 people | kept |
| knownForTitles id not in title.basics | 218 ids | id removed, person kept |
| director id not in name.basics | 466 ids | id removed, title kept |
| writer id not in name.basics | 1,203 ids | id removed, title kept |
| episode parentTconst not in title.basics | 5 episodes | row dropped |
| seasonNumber above 300 | 1,363 episodes | `seasonNumber` set to null |
| episodeNumber is 0 | 7,123 episodes | kept |
| principal nconst not in name.basics | 7,976 credits | row dropped |
| aka titleId not in title.basics | 1 row | row dropped |

Parsed ranges before cleaning: `startYear` 1874–2115, `runtimeMinutes` 0–3,692,080, `birthYear` 4–2026, `deathYear` 17–2026.

The seven bad runtimes are four shorts with runtime 0, plus *The Angry Grandpa Show* (3,692,080), *Love Link TV the Mixer* (1,000,000), and *Shree Haricharitramrut Sagar* (146,445). *Logistics* (51,420 minutes) and *Ambiancé* (43,200) stay, because those runtimes are real.

The 1,363 bad seasons are mostly a calendar year written into `seasonNumber` (2024, 2025, 2009, …), plus a couple of junk values (947, 1000).

The 18 early “death before birth” rows are people like Homer (850 / 800) and Plato (427 / 347). IMDb stores years BC as positive numbers, so a smaller death year does not mean they died before they were born. The 10 later ones (for example A. Maneck, 1899 / 1867) really are inconsistent, and it is not obvious which of the two years is wrong.

Principal rows removed: 7,987. That is the 7,976 dangling `nconst` values, plus 11 credits that pointed at one of the 89 people dropped for a missing name.

## Rows after cleaning

| Table | In | Out | Dropped |
| --- | ---: | ---: | ---: |
| title.basics | 12,953,566 | 12,953,566 | 0 |
| name.basics | 15,808,370 | 15,808,281 | 89 |
| title.ratings | 1,731,454 | 1,731,454 | 0 |
| title.crew | 12,953,566 | 12,953,566 | 0 |
| title.episode | 10,026,289 | 10,026,284 | 5 |
| title.principals | 102,713,417 | 102,705,430 | 7,987 |
| title.akas | 60,353,877 | 60,353,876 | 1 |

Cleaned Parquet is under `data/clean/`. Array columns (`genres`, `directors`, `writers`, `primaryProfession`, `knownForTitles`) are real arrays, not comma-separated strings. `title.akas.titleId` is still the title id; it joins to `title.basics.tconst`.

## Methodology

Draft, to be rewritten by the group.

Nulls were counted on the files as published, after turning `\N` and blanks into null and before dropping anything. A percentage of nulls is what the question asks for. Counting only “surprising” nulls would have hidden `endYear` and `deathYear`, which are the whole top of the ranking.

We did not impute those columns. A mean runtime, a fake death year, or the string `"unknown"` would look like a real value in the exploration queries. Leaving the null is the smaller lie. Dropping every row that has any null would delete almost every movie, because `endYear` is null on purpose.

Impossible values are cleared on the cell, and the row stays. One title with `startYear` 2115 is still a title. A runtime of 0 or of several million minutes is not a runtime. The cutoff for runtime is 100,000 minutes so that *Logistics* (about 857 hours) is kept and the obvious garbage is not.

Foreign keys are the other case. A rating, credit, episode, or aka that points at a missing title or person cannot be joined, so the child row is dropped. Inside `directors`, `writers`, and `knownForTitles`, only the bad id is removed. Dropping the whole person because one `knownForTitles` id is stale would throw away a good row for a single broken token.

`deathYear < birthYear` is not one problem. Under year 1000 it matches the way IMDb writes BC dates, so those rows are kept. From year 1000 up, the pair disagrees and either year could be the typo (Ahmad Shawqi is stored as 1968–1932; the death year is the plausible one). Both years are set to null rather than guessing. The alternative was to always null `deathYear`. That keeps a wrong birth year and deletes the year that was sometimes right.

`episodeNumber` 0 is kept. Several thousand episodes use it, and treating 0 as missing would delete a value the source actually stored. Season numbers above 300 are not kept: they are years, not seasons.

Things we checked and did not find, so there was nothing to fix: duplicate primary keys, ratings outside 1–10, `endYear` sitting on a movie, and episode rows whose parent is not a series.

# Data exploration

Numbers below come from `python -m src.explore`. The CSV copies are in `output/part02/`.

> TODO (group): paste the result tables after the run, then write each methodology in your own words. The course does not allow an AI-written explanation in this file.

## Q1. Movies whose runtime is far from their genre average

Output: `output/part02/q1_genre_runtime_summary.csv`, `output/part02/q1_runtime_outliers.csv`.

TODO: results.

### Methodology

TODO: to be written by the group.

## Q2. Top three genres per year, 1980-2025

Output: `output/part02/q2_top3_genres_by_year.csv`.

TODO: results.

### Methodology

TODO: to be written by the group.

## Q3. Top three genres compared with the rest

Output: `output/part02/q3_top3_vs_rest_by_year.csv`, `output/part02/q3_genre_top3_frequency.csv`.

TODO: results.

### Methodology

TODO: to be written by the group.

## Q4. Directors with exactly one movie rated 8.0 or more

Output: `output/part02/q4_one_hit_directors.csv`.

TODO: results.

### Methodology

TODO: to be written by the group.

## Q5. Series whose best episode is early and worst episode is in the last season

Output: `output/part02/q5_series_best_early_worst_last.csv`.

TODO: results.

### Methodology

TODO: to be written by the group.

## Q6. Actors with successful movies in several languages

Output: `output/part02/q6_language_coverage.csv`, `output/part02/q6_hit_movies_by_language.csv`, `output/part02/q6_actors_by_language_count.csv`, `output/part02/q6_hits_in_3plus_languages.csv`, `output/part02/q6_hits_in_2plus_languages.csv`.

TODO: results.

### Methodology

TODO: to be written by the group.

## Q7. Actors whose best movies came early

Output: `output/part02/q7_early_peak_actors.csv`.

TODO: results.

### Methodology

TODO: to be written by the group.

## Q8. Multi-talented people

Output: `output/part02/q8_multi_talent_counts.csv`, `output/part02/q8_combined_vs_single_by_roles.csv`, `output/part02/q8_multi_talent_people.csv`.

TODO: results.

### Methodology

TODO: to be written by the group.
