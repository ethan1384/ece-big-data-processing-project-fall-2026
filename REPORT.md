# ECE Big Data Processing: Report

**Group Contributors** · Ethan Petain and Nicolas Cottez-Abrate

## 1. Data preparation

All seven IMDb tables were profiled before cleaning. The table shows the five columns with the most missing values. The full exports cover [every column](output/part01/null_percentages.csv), [all quality checks](output/part01/quality_issues.csv) and [row counts before and after cleaning](output/part01/cleaning_summary.csv).

| Column | Missing |
| --- | ---: |
| `title.akas.attributes` | 99.47% |
| `title.basics.endYear` | 98.73% |
| `name.basics.deathYear` | 98.32% |
| `name.basics.birthYear` | 95.66% |
| `title.principals.job` | 80.66% |

**Methodology.** Spark transformed the missing-value markers and the blank fields into nulls. Then it divided the number of nulls by the total number of rows. Many of these nulls are expected. A film has no series end year, for example. Deleting every incomplete row would throw away valid records. Filling in dates or runtimes would add values with nothing to back them.

The cleaning set seven invalid runtimes to null. It also set 1,363 out-of-range season numbers to null. Ten people had birth and death years that contradicted each other. A pair is suspicious when deathYear is before birthYear. If the birth year is before 1000, we keep the pair. These are probably BC dates without the sign. Eighteen older pairs were kept this way. If the years are from 1000 or after, we set both years to null. Not only the death year. Because we don't know which of the two years is wrong. Rows whose required parent was missing were removed. This includes 7,987 principal credits after the person table was cleaned. In the director, writer and known-for arrays, only the broken identifiers were dropped.



## 2. Data exploration

A feature film here is an IMDb movie that is not marked adult, has a known release year and runs at least 40 minutes. The [full exploration exports](output/part02/) hold the result lists for all eight questions.

| Question | Method and main result |
| --- | --- |
| Q1 · Runtime outliers | In genres with at least 30 films, a film-genre pair is flagged when its log runtime is three standard deviations or more from the genre mean. **4,791 pairs** were flagged, covering **3,720 films**. [Full list](output/part02/q1_runtime_outliers.csv) |
| Q2 · Top three genres | For each release year from 1980 to 2025, genres were ranked by total IMDb votes. **Drama** made the top three every year and came first **42 times**. [All 46 rankings](output/part02/q2_top3_genres_by_year.csv) |
| Q3 · Top three vs. the rest | Depending on the year, the top three took **38.3% to 58.3%** of genre-attributed votes. In 2025 they averaged **3,405 votes per film-genre pair**, against **3,717** for the other genres. [Year-by-year comparison](output/part02/q3_top3_vs_rest_by_year.csv) |
| Q4 · Directors with one highly rated film | **168 directors** had exactly one film rated 8.0 or higher, plus at least two other eligible films averaging 6.0 or less. Every film needed at least 100 votes. [Directors](output/part02/q4_one_hit_directors.csv) |
| Q5 · Best episode early, worst episode last | **272 series** qualified. Each had at least three observed seasons, with at least ten listed episodes in every season. Only episodes with 20 votes or more were compared, and ties across the compared seasons were excluded. [Series](output/part02/q5_series_best_early_worst_last.csv) |
| Q6 · Successful films across languages | A successful film had at least 1,000 votes and a rating of 7.0 or more. The pipeline estimated a language for **12,917 of 14,158** of these films. **3,605 performers** had successes in [two or more estimated languages](output/part02/q6_hits_in_2plus_languages.csv), and **605** of them in [three or more](output/part02/q6_hits_in_3plus_languages.csv). |
| Q7 · Early career peak | **2,093 performers** had at least 15 eligible films over 20 years or more, their best-rated film in roughly the first third, and a first-half mean at least 0.5 points above the second-half mean. Every film needed at least 100 votes. [Performers](output/part02/q7_early_peak_actors.csv) |
| Q8 · Multiple roles on the same film | **31,653 people** had at least three feature credits in each of two or more roles. For the **12,585** with rated films of both kinds, the mean within-person difference was only **+0.023 points** in favour of combined roles. [Role counts](output/part02/q8_multi_talent_counts.csv) · [Rating comparison](output/part02/q8_combined_vs_single_by_roles.csv) |

**Methodology and trade-offs.** For Q1, we could have used the raw minutes. But a few films are extremely long. And with the raw minutes, they would dominate the results. So we used the log of the runtime.
Vote totals measure engagement on IMDb. But they also grow with the number of films in a genre. And each film adds its votes to every genre it belongs to. The rating thresholds limit the weight of titles that have very few votes. But this reduces the coverage.

Q6 estimates the language of a film. It uses alternate-title records, regions, and the language candidates that recur across a director's films. A real original-language field would be a sounder basis than this estimation.
Q7 splits careers by the number of films. A split by calendar years can leave one half with very few films.
Film ratings measure how the film was received.
Q8 compares each person with their own single-role work. This reduces the differences between people. But the projects they choose can still skew the comparison.

## 3. Data enrichment

Spark took the ten most-voted feature films, for each release year and genre, from 1980 to 2025. Then it took up to three distinct principals per film, in IMDb billing order. Wikidata linked the IMDb IDs to English Wikipedia articles. The output keep the source revision URL when there is one, and flag missing content.

| Metric | Result |
| --- | ---: |
| Films selected | 6,821 |
| Films with plot text | 5,889 (86.34%) |
| Films using the article lead as fallback | 384 (5.63%) |
| Films without a synopsis | 548 (8.03%) |
| Principal credits with a biography | 17,729 of 20,431 (86.77%) |
| Films with three biographies | 5,197 (76.19%) |

See [coverage by year](output/part03/coverage_films_by_year.csv), [missing-content flags](output/part03/coverage_notes.csv) and [all enrichment exports](output/part03/).

**Methodology and trade-offs.** Matching on IMDb identifiers avoids many collisions between titles and names. A rate-limited Python client caches the responses. Spark does the joins, the schema enforcement, and the final text cleaning. Python also strip the plot markup during retrieval. So part of the preprocessing happens outside Spark. One source keeps attribution simple, but it limits the coverage. We did not add a second source when the Wikipedia content is missing. This is a voluntary choice. The principals follow a fixed billing order, with no substitutes. So 1,624 films ended up with fewer than three biographies. The driver-side collection also means these results say little about how the pipeline would scale to the full dataset.

## 4. Streaming

Wikimedia recent-change events went through Kafka into Spark Structured Streaming. The filter kept edits to English Wikipedia articles. It matched them to the IMDb on resolved page titles. It also matched them on film titles with or without a release year. And it matched them on the names of credited professionals.

| Metric | Result |
| --- | ---: |
| Kafka events read across all wikis | 52,027 |
| Distinct matched edits | 53 |
| Distinct pages matched | 45 |
| Time between first and last matched edits | 25.6 min |
| Matched edits in finalized windows | 41 |
| Median / 95th percentile arrival delay | 3 s / 8 s |
| Maximum matched arrival delay | 603 s |

Of the 53 matches, 43 came from person names, eight from film titles and two from film titles with a year. Name matches can be ambiguous. See the [run summary](output/part04/run_summary.csv), [match breakdown](output/part04/matches_by_type.csv) and [window counts](output/part04/window_counts.csv).

**Methodology and late data.** The two-minute windows use the Wikipedia edit timestamp. So a delayed edit is counted in the window when it actually happened. The watermark is five minutes. This is enough for most of the delays we observed in the data. And it does not keep the windows open too long. A longer watermark would keep more late edits. But the final counts would be published later. The edit that reached Kafka after 603 seconds was kept in the separate match output. The exports can't say how much of the gap between the 53 recorded matches and the 41 finalized ones comes from late arrivals. Open windows and different deduplication also affect the totals. Spark reported 30 dropped partial aggregates.

## 5. Open question · Do sequels hold up?

**Question.** Does a later film in a series get the same rating as the one before it? Does the change depend on keeping the director? Or on bringing back the previous lead actor? The analysis also checks whether the pattern differs with continuity. And it compares it with the director's next film, outside the retrieved series set. A sequel usually comes out after a successful film. So a drop in the rating can be a regression to the mean. This is why the question is interesting.

| Comparison | Pairs | Mean rating change |
| --- | ---: | ---: |
| Later entry vs. previous entry | 1,469 | −0.257 |
| Director's next film outside the retrieved series set | 29,260 | −0.105 |
| Both director and previous lead retained | 441 | −0.259 |
| Neither identified as returning | 489 | −0.199 |

**Methodology and trade-offs.** Wikidata series memberships were joined to IMDb titles, ratings, directors, and principal acting credits. The join kept films with at least 1,000 votes. Within each series, films were sorted by release year and IMDb ID. Then they were compared with the previous retained entry. Membership identifiers avoid to guess from film names. But verified predecessor links would separate direct sequels from reboots and spin-offs more reliably. A director counts as kept when the two films share at least one director. The previous lead counts as returning when they appear among the sequel's principal acting credits.

**Result.** The later entry scored lower in 62.8% of pairs. The baseline dropped less overall. But that order did not held in every prior-rating group. Returning contributors did not consistently go with a smaller drop. Incomplete membership data limit what these descriptive comparisons can show. And differences between groups limit it too. [Overall results](output/part05/sequel_vs_previous.csv) · [Baseline by prior rating](output/part05/sequel_vs_baseline.csv) · [Continuity](output/part05/continuity.csv) · [All open-question exports](output/part05/)
