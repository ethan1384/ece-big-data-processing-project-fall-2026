# PROJECT

## Contributors

- Ethan Petain — ethan.petain@gmail.com — github [ethan1384](https://github.com/ethan1384)
- Nicolas Cottez-Abrate — github [Nicolas-cottez](https://github.com/Nicolas-cottez) — email: fill in before submission

Group Gr-02.

## Environment

- Python 3.11
- Java 17 (Spark will not start without a JDK on `PATH`)
- PySpark 3.5.4 (`requirements.txt`)

The preparation job runs in Spark local mode and asks for a 16 GB driver heap. A laptop with 16 GB of RAM is tight; 32 GB or more is comfortable. Nothing is written to a cluster.

IMDb’s dumps are for personal, non-commercial use. Do not redistribute them.

## Data preparation

From the repository root:

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
bash scripts/download_imdb.sh
python -m src.prepare
```

`scripts/download_imdb.sh` fetches the seven gzipped TSV files from `https://datasets.imdbws.com/` into `data/raw/` (about 1.9 GB). The script skips a file that is already there.

`python -m src.prepare` then:

1. Reads each TSV. IMDb’s `\N` and blank cells become null. The files are not quoted, so Spark’s quote handling is turned off (the `characters` column contains JSON).
2. Rewrites them as Parquet under `data/tmp/normalized/` so the gzip parse happens once.
3. Computes null percentages and the other quality checks.
4. Writes cleaned tables under `data/clean/` and CSV summaries under `output/part01/`.

A second run reuses `data/tmp/normalized/`. Pass `--rebuild` to parse the TSV files again.

Cleaned tables:

| Path | Contents |
| --- | --- |
| `data/clean/name_basics` | people |
| `data/clean/title_basics` | titles |
| `data/clean/title_ratings` | ratings |
| `data/clean/title_crew` | directors and writers, as arrays of `nconst` |
| `data/clean/title_episode` | episodes |
| `data/clean/title_principals` | cast and crew credits |
| `data/clean/title_akas` | alternate titles (`titleId` is the title’s `tconst`) |

`data/raw/`, `data/tmp/` and `data/clean/` are gitignored. The CSV summaries in `output/part01/` are committed. The numbers discussed in `REPORT.md` come from those files.

Checked on this machine with Java 17 and Python 3.11. The job takes a while because `title.principals` and `title.akas` are large.

## Data exploration

Needs `data/clean/` from the preparation step.

```bash
python -m src.explore            # all eight questions
python -m src.explore q4 q5      # only some of them
```

Each question writes CSV files to `output/part02/` (committed, like `output/part01/`). Thresholds (feature-length cutoff, minimum votes, what counts as a hit, ...) are constants at the top of `src/explore.py`.

| Question | Files |
| --- | --- |
| q1 runtime outliers by genre | `q1_runtime_outliers.csv`, `q1_genre_runtime_summary.csv` |
| q2 top 3 genres per year | `q2_top3_genres_by_year.csv` |
| q3 top 3 vs other genres | `q3_top3_vs_rest_by_year.csv`, `q3_genre_top3_frequency.csv` |
| q4 one-hit directors | `q4_one_hit_directors.csv` |
| q5 series, best early / worst last | `q5_series_best_early_worst_last.csv` |
| q6 actors with hits in several languages | `q6_hits_in_2plus_languages.csv`, `q6_hits_in_3plus_languages.csv`, `q6_actors_by_language_count.csv`, `q6_language_coverage.csv`, `q6_hit_movies_by_language.csv` |
| q7 early-peak actors | `q7_early_peak_actors.csv` |
| q8 multi-talented people | `q8_multi_talent_counts.csv`, `q8_multi_talent_people.csv`, `q8_combined_vs_single_by_roles.csv` |

## Data enrichment

Needs `data/clean/` from the preparation step and internet access (Wikidata and the English Wikipedia).

```bash
python -m src.enrich             # select, fetch, build
python -m src.enrich select      # only the Spark selection
python -m src.enrich fetch       # only the download (resumable)
python -m src.enrich build       # only the Spark cleaning and outputs
```

1. `select` (Spark): keeps feature films from 1980 to 2025, ranks them by votes inside each (year, genre) and keeps the top 10, then takes the first 3 principals of each film in IMDb billing order. Written to `data/enrich/selection/`.
2. `fetch` (plain Python, `src/wikipedia.py`): maps each IMDb id to its English Wikipedia article with Wikidata (property P345, the IMDb id), then downloads the article. Films get the "Plot" section (wikitext cleaned with `mwparserfromhell`) and the lead paragraph; people get the lead paragraph as biography. Requests are sent one at a time, at least one second apart, with `maxlag`, `Retry-After` handling and a descriptive User-Agent. Every answer is appended to `data/enrich/cache/*.jsonl`, so an interrupted run resumes where it stopped. Expect roughly 30 to 60 minutes.
3. `build` (Spark): reads the cache with an explicit schema, tidies the text, adds a `notes` array that says what is missing, keeps the lineage fields, and writes `data/enriched/films` and `data/enriched/principals` (Parquet) plus the CSV summaries in `output/part03/`.

Every enriched row carries `source_url` (a permalink to the exact Wikipedia revision), `wikidata_uri`, `revision_id` and `retrieved_at`. The scope is set by `TOP_N_FILMS`, `N_PRINCIPALS`, `FIRST_YEAR` and `LAST_YEAR`; the code is the same for the whole dataset.

| Output | Contents |
| --- | --- |
| `data/enriched/films` | one row per film: synopsis, genres where it is top 10, lineage, notes (not committed) |
| `data/enriched/principals` | one row per (film, principal): biography, lineage, notes (not committed) |
| `output/part03/coverage_summary.csv` | how many films and people were found |
| `output/part03/coverage_films_by_year.csv` | synopsis coverage per year |
| `output/part03/coverage_notes.csv` | count of each "missing" note |
| `output/part03/sample_films.csv`, `sample_principals.csv` | readable sample of the output |

## Streaming

Needs Java 17, `data/clean/` from the preparation step, and internet access (Kafka download, Spark's Kafka connector from Maven Central, and `stream.wikimedia.org`). The `wikidata_page` match type also uses the part 3 cache (`data/enrich/cache/wikidata.jsonl`); without it the other match types still work.

Kafka runs as a single local broker in KRaft mode (no ZooKeeper). The script downloads Kafka 3.9.0 into `~/.local/share/imdb-kafka` on the first run, starts it on `localhost:9092`, and creates the topic `wiki-recentchange`. With Docker, `docker run -d --name kafka -p 9092:9092 apache/kafka:3.9.0` works too (the topic is then created by the producer).

Use three terminals, all at the repository root with the virtual environment active:

```bash
# terminal 1: Kafka, then the IMDb lookup tables (once, a few minutes)
bash scripts/kafka_local.sh start
python -m src.stream lookup

# terminal 2: Wikimedia -> Kafka (5% of events held back 10 minutes to test late data)
python -m src.wiki_producer --minutes 30 --late-share 0.05 --late-delay 600

# terminal 3: the Spark Structured Streaming job, then the summary
python -m src.stream run --minutes 25 --fresh
python -m src.stream report

# afterwards
bash scripts/kafka_local.sh stop
```

1. `src/wiki_producer.py` (plain Python): reads Wikimedia's recent-change stream (Server-Sent Events, every edit of every wiki) and writes each event unchanged to Kafka. It resumes with `Last-Event-ID` after a dropped connection.
2. `python -m src.stream lookup` (Spark batch): builds the IMDb lookup tables in `data/stream/lookup/` from feature films with at least 1,000 votes and the actors, actresses, directors and writers credited on them.
3. `python -m src.stream run` (Spark Structured Streaming): reads the topic, keeps English Wikipedia article edits, matches the page to a film or a person through the lookup tables (stream-static joins), and runs two queries every 30 seconds: every matched edit goes to `data/stream/matches/`, and edit counts per 2-minute event-time window go to `data/stream/windows/` with a 5-minute watermark. Progress (rows read, watermark, rows dropped as late) is logged to `data/stream/progress.jsonl`.
4. `python -m src.stream report` (Spark batch): writes the CSV summaries in `output/part04/`.

`--fresh` deletes the previous outputs and checkpoints; without it a stopped job resumes from its checkpoint. Spark downloads `spark-sql-kafka-0-10_2.12:3.5.4` on the first run.

| Output | Contents |
| --- | --- |
| `output/part04/run_summary.csv` | events read, matches, matches per minute, lateness, rows dropped by the watermark |
| `output/part04/matches_by_type.csv` | matched edits per match type |
| `output/part04/window_counts.csv` | edits per 2-minute window and match type (closed windows only) |
| `output/part04/lateness_buckets.csv` | how late the matched edits reached Kafka |
| `output/part04/top_pages.csv` | most edited matched pages |
| `output/part04/sample_matches.csv` | first matched edits, with a link to the diff |

## AI tools

Cursor (Grok) was used to write the data-preparation code: the loader, the null profile, the quality checks, and the cleaning job. It was also used to draft the run steps in this file.

Claude (Anthropic) was used to write the exploration code in `src/explore.py` (the eight queries of part 2) and its run steps in this file.

Claude (Anthropic) was also used to write the enrichment code in `src/enrich.py` and `src/wikipedia.py` (part 3) and its run steps in this file.

Claude (Anthropic) was also used to write the streaming code in `src/wiki_producer.py`, `src/stream.py` and `scripts/kafka_local.sh` (part 4) and its run steps in this file.

The methodology in `REPORT.md` has to be rewritten by the group before the project is submitted. The course does not allow an AI-written explanation there.

## Not done yet

The open question is not implemented.
