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

## AI tools

Cursor (Grok) was used to write the data-preparation code: the loader, the null profile, the quality checks, and the cleaning job. It was also used to draft the run steps in this file.

The methodology in `REPORT.md` has to be rewritten by the group before the project is submitted. The course does not allow an AI-written explanation there.

## Not done yet

Exploration, enrichment, streaming, and the open question are not implemented.
