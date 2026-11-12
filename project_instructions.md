# ECE big data processing project

## Submission and deliverables

The project will be submitted via a GitHub repo including two primary Markdown files and all relevant code. 

1. **PROJECT.md (Top level information and Execution and Operations Guide):**
    - A list of the project contributors including email addresses
    - Clear, step-by-step instructions for running the project including environment set up. 
    > **NOTE:** All code must be runnable from scratch by following these instructions. 
    - Explicit declaration of any AI/LLM tools used during development, including which tool was used and which tasks relied on them
2. **REPORT.md**
    - All final query outputs, tables, streaming metrics, etc.
    - **Required Methodology Write-up:** For each major section (Preparation, Exploration, Enrichment, Streaming), you must include a dedicated **Methodological Explanation** covering:
        - The methodology and why it was chosen
        - Alternatives considered and trade-offs made
3. **Git standards**
    - Each task must originate from its own feature branch and be submitted via Merge/Pull Request
    - Every MR requires at least one peer review and approval from a group member before merging. Direct commits to `main` or web uploads are prohibited.
    - Roughly equal contribution across group members is expected and will be audited via Git commit history and PR discussion quality.

## General guidelines

- **Due Date:** Sunday, November 15, 2026, at 11:59 PM
- **Late Policy:** Pre-approved extensions must be requested and approved by Friday, November 13, 2026. Unapproved late submissions incur a 1-point deduction per day.
- **Group Size:** Groups of 2-3 students
- **Project Submission:** Once the project is complete and ready for grading, students will send an email to `joe@adaltas.com` following the guidelines detailed in the [README](https://github.com/adaltas/ece-big-data-processing-fall-2026/blob/main/README.md).
- **AI / LLM Policy:** AI tools may be used for coding (must be declared in `PROJECT.md`). AI tools are **strictly prohibited** for writing explanations or reasoning in `REPORT.md`. Unauthorized use of AI tools will result in 0 points for the question **AND** incur a penalty per occurrence.
- **Report Formatting and Code Style:** Markdown reports must follow the [Markdown Guidelines](https://www.markdownguide.org/) and Python code should follow the [Python Style Guides](https://www.python.org/doc/essays/styleguide/).
- **Core Technology Stack:** Apache Spark (PySpark Structured Streaming & Batch) and Apache Kafka will be used unless otherwise noted in the instructions.  

## Questions/Instructions

### Data preparation 

- For each column of each table in the [IMDB dataset](https://data.imdb.com/non-commercial-datasets/), calculate the percentage of nulls, missing, or empty records.
- Which five columns across the entire dataset have the highest percentage of null values?
- Identify other data quality issues for the dataset such as principals who died before they were born, foreign keys with missing parent records, etc. 
- Design and implement a way to handle these values.

### Data exploration

- Find all feature-length movies by genre whose length is notably different than the average length of movies in that genre.
- What are the top three most popular movie genres for each year between 1980 and 2025? 
- Compare the results from the previous question. How do the top three genres compare to the rest of the genres of movies in a given year?
- List all directors who have directed exactly one movie with a rating >= 8.0 but who have multiple other movies directed with an average rating of <= 6.0. 
- Identify television series with at least three seasons consisting of >=10 episodes where the worst episode occurred in the last season and the best episode occurred within the first two seasons. 
- Find any actors/actresses who have achieved success across films in multiple distinct languages (e.g. been in a successful film originally in French and English). Additionally, list all actors/actresses who have achieved success in movies in >=3 languages (e.g. French, German, and English).
- Find all actors/actresses who have been in movies for 20+ years and at least 15 or more movies whose popularity peaked early in their career (e.g. had their highest rated movies early in their career as compared to later.) 
- Identify film professionals who can be considered "multi-talented" (e.g. some combo of writer/director/actor). How do these individuals perform when performing multiple roles on the same project (e.g. directing and acting in the same movie)?

### Data enrichment 

For all feature-length films released between 1980 and 2025, enrich the dataset by including a film synopsis for each of the top 10 films per genre for each year and biographical information for at least three principals from each film.  

- **Data Origin Rules:** All information must come from human-authored open sources such as Wikipedia or open-license movie databases. Machine generated text is strictly prohibited.
- **Provenance & Ethics:** The pipeline must respect target domain API rate limits and all other scraping policies and every enriched record must maintain an explicit lineage field citing its source URL/URI.
- **Engine Requirements:** Data gathering may be done outside of Spark but all other operations such as cleaning, schema enforcement, etc., must be performed using Spark.
- **Single-source Option:** Pipelines are not expected to hit multiple sources. For instance, if Wikipedia is used as the source and a film does not have a wiki page, it is not required to search a secondary source. However, it is expected that the majority of the information should be available in the chosen datasource, and notes should be programmatically added to the output when information is not currently available. 
- **Scalability:** The processing scope (top 10 films per genre, 3 principals) is intentionally restricted to keep local container memory footprint low. However, your transformation logic, scraping routines, and join functions must be written generically so they can scale to the full IMDb dataset with zero structural code changes.

### Streaming data processing

Design and implement a PySpark Structured Streaming pipeline that ingests live Wikipedia change events via Kafka and identifies updates relevant to your IMDb dataset. 

- You may define your own strategy for filtering or sub setting the incoming stream. However, your filtering criteria must be sufficiently broad to guarantee that multiple matching events are captured and processed within a 15 to 30 minute test window.
- The `REPORT.md` should include an explanation of your chosen method to deal with late arriving data.  

### Open question

Propose and answer a question related to the IMDB and related datasets that was not covered by previous questions.   

- **Nontrivial:** The question must require nontrivial solutions (i.e. it must require some complex analytics such as multi-table joins).
- **Additional Data:** You are free to utilize datasets aside from the IMDB dataset to answer the question provided they follow the guidelines given in the `Data enrichment` section.
- **REPORT.md write up:** 
    1. What is the question being investigated, why is it interesting and what additional data (if any) was required for the investigation.
    2. Results will be presented using clear output tables or summary metrics, along with key takeaways.
    3. As with previous sections, include a write up of the methodology used, why it was chosen, and what alternatives were considered.
