"""Polite client for Wikidata and the English Wikipedia (part 3, gathering step).

Gathering is the one step the course allows outside Spark. Everything here is
plain Python: it reads IMDb ids, writes JSON lines, and never touches Spark.

Two lookups, both resumable (a line already in the cache file is not fetched
again):

1. Wikidata SPARQL maps an IMDb id (property P345) to the English Wikipedia
   article of the same film or person. The id is exact, so nothing is guessed
   from a title.
2. The MediaWiki API returns the article's lead text and, for films, its
   wikitext, from which the "Plot" section is cut out.

Rate policy: one request at a time, at least MIN_INTERVAL seconds apart,
`maxlag` on every API call, `Retry-After` honoured on 429/503, and a descriptive
User-Agent.
"""

import json
import os
import re
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timezone

import mwparserfromhell

USER_AGENT = "ece-bigdata-imdb-enrichment/1.0 (university course project; non-commercial)"
SPARQL_URL = "https://query.wikidata.org/sparql"
API_URL = "https://en.wikipedia.org/w/api.php"
WIKI_BASE = "https://en.wikipedia.org/wiki/"

MIN_INTERVAL = 1.0
MAX_RETRIES = 6
# Wikidata's query service currently allows about one request per minute,
# so each SPARQL request carries many ids and the client waits 65 s between them.
SPARQL_BATCH = 400
SPARQL_INTERVAL = 65.0
PAGE_BATCH = 20
MAX_LAG = 5

# Level-2 headings that hold the story of a film.
PLOT_HEADINGS = ("plot", "synopsis", "plot summary", "premise", "story", "storyline", "summary")
PLOT_RE = re.compile(
    r"^==\s*(?P<h>" + "|".join(re.escape(h) for h in PLOT_HEADINGS) + r")\s*==\s*$",
    re.IGNORECASE | re.MULTILINE,
)
NEXT_H2_RE = re.compile(r"^==[^=].*$", re.MULTILINE)
MEDIA_LINE_RE = re.compile(r"^\s*\[\[(File|Image):.*$", re.IGNORECASE | re.MULTILINE)
SUBHEADING_RE = re.compile(r"^=+[^=].*?=+\s*$", re.MULTILINE)


def now():
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


class Client:
    def __init__(self, min_interval=MIN_INTERVAL):
        self.min_interval = min_interval
        self._last = 0.0
        self.requests = 0

    def _wait(self, interval):
        gap = time.time() - self._last
        if gap < interval:
            time.sleep(interval - gap)
        self._last = time.time()

    def get_json(self, url, params, accept="application/json", post=False, interval=None):
        encoded = urllib.parse.urlencode(params)
        full = url if post else url + "?" + encoded
        request = urllib.request.Request(
            full,
            data=encoded.encode() if post else None,
            headers={"User-Agent": USER_AGENT, "Accept": accept},
        )
        for attempt in range(MAX_RETRIES):
            self._wait(self.min_interval if interval is None else interval)
            self.requests += 1
            try:
                with urllib.request.urlopen(request, timeout=90) as response:
                    data = json.load(response)
            except urllib.error.HTTPError as err:
                if err.code in (429, 500, 502, 503, 504):
                    pause = err.headers.get("Retry-After")
                    time.sleep(min(float(pause), 1200) if pause and pause.isdigit() else 5 * 2**attempt)
                    continue
                raise
            except (urllib.error.URLError, TimeoutError, json.JSONDecodeError):
                time.sleep(5 * 2**attempt)
                continue
            if isinstance(data, dict) and data.get("error", {}).get("code") == "maxlag":
                time.sleep(5 * 2**attempt)
                continue
            return data
        raise RuntimeError("giving up on " + full[:200])


def chunks(items, size):
    for start in range(0, len(items), size):
        yield items[start : start + size]


def read_jsonl(path):
    if not os.path.exists(path):
        return []
    with open(path, encoding="utf-8") as handle:
        return [json.loads(line) for line in handle if line.strip()]


def append_jsonl(path, records):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "a", encoding="utf-8") as handle:
        for record in records:
            handle.write(json.dumps(record, ensure_ascii=False) + "\n")


# --- 1. IMDb id -> Wikipedia article -----------------------------------------

def sparql_query(ids):
    values = " ".join('"' + i + '"' for i in ids)
    return (
        "SELECT ?imdb ?item ?article WHERE { VALUES ?imdb { " + values + " } "
        "?item wdt:P345 ?imdb . "
        "OPTIONAL { ?article schema:about ?item ; "
        "schema:isPartOf <https://en.wikipedia.org/> . } }"
    )


def resolve_ids(client, ids, path):
    """Append one record per IMDb id: its Wikidata item and English article, or nulls."""
    done = {r["imdb_id"] for r in read_jsonl(path)}
    todo = sorted(set(ids) - done)
    print(f"  resolve: {len(todo)} ids to look up ({len(done)} cached)", flush=True)
    for number, batch in enumerate(chunks(todo, SPARQL_BATCH), 1):
        data = client.get_json(
            SPARQL_URL,
            {"query": sparql_query(batch), "format": "json"},
            accept="application/sparql-results+json",
            post=True,
            interval=SPARQL_INTERVAL,
        )
        found = {}
        for row in data["results"]["bindings"]:
            imdb = row["imdb"]["value"]
            article = row.get("article", {}).get("value")
            item = row["item"]["value"]
            # an id can sit on two items; prefer the one with an article, then the lowest Q number
            rank = (article is None, int(item.rsplit("/Q", 1)[1]))
            if imdb not in found or rank < found[imdb][0]:
                found[imdb] = (rank, item, article)
        stamp = now()
        records = []
        for imdb in batch:
            _, item, article = found.get(imdb, (None, None, None))
            title = None
            if article:
                title = urllib.parse.unquote(article[len(WIKI_BASE):]).replace("_", " ")
            records.append(
                {
                    "imdb_id": imdb,
                    "wikidata_uri": item,
                    "wikipedia_title": title,
                    "resolved_at": stamp,
                }
            )
        append_jsonl(path, records)
        if number % 10 == 0:
            print(f"    resolve batch {number}/{(len(todo) + SPARQL_BATCH - 1) // SPARQL_BATCH}", flush=True)


# --- 2. article text ---------------------------------------------------------

def clean_wikitext(text):
    """Turn a wikitext section into plain prose (templates, refs and links removed)."""
    text = re.sub(r"<!--.*?-->", "", text, flags=re.DOTALL)
    text = re.sub(r"<ref\b[^>]*/\s*>", "", text, flags=re.IGNORECASE)
    text = re.sub(r"<ref\b[^>]*>.*?</ref\s*>", "", text, flags=re.IGNORECASE | re.DOTALL)
    text = MEDIA_LINE_RE.sub("", text)
    text = SUBHEADING_RE.sub("", text)
    plain = mwparserfromhell.parse(text).strip_code(normalize=True, collapse=True)
    plain = re.sub(r"<!--.*?-->", "", plain, flags=re.DOTALL)
    plain = re.sub(r"[ \t]+", " ", plain)
    plain = re.sub(r"\n{3,}", "\n\n", plain)
    return plain.strip()


def plot_section(wikitext):
    """The text under the first Plot/Synopsis-like level-2 heading, or None."""
    if not wikitext:
        return None
    start = PLOT_RE.search(wikitext)
    if not start:
        return None
    rest = wikitext[start.end():]
    stop = NEXT_H2_RE.search(rest)
    body = rest[: stop.start()] if stop else rest
    plain = clean_wikitext(body)
    return plain or None


def page_url(title, revid):
    quoted = urllib.parse.quote(title.replace(" ", "_"), safe="()_,'!*-.")
    if revid:
        return f"https://en.wikipedia.org/w/index.php?title={quoted}&oldid={revid}"
    return WIKI_BASE + quoted


def fetch_batch(client, titles, with_wikitext):
    """Lead extract (and wikitext) for up to PAGE_BATCH titles, keyed by the requested title."""
    props = "extracts|revisions"
    params = {
        "action": "query",
        "format": "json",
        "formatversion": "2",
        "maxlag": str(MAX_LAG),
        "redirects": "1",
        "titles": "|".join(titles),
        "prop": props,
        "exintro": "1",
        "explaintext": "1",
        "exlimit": str(PAGE_BATCH),
        "rvprop": "ids|content" if with_wikitext else "ids",
        "rvslots": "main",
    }
    data = client.get_json(API_URL, params)["query"]
    renamed = {}
    for key in ("normalized", "redirects"):
        for hop in data.get(key, []):
            renamed[hop["from"]] = hop["to"]
    by_title = {p["title"]: p for p in data.get("pages", [])}
    out = {}
    for requested in titles:
        final = requested
        for _ in range(3):
            final = renamed.get(final, final)
        out[requested] = by_title.get(final)
    return out


def fetch_pages(client, kind, targets, path):
    """targets: [{imdb_id, wikipedia_title}]. Appends one record per article."""
    done = {r["imdb_id"] for r in read_jsonl(path)}
    todo = [t for t in targets if t["wikipedia_title"] and t["imdb_id"] not in done]
    print(f"  {kind}: {len(todo)} articles to fetch ({len(done)} cached)", flush=True)
    for number, batch in enumerate(chunks(todo, PAGE_BATCH), 1):
        pages = fetch_batch(client, [t["wikipedia_title"] for t in batch], kind == "film")
        stamp = now()
        records = []
        for target in batch:
            page = pages.get(target["wikipedia_title"])
            missing = page is None or page.get("missing") or page.get("invalid")
            revision = None if missing else (page.get("revisions") or [None])[0]
            wikitext = None
            if revision and kind == "film":
                wikitext = revision.get("slots", {}).get("main", {}).get("content")
            title = target["wikipedia_title"] if missing else page["title"]
            revid = revision.get("revid") if revision else None
            records.append(
                {
                    "imdb_id": target["imdb_id"],
                    "kind": kind,
                    "page_title": None if missing else title,
                    "revision_id": revid,
                    "source_url": None if missing else page_url(title, revid),
                    "lead": None if missing else (page.get("extract") or None),
                    "plot": plot_section(wikitext) if kind == "film" else None,
                    "retrieved_at": stamp,
                }
            )
        append_jsonl(path, records)
        if number % 10 == 0:
            print(f"    {kind} batch {number}/{(len(todo) + PAGE_BATCH - 1) // PAGE_BATCH}", flush=True)
