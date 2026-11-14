"""Push Wikimedia's live recent-change events into Kafka (part 4, ingestion).

Run from the repo root, with Kafka up (bash scripts/kafka_local.sh start):

    python -m src.wiki_producer                    # until Ctrl+C
    python -m src.wiki_producer --minutes 30
    python -m src.wiki_producer --minutes 30 --late-share 0.05 --late-delay 600

Wikimedia publishes every edit of every wiki as Server-Sent Events at
STREAM_URL. Each event is sent to Kafka untouched (raw JSON, all wikis):
the filtering is the Spark job's work, not this script's.

--late-share holds back that share of the events for --late-delay seconds
before sending them. Real events from this feed are rarely more than a few
seconds late, so this is how the late-data handling of the Spark job gets
something to do during a 15-30 minute test.
"""

import argparse
import heapq
import http.client
import json
import os
import random
import time
import urllib.error
import urllib.request

from confluent_kafka import Producer

STREAM_URL = "https://stream.wikimedia.org/v2/stream/recentchange"
USER_AGENT = "ece-bigdata-imdb-streaming/1.0 (university course project; non-commercial)"
KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "localhost:9092")
TOPIC = "wiki-recentchange"


def sse_events(last_id=None):
    """Yield (event id, data) pairs from the SSE stream until the connection drops."""
    headers = {"User-Agent": USER_AGENT, "Accept": "text/event-stream"}
    if last_id:
        # lets Wikimedia resume right after the last event we got
        headers["Last-Event-ID"] = last_id
    request = urllib.request.Request(STREAM_URL, headers=headers)
    with urllib.request.urlopen(request, timeout=60) as response:
        event_id, data = None, []
        for raw in response:
            line = raw.decode("utf-8").rstrip("\r\n")
            if not line:
                if data:
                    yield event_id, "\n".join(data)
                data = []
                continue
            if line.startswith(":"):
                continue
            field, _, value = line.partition(":")
            if value.startswith(" "):
                value = value[1:]
            if field == "id":
                event_id = value
            elif field == "data":
                data.append(value)


def main():
    parser = argparse.ArgumentParser(description="Wikimedia recent changes -> Kafka")
    parser.add_argument("--minutes", type=float, default=None, help="stop after this long")
    parser.add_argument("--late-share", type=float, default=0.0, help="share of events held back")
    parser.add_argument("--late-delay", type=float, default=600.0, help="seconds they are held back")
    args = parser.parse_args()

    failures = []
    producer = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP, "linger.ms": 100})

    def delivered(err, _msg):
        if err is not None:
            failures.append(str(err))

    def send(key, value):
        producer.produce(TOPIC, value=value, key=key, on_delivery=delivered)

    deadline = time.time() + args.minutes * 60 if args.minutes else None
    held = []  # (release time, sequence, key, value)
    seq = sent = delayed = 0
    last_id = None
    last_print = time.time()
    print(f"reading {STREAM_URL} -> kafka {KAFKA_BOOTSTRAP} topic {TOPIC}", flush=True)
    done = False
    while not done:
        try:
            for event_id, data in sse_events(last_id):
                last_id = event_id or last_id
                try:
                    event = json.loads(data)
                except json.JSONDecodeError:
                    continue
                # Wikimedia injects test events on a fake "canary" domain
                if event.get("meta", {}).get("domain") == "canary":
                    continue
                key = (event.get("title") or "").encode("utf-8")
                value = data.encode("utf-8")
                now = time.time()
                if args.late_share and random.random() < args.late_share:
                    seq += 1
                    heapq.heappush(held, (now + args.late_delay, seq, key, value))
                    delayed += 1
                else:
                    send(key, value)
                    sent += 1
                while held and held[0][0] <= now:
                    _, _, k, v = heapq.heappop(held)
                    send(k, v)
                    sent += 1
                producer.poll(0)
                if now - last_print >= 30:
                    print(f"  sent {sent}, held back so far {delayed}, waiting {len(held)}", flush=True)
                    last_print = now
                if deadline and now >= deadline:
                    done = True
                    break
            else:
                print("stream closed by server, reconnecting", flush=True)
                time.sleep(2)
        except KeyboardInterrupt:
            done = True
        except urllib.error.HTTPError as err:
            pause = 60 if err.code == 429 else 10
            print(f"HTTP {err.code}, retry in {pause}s", flush=True)
            time.sleep(pause)
        except (urllib.error.URLError, http.client.HTTPException, ConnectionError, TimeoutError, OSError) as err:
            print(f"connection lost ({err}), reconnecting", flush=True)
            time.sleep(5)

    if held:
        # Still late, even more so: send them before leaving.
        print(f"sending {len(held)} held-back events", flush=True)
        for _, _, k, v in held:
            send(k, v)
            sent += 1
    producer.flush(30)
    print(f"done: {sent} events sent, {delayed} of them held back, {len(failures)} delivery errors", flush=True)
    if failures:
        print("first error: " + failures[0], flush=True)


if __name__ == "__main__":
    main()
