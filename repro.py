"""Reproduce: adaptive docid scheduling (numSearchPartitions=0) + match-phase limiting deadlocks match threads.

    uv run repro.py deploy   # start Vespa in Docker (container "deadlock") and deploy ./app
    uv run repro.py feed     # N_DOCS items; "hot" titles only in the last HOT_SHARE of the docid space
    uv run repro.py test     # default scheduler, then adaptive until a query hangs, then default again
    uv run repro.py stacks   # gdb stacks of the blocked match threads
"""

import os
import random
import subprocess
import sys
import time
from pathlib import Path

import requests
from vespa.application import Vespa
from vespa.deployment import VespaDocker

ROOT = Path(__file__).parent
IMAGE = os.environ.get("VESPA_IMAGE", "vespaengine/vespa:8.763.13")
PORT = int(os.environ.get("VESPA_PORT", "8081"))
CONTAINER = "deadlock"
N_DOCS = int(os.environ.get("N_DOCS", "200000"))
HOT_SHARE = float(os.environ.get("HOT_SHARE", "0.1"))
ADAPTIVE_TRIES = int(os.environ.get("ADAPTIVE_TRIES", "20"))
SEARCH = f"http://localhost:{PORT}/search/"


def deploy():
    VespaDocker(port=PORT, container_image=IMAGE, cfgsrv_port=19072).deploy_from_disk(
        application_name=CONTAINER, application_root=ROOT / "app")


def docs():
    rng = random.Random(1)
    for i in range(N_DOCS):
        hot = i >= N_DOCS * (1 - HOT_SHARE)
        yield {"id": str(i), "fields": {"seq": i, "title": "hot" if hot else "cold", "popularity": rng.random()}}


def feed():
    errors = []
    t0 = time.time()
    # Fed in id order, so local docids roughly follow seq on a fresh node.
    Vespa(url="http://localhost", port=PORT).feed_async_iterable(
        docs(), schema="item", namespace="repro", max_workers=64, max_connections=4,
        callback=lambda r, i: None if r.is_successful() else errors.append(i))
    print(f"fed {N_DOCS} docs in {time.time() - t0:.0f}s, {len(errors)} errors")


def query(partitions=None, timeout="5s"):
    """Returns (ok, line). ok is False when the query timed out or failed."""
    params = {"yql": 'select * from item where title contains "hot"', "ranking": "limited", "hits": 10,
              "timeout": timeout}
    if partitions is not None:
        params["ranking.matching.numSearchPartitions"] = partitions
    t0 = time.time()
    try:
        root = requests.get(SEARCH, params=params, timeout=30).json()["root"]
    except requests.RequestException as e:
        return False, f"{time.time() - t0:5.2f}s client error {type(e).__name__}"
    errors = [e.get("summary") for e in root.get("errors", [])]
    return not errors, f"{time.time() - t0:5.2f}s totalCount={root['fields'].get('totalCount')} errors={errors}"


def test():
    for _ in range(2):
        print("default               ", query()[1])
    for i in range(ADAPTIVE_TRIES):
        ok, line = query(partitions=0)
        print(f"numSearchPartitions=0 #{i}", line)
        if not ok:
            break
    else:
        print(f"no hang in {ADAPTIVE_TRIES} tries")
        return
    for _ in range(3):
        print("default afterwards    ", query()[1])
    print(f"\nThe node stays stuck until proton restarts: docker restart {CONTAINER}")


def stacks():
    gdb = ("gdb -p $(pgrep -f vespa-proton-bin) -batch -ex 'thread apply all bt 8' 2>/dev/null"
           " | grep -B1 -A6 -E 'AdaptiveDocidRangeScheduler::next_range|MatchLoopCommunicator::estimate_match_frequency'"
           " | grep -E '^Thread|^#[0-5] '")
    subprocess.run(["docker", "exec", "-u", "root", CONTAINER, "bash", "-c", gdb], check=False)


if __name__ == "__main__":
    {"deploy": deploy, "feed": feed, "test": test, "stacks": stacks}[sys.argv[1]]()
