# Adaptive docid scheduling + match-phase limiting deadlocks match threads

**Vespa version:** 8.763.13. The code paths involved are unchanged on master.

This repository contains a minimal application that reproduces the problem. See
[How to reproduce](#how-to-reproduce).

## Summary

With `ranking.matching.numSearchPartitions=0` (the adaptive docid scheduler) and a rank profile
with `match-phase`, a query can block its match threads **forever**:

- The query times out on the container side, but the threads on the content node never return.
- Each stuck query keeps its search thread and match threads. When enough queries are stuck, the
  node stops answering **all** queries, including ones that don't use the adaptive scheduler,
  until proton is restarted.
- Neither wait checks the query timeout, so nothing ever releases them.

In the repro, the second adaptive query hangs and the node is unusable from then on.

## What happens

Two waits block each other:

1. **A thread reaches the match-phase sample limit.** `inner_match_loop` calls `maybe_limit`
   ([match_thread.cpp#L211-L213](https://github.com/vespa-engine/vespa/blob/v8.763.13/searchcore/src/vespa/searchcore/proton/matching/match_thread.cpp#L211-L213)),
   which calls `estimate_match_frequency`
   ([match_thread.cpp#L151-L154](https://github.com/vespa-engine/vespa/blob/v8.763.13/searchcore/src/vespa/searchcore/proton/matching/match_thread.cpp#L151-L154)).
   That is a rendezvous of **all** match threads (`EstimateMatchFrequency : vespalib::Rendezvous`,
   [match_loop_communicator.h#L28-L29](https://github.com/vespa-engine/vespa/blob/v8.763.13/searchcore/src/vespa/searchcore/proton/matching/match_loop_communicator.h#L28-L29)).
   The thread blocks until every other thread arrives.
2. **Another thread runs out of work before reaching the limit.** It calls
   `AdaptiveDocidRangeScheduler::next_range`, marks itself idle and waits on its condition
   variable for a donated range
   ([docid_range_scheduler.cpp#L118-L128](https://github.com/vespa-engine/vespa/blob/v8.763.13/searchcore/src/vespa/searchcore/proton/matching/docid_range_scheduler.cpp#L118-L128)).
3. **Nobody can wake it.**
   - Work is only donated by a thread inside `inner_match_loop` (`try_share`), and the thread
     from step 1 is blocked in the rendezvous.
   - `all_work_done` needs every other thread to be idle
     ([docid_range_scheduler.cpp#L81-L84](https://github.com/vespa-engine/vespa/blob/v8.763.13/searchcore/src/vespa/searchcore/proton/matching/docid_range_scheduler.cpp#L81-L84)),
     and the thread in the rendezvous is not idle.
4. **So the idle thread never leaves the match loop.** It never reaches its own
   `estimate_match_frequency` call after the loop
   ([match_thread.cpp#L259-L262](https://github.com/vespa-engine/vespa/blob/v8.763.13/searchcore/src/vespa/searchcore/proton/matching/match_thread.cpp#L259-L262)),
   which the rendezvous is waiting for.

The comment in `match_loop`
([match_thread.cpp#L239-L240](https://github.com/vespa-engine/vespa/blob/v8.763.13/searchcore/src/vespa/searchcore/proton/matching/match_thread.cpp#L239-L240))
already says all threads must complete the loop because schedulers communicate across threads.
The match-phase rendezvous inside the loop breaks that assumption for the adaptive scheduler.
`numSearchPartitions=0` selects that scheduler
([match_master.cpp#L53-L56](https://github.com/vespa-engine/vespa/blob/v8.763.13/searchcore/src/vespa/searchcore/proton/matching/match_master.cpp#L53-L56)).
The static partition (default) and task schedulers don't block in `next_range`, so they're not
affected.

Stacks of a stuck query (4 threads per search), from `uv run repro.py stacks`:

```
# 2 threads: reached the limit, waiting for all threads
#1  std::condition_variable::wait(...)
#2  proton::matching::MatchLoopCommunicator::estimate_match_frequency(...)
#3  proton::matching::MatchThread::estimate_match_frequency(unsigned int, unsigned int)
#4  proton::matching::MatchThread::maybe_limit(...)
#5  proton::matching::MatchThread::inner_match_loop<...>(...)

# 2 threads: out of work, waiting for a donation
#1  std::condition_variable::wait(...)
#2  proton::matching::AdaptiveDocidRangeScheduler::next_range(unsigned long)
#3  proton::matching::MatchThread::match_loop<...>(...)
#4  proton::matching::MatchThread::findMatches(...)
#5  proton::matching::MatchThread::run()
```

## How to reproduce

### What you need

- **Docker** with ~4 GB of memory for the container. Docker Desktop, Rancher Desktop or Colima all work.
- **[uv](https://docs.astral.sh/uv/getting-started/installation/)**, which installs Python 3.11+ and pyvespa for you.
- ~5 minutes.

### Steps

```sh
git clone https://github.com/andrius-jokubauskas/vespa-deadlock.git
cd vespa-deadlock

uv run repro.py deploy   # pulls vespaengine/vespa:8.763.13, starts container "deadlock", deploys ./app
uv run repro.py feed     # feeds 200,000 items (~2 min)
uv run repro.py test     # default, then numSearchPartitions=0 until a query hangs, then default again
uv run repro.py stacks   # optional: gdb stacks of the blocked match threads
```

Expected output of `test`:

```
default                 0.06s totalCount=20000 errors=[]
default                 0.02s totalCount=20000 errors=[]
numSearchPartitions=0 #0  0.02s totalCount=20000 errors=[]
numSearchPartitions=0 #1  5.01s totalCount=0 errors=['Timed out']
default afterwards      5.01s totalCount=0 errors=['Timed out']
default afterwards      5.01s totalCount=0 errors=['Timed out']
default afterwards      5.01s totalCount=0 errors=['Timed out']
```

The hang is a race, so it can take a few adaptive queries. `test` stops at the first one that
times out (up to `ADAPTIVE_TRIES`, default 20).

- **Other Docker engines.** With Rancher Desktop or Colima, point the Docker client to their socket first:
  - `export DOCKER_HOST=unix://$HOME/.rd/docker.sock` (Rancher Desktop)
  - `export DOCKER_HOST=unix://$HOME/.colima/default/docker.sock` (Colima)
- **Options** (environment variables): `VESPA_PORT` (default `8081`), `VESPA_IMAGE` (default
  `vespaengine/vespa:8.763.13`), `N_DOCS` (default `200000`), `HOT_SHARE` (default `0.1`),
  `ADAPTIVE_TRIES` (default `20`).
- **Unstick the node:** `docker restart deadlock`. **Clean up:** `docker rm -f deadlock`.

### What the repro does

- `app/schemas/item.sd`: items with `seq` (the feed order, fast-search), `title` and `popularity`.
  The `limited` rank profile has 4 threads per search and
  `match-phase { attribute: seq, order: descending, max-hits: 1000 }`.
- `app/services.xml`: one content node with 4 search threads, so a few stuck queries are enough
  to make the whole node stop answering.
- Data: only the last 10% of the items (the highest docids) have the title "hot". The query is
  `title contains "hot"`, so most threads run out of work early while one thread reaches the
  match-phase sample limit. That's the interleaving above.

## Possible fixes

- **Short term:** don't combine the two. When the match-phase limiter is enabled, fall back from
  `AdaptiveDocidRangeScheduler` to the task scheduler (or turn off work sharing).
- **Longer term:** make the match-frequency estimate not depend on every thread reaching a
  rendezvous inside the match loop. Or let idle threads in `next_range` take part in it, for
  example by having the scheduler wake idle threads when a thread enters the rendezvous.
- Either way, both waits could check the query's doom, so a bug like this ends at the query timeout
  instead of keeping the threads forever.

We haven't built a patched Vespa. If you agree on a direction, we are happy to open a PR with the
change and a test, and to verify it against this repro.
