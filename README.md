# Rhombus AI Technical Assessment — Distributed NL-to-Regex Data Processing Platform

Upload a CSV/Excel file, describe a pattern in plain English ("find email addresses"),
and this app finds and replaces it across every row — asynchronously, and scaled to
millions of rows via PySpark.

**Demo video:** (https://youtu.be/pkUTT8xybaQ)
**Live URL:** _[add your deployed URL here before submitting]_

---

## Architecture

```
┌─────────────┐      1. POST /api/jobs/ (file + prompt)     ┌──────────────┐
│   React     │ ───────────────────────────────────────────▶│   Django     │
│  (Vite)     │                                              │   REST API   │
│             │◀──── 2. { job_id, status: QUEUED } ──────────│              │
└─────────────┘        (returns immediately, no blocking)    └──────┬───────┘
      │                                                             │ 3. dispatch task
      │ 4. poll GET /status/ every 1.5s                             ▼
      │                                                      ┌──────────────┐
      │                                                      │    Redis     │
      │                                                      │ (broker +    │
      │                                                      │  cache)      │
      │                                                      └──────┬───────┘
      │                                                             │ 5. picked up by
      │                                                             ▼
      │                                                      ┌──────────────┐
      │                                                      │ Celery worker│
      │                                                      │  ┌────────┐  │
      │                                                      │  │  LLM   │  │ 6. NL -> regex
      │                                                      │  │ (Claude)│ │    (cached in Redis)
      │                                                      │  └────────┘  │
      │                                                      │  ┌────────┐  │
      │                                                      │  │ PySpark│  │ 7. distributed
      │                                                      │  │ engine │  │    regexp_replace
      │                                                      │  └────────┘  │
      │                                                      └──────┬───────┘
      │                                                             │ 8. writes Parquet
      │ 9. GET /result/?page=N                                      ▼
      │    (DuckDB reads only the requested page              ┌──────────────┐
      └────────────────────────────────────────────────────── │   Parquet    │
                                                                │ output files │
                                                                └──────────────┘
```

### Why each piece exists

- **Django** is the API/control-plane layer only. It never does file parsing or
  regex work inline — it creates a `Job` row and hands off to Celery, which is what
  lets `POST /api/jobs/` return in milliseconds regardless of file size.
- **Celery + Redis** decouple "a request came in" from "the work got done." Redis
  plays two separate roles here (kept on separate logical DBs: `redis://redis:6379/0`
  for the Celery broker/result store, `/1` for the regex cache) so they can be
  reasoned about and scaled independently.
- **PySpark** is the actual data-transformation engine. `regexp_replace` is applied
  as a Spark SQL transformation across partitions — this is declarative and
  parallel, as opposed to a Python `for row in rows` loop, which is why this
  approach doesn't fall over as row counts grow into the millions.
- **DuckDB** on the read side: rather than loading the full Spark output back into
  Django's memory to paginate it, the result endpoint runs a zero-copy
  `SELECT * FROM read_parquet(...) LIMIT x OFFSET y` directly against the Parquet
  files Spark wrote. A page of 25 rows out of a 5-million-row result costs a cheap
  columnar seek, not a full load.

### Partitioning choice (Spark)

The dataframe is repartitioned to `SPARK_SHUFFLE_PARTITIONS` (default 8) before the
transform, splitting it into an accumulator-tracked batch per partition. On a single
multi-core machine, 8 is a reasonable default: enough to use all cores in parallel
without adding excessive task-scheduling overhead. In a real multi-worker cluster,
this would instead be tuned relative to total core count across the cluster and a
target partition size (~128MB/partition is the common Spark rule of thumb), rather
than a fixed constant — noted here as a deliberate scope trade-off for a local/single-
node deployment.

### Progress reporting

True per-row progress isn't something Spark exposes cheaply out of the box. This
implementation uses a Spark `Accumulator` that each partition increments as it
finishes counting/scanning its rows, polled from a background thread inside the
Celery task every 500ms and surfaced via `self.update_state(state="PROGRESS", ...)`.
It's coarse (partition-granularity, not row-granularity) but it's a real signal, not
a fake progress bar — documented here as a known limitation rather than glossed over.

### Regex safety (LLM output is untrusted input)

Every LLM-generated pattern goes through `jobs/regex_utils.py` before it ever touches
real data:
1. **Syntax check** — must compile as a valid Python regex.
2. **ReDoS guard** — run against a battery of known catastrophic-backtracking
   trigger strings, each in an isolated subprocess with a hard 1-second timeout. If
   it can't finish fast on all of them, it's rejected. This can't *prove* a pattern
   is safe in the general case (that's undecidable), but it catches the textbook
   exponential-blowup patterns like `(a+)+$`. A production system handling
   adversarial input at scale might reach for Google's RE2 engine instead, which
   guarantees linear-time matching by construction — noted here as the natural next
   step.

### LLM integration + caching

`jobs/llm.py` calls Claude with a system prompt constraining it to return *only* the
raw pattern. Results are cached in Redis keyed by a SHA-256 hash of the (lowercased,
trimmed) prompt, so identical requests never re-hit the LLM. **If no
`ANTHROPIC_API_KEY` is set, the app still works** via a small rule-based fallback
library (email, phone, URL, date, ZIP, IPv4, hashtag, currency) — useful for offline
development and demos without burning API credits.

---

## Project layout

```
rhombus-regex-app/
├── docker-compose.yml
├── .env.example
├── backend/                  # Django + Celery + PySpark
│   ├── config/                (settings, celery app, urls)
│   └── jobs/
│       ├── models.py           Job model (status/progress/results)
│       ├── views.py            REST endpoints
│       ├── tasks.py            Celery orchestration task
│       ├── llm.py               NL -> regex (Claude + Redis cache + fallback)
│       ├── regex_utils.py       syntax + ReDoS validation
│       └── spark_engine.py      the distributed transform engine
└── frontend/                 # React (Vite)
    └── src/
        ├── App.jsx
        ├── api.js
        └── components/
```

---

## Running it locally

### Prerequisites
- [Docker Desktop](https://www.docker.com/products/docker-desktop/) installed and running
- That's genuinely it — Docker Compose brings up Postgres, Redis, the Django API,
  the Celery worker, Flower (monitoring), and the React frontend for you.

### Steps
```bash
git clone <your-repo-url>
cd rhombus-regex-app
cp .env.example .env
# optional: open .env and paste in a real ANTHROPIC_API_KEY

docker compose up --build
```

Then open:
- **App:** http://localhost:3000
- **API:** http://localhost:8000/api/jobs/
- **Django admin:** http://localhost:8000/admin/ (create a superuser first, see below)
- **Flower (Celery monitoring):** http://localhost:5555

To create a Django admin user (optional, for poking at the DB directly):
```bash
docker compose exec backend python manage.py createsuperuser
```

### Testing with a large file

To prove the pipeline holds up at scale, generate a synthetic multi-million-row CSV:
```bash
docker compose exec backend python -c "
import csv, random
random.seed(0)
with open('/app/media/big_sample.csv', 'w', newline='') as f:
    w = csv.writer(f)
    w.writerow(['ID', 'Name', 'Email'])
    for i in range(2_000_000):
        w.writerow([i, 'User', f'user{i}@example.com'])
"
```
Then upload `backend/media/big_sample.csv` (mounted into the container) through the
UI, describe the pattern as "find email addresses," and watch the progress bar and
Flower dashboard while it runs.

---

## Deploying it publicly

The assessment asks for a working public URL. The simplest path that matches this
docker-compose setup:
- **Backend + worker + Redis + Postgres:** [Railway](https://railway.app) or
  [Render](https://render.com) — both can deploy directly from a `docker-compose.yml`
  or from separate services per container, and both have free/low-cost tiers.
- **Frontend:** deploy the `frontend/` folder to [Vercel](https://vercel.com) or
  [Netlify](https://netlify.com), setting `VITE_API_BASE_URL` to your deployed
  backend's public URL.

Whichever you use, remember to set `DJANGO_ALLOWED_HOSTS` and CORS origins to your
real deployed domain rather than leaving them wide open, and set a real
`DJANGO_SECRET_KEY`.

---

## Known trade-offs (called out deliberately, not accidentally)

- Excel (`.xlsx`) files are parsed with pandas before being handed to Spark, since
  vanilla PySpark has no native distributed Excel reader. CSV, by contrast, is read
  by Spark directly and is the truly distributed path. For very large Excel files,
  converting to CSV upstream (or using the `spark-excel` connector) would be the
  production-grade fix.
- Progress reporting is partition-granular, not row-granular (see above).
- This runs Spark in local mode (`local[*]`) inside a single container for a
  self-contained one-command demo. Pointing `SPARK_MASTER` at a real standalone or
  YARN/Kubernetes cluster is a config change, not a code change — `spark_engine.py`
  doesn't know or care how many physical machines are behind `SparkSession`.
- The ReDoS guard is a heuristic safety net, not a formal proof — see above.
 
