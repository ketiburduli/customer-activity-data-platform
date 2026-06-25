# Data Platform Engineer — Case Study Harness

This bundle is the runnable starting point for the take-home. It gives you a local
**ClickHouse** to build against, the **sample data**, and an **optional Kafka** path if
you'd rather consume the records from topics. Read it alongside the case-study brief
(`Data_Platform_Engineer_Case_Study.docx`), which is the source of truth for what to build.

> **You are not asked to fill in our boilerplate.** This repo is plumbing only — a database
> to load into and the input data. The schema (Part A), the pipeline (Part B) and the
> identity logic (Part C) are yours to design and add wherever you like.

---

## Prerequisites

- Docker + Docker Compose (Compose v2 — `docker compose ...`).
- Python 3.10+ if you build your pipeline in Python (recommended, but your choice).
- `make` is optional — every target maps to a plain `docker compose` command.

---

## Quick start (ClickHouse — the default path)

```bash
make up            # or: docker compose up -d clickhouse
```

This starts ClickHouse and creates an empty `warehouse` database for you.

| Setting | Value |
|---|---|
| HTTP endpoint | `http://localhost:8123` |
| Native endpoint | `localhost:9000` |
| Database | `warehouse` |
| User / password | `dpe` / `dpe` |

Open a SQL shell:

```bash
make ch            # or: docker compose exec clickhouse clickhouse-client --user dpe --password dpe -d warehouse
```

Sanity check over HTTP:

```bash
curl -s 'http://localhost:8123/?user=dpe&password=dpe' --data-binary 'SELECT version()'
```

Tear down (removes the container and its data volume):

```bash
make down          # or: docker compose down -v
```

---

## The data

Everything lives in [`sample-data/`](./sample-data) as **newline-delimited JSON**
(one object per line). Start with [`sample-data/_dictionary.md`](./sample-data/_dictionary.md)
for the field-level contract.

| File | Source | Grain |
|---|---|---|
| `pam_customers.json` | PAM (customer system) | one row per PAM account |
| `app_customers.json` | Activity platform | one row per platform account |
| `activity_events.json` | Activity platform | one row per customer per event |
| `customer_sessions.json` | PAM (customer system) | one row per session |

The data **intentionally** contains duplicate, late/out-of-order, malformed, and
identity-edge records. Handle them in your pipeline per the brief — **don't edit the
files to clean them out**. The dictionary lists what's planted.

---

## Optional: the Kafka streaming path

Off by default. Use it only if you want to consume the same records from topics instead
of reading the files. The contract-conformance logic is identical either way, so skipping
this costs you nothing.

```bash
make kafka-up      # or: docker compose --profile kafka up -d
make produce       # or: docker compose --profile kafka run --rm producer
```

`produce` replays each file onto a topic named after it (`pam_customers`, `app_customers`,
`activity_events`, `customer_sessions`), one message per line, raw JSON bytes — so the
planted edge cases survive. Broker is at `localhost:9092`.

---

## Prefer not to run ClickHouse?

That's fine. As the brief says, a local **SQLite/DuckDB** target is acceptable if time is
tight — just note in your write-up what you'd change for ClickHouse (engine, sort key,
partitioning). You won't lose points for the fallback, only for not explaining the
difference.

---

## What to submit

Mirror the deliverables in the brief:

1. **Schema DDL** for the warehouse slice (Part A) — with grain, engine, sort key and
   money decisions in comments.
2. **Runnable pipeline** that loads the sample data idempotently and handles the planted
   edge cases (Part B).
3. **Identity reconciliation** logic + the populated source→canonical mapping (Part C).
4. **Three KPI queries** with the values you got (Part D).
5. **A 1–2 page design note** (Part D).
6. A short **README** for your own code: how to run it, plus a "what I'd do with another
   day" section.

Put it all in a single Git repo (or a zip). Keep it runnable; don't commit data dumps or
build artifacts.

---

## Layout

```
.
├── Data_Platform_Engineer_Case_Study.docx   # the brief — read this first
├── README.md                 # this file
├── docker-compose.yml        # ClickHouse (default) + optional kafka profile
├── Makefile                  # convenience targets (make help)
├── clickhouse/
│   └── init/
│       └── 001_create_database.sql   # creates empty `warehouse` db (no tables)
├── kafka/
│   └── produce.py            # optional: replay sample files onto topics
└── sample-data/
    ├── _dictionary.md
    ├── pam_customers.json
    ├── app_customers.json
    ├── activity_events.json
    └── customer_sessions.json
```
