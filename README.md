[![Review Assignment Due Date](https://classroom.github.com/assets/deadline-readme-button-22041afd0340ce965d47ae6ef1cefeee28c7c493a6346c4f15d667ab976d596c.svg)](https://classroom.github.com/a/Nvxy3054)
# ETL Pipeline — Amman Digital Market

## Overview

<!-- What does this pipeline do? -->

## Setup

1. Start PostgreSQL container:
   ```bash
   docker run -d --name postgres-m3-int \
     -e POSTGRES_USER=postgres -e POSTGRES_PASSWORD=postgres \
     -e POSTGRES_DB=amman_market \
     -p 5432:5432 -v pgdata_m3_int:/var/lib/postgresql/data \
     postgres:15-alpine
   ```
2. Load schema and data:
   ```bash
   psql -h localhost -U postgres -d amman_market -f schema.sql
   psql -h localhost -U postgres -d amman_market -f seed_data.sql
   ```
3. Install dependencies: `pip install -r requirements.txt`

## How to Run

```bash
python etl_pipeline.py
```

## Output

<!-- What does customer_analytics.csv contain? -->

## Quality Checks

<!-- What validations are performed and why? -->

---
## Tier 2:

## Full vs Incremental Run Comparison

The pipeline was tested in both full-load mode and incremental-load mode.

### Full run
- Rows processed: 85
- Execution time: 0.24 seconds

### Incremental run
- Rows processed: 0
- Execution time: 0.33 seconds

### Tradeoffs
- A full load is simpler because it processes the entire dataset every time.
- A full load is useful for initial setup and for rebuilding analytics from scratch.
- An incremental load is faster because it only processes data newer than the last successful ETL run.
- Incremental ETL reduces repeated work, but it requires extra logic and metadata tracking.
- Incremental ETL can return 0 rows when no new data is available, which is expected behavior.

## License

This repository is provided for educational use only. See [LICENSE](LICENSE) for terms.

You may clone and modify this repository for personal learning and practice, and reference code you wrote here in your professional portfolio. Redistribution outside this course is not permitted.
