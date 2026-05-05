# Inclusive Mobility: Accessibility and Weather Risk Analysis

Airflow project for Groupe Lab 3 by Yuefan Liu and Mouzheng Li.

This project supports mobility and inclusion for people with disabilities by combining public place accessibility data with daily weather conditions. It produces daily mobility scores to identify safer accessible places, highlight risky areas during bad weather, and help local communities prioritize accessibility improvements.

## Architecture Choice

The project follows the **Spark architecture** from `Big Data Project.pdf`.

We do **not** use the DBT architecture. DBT is only mentioned in the PDF as an alternative route. Our transformation and combination layers will use Spark, orchestrated by Airflow.

Fixed route:

```text
REST APIs
  -> Airflow ingestion jobs
  -> Data Lake raw layer
  -> Spark formatting jobs
  -> Data Lake formatted layer
  -> Spark combination/scoring jobs
  -> Data Lake usage layer
  -> Elasticsearch indexing
  -> Kibana dashboard
```

Optional bonus route:

```text
Kafka realtime ingestion
  -> Spark streaming or consumer jobs
  -> Data Lake / Elasticsearch
  -> realtime dashboard
```

## Project Sources

| Source | Purpose | Refresh | Link |
| --- | --- | --- | --- |
| AccesLibre API | French public accessibility database for public establishments, including wheelchair access, ramps, steps, accessible toilets and parking. | Public reference data | <https://www.data.gouv.fr/dataservices/api-acces-libre> |
| Open-Meteo API | Free weather API for temperature, precipitation, wind speed and weather conditions. | Daily / hourly | <https://open-meteo.com/en/docs> |

## Reusable Progress Tracker

This table is the main project tracking source. Every stage update must keep this table current.

| Stage | PDF Requirement | Goal | Main Tasks | Expected Deliverable | Verification | Status | Commit |
| --- | --- | --- | --- | --- | --- | --- | --- |
| 0. Topic and sources | Step 1.1, Step 1.2 | Define theme and at least two data sources. | Choose inclusive mobility theme; choose AccesLibre and Open-Meteo. | Theme and API sources documented. | README review. | Done | `3b23dc4` |
| 1. Airflow skeleton | Step 2 Data Pipeline | Create one Airflow DAG and clean project structure. | Remove classroom examples; create DAG; create `dags/lib`; create Data Lake folders; add base tests. | `inclusive_mobility_daily_pipeline` loads in Airflow. | `airflow dags list-import-errors`; `airflow dags test`. | Done | `3b23dc4` |
| 2. Spark/Kafka infrastructure | Spark architecture, Kafka bonus | Prepare Spark route and optional realtime route. | Add Spark master/worker; add Kafka/Zookeeper; add workspace folders; verify services. | Docker stack includes Airflow, Spark, Kafka and Zookeeper. | `docker compose ps`; Spark version; Kafka topic list. | Done | `f47f521` |
| 3. Raw ingestion | Step 2.1 Ingestion | Fetch N data sources through REST APIs into raw Data Lake files. | Implement AccesLibre fetcher; implement Open-Meteo fetcher; store raw JSON; handle API parameters and pagination. | Raw API files under `datalake/raw/...`. | DAG test; file existence checks; raw JSON preview. | Done | `5077f6a` |
| 4. Spark formatting | Step 2.2 Formatting | Normalize raw data and write parquet files. | Create Spark jobs; normalize fields; clean dates; select useful columns; write parquet. | Parquet files under `datalake/formatted/...`. | Spark job run; parquet schema checks. | Planned |  |
| 5. Spark combination and mobility score | Step 2.3 Combination | Join sources and create useful output. | Join accessibility and weather data; compute accessibility score, weather risk score and mobility score; create risk/prioritization outputs. | Usage parquet outputs under `datalake/usage/...`. | Spark job run; sample output checks. | Planned |  |
| 6. Elasticsearch indexing | Step 2.4 Indexing | Expose final output to a search/dashboard layer. | Add Elasticsearch service; index usage outputs; define index mappings if needed. | Indexed mobility results. | Elasticsearch query returns documents. | Planned |  |
| 7. Kibana dashboard | Data Viz / Dashboarding | Build dashboard on top of final result. | Add Kibana service; create visualizations for mobility scores, risky areas and improvement priorities. | Kibana dashboard. | Dashboard opens and displays indexed data. | Planned |  |
| 8. Kafka realtime bonus | Realtime via Kafka bonus | Add optional near-realtime update flow. | Create Kafka topics; create producer/consumer scripts; optionally connect Spark streaming. | Kafka-based refresh path. | Produce and consume sample messages in under 1 minute. | Optional |  |
| 9. Final deliverables | Deliverable section | Prepare final hand-in package. | Write max 10-page PDF; record max 10-minute video; prepare code zip; final README cleanup. | PDF, video and code zip. | Final run from Airflow DAG; deliverable review. | Planned |  |

## Score Mapping

| Score Area | How This Project Covers It | Status |
| --- | --- | --- |
| Ingestion into Data Lake | AccesLibre and Open-Meteo raw JSON files. | Done |
| Realtime via Kafka | Kafka service is installed; realtime path is optional bonus. | Optional |
| Formatting to parquet | Spark formatting jobs will write parquet. | Planned |
| Field normalization | Spark formatting will clean columns and date/time fields. | Planned |
| Use Spark | Spark services are installed; formatting and combination will use Spark. | In progress |
| Combination output | Mobility score, risky areas and improvement priorities. | Planned |
| Indexing | Elasticsearch indexing stage. | Planned |
| Dashboard | Kibana dashboard stage. | Planned |
| Clean naming convention | Fixed Data Lake convention below. | In progress |
| Run all at once | One Airflow DAG will orchestrate ingestion, formatting, combination and indexing. | Planned |
| Innovative output | Inclusive mobility score for accessibility and weather risk. | Planned |
| DBT bonus | Not used. We choose Spark route instead. | Not applicable |

## Stage Details

### Stage 0: Topic and Sources

Status: done.

Completed:

- Chose theme: Inclusive Mobility, Accessibility and Weather Risk Analysis.
- Chose AccesLibre as accessibility source.
- Chose Open-Meteo as weather source.
- Confirmed that Open-Meteo can refresh at least daily.

### Stage 1: Airflow Skeleton

Status: done.

Completed:

- Removed classroom DAG files and old helper/test files.
- Added `inclusive_mobility_daily_pipeline`.
- Added reusable modules under `dags/lib`.
- Added `datalake/raw`, `datalake/formatted`, and `datalake/usage`.
- Disabled Airflow example DAGs.
- Mounted local Data Lake into Airflow containers.
- Added `.env` with `AIRFLOW_UID=50000`.
- Deleted old classroom DAG metadata from Airflow.

Verification:

```bash
docker compose ps
docker compose exec airflow-scheduler airflow dags list-import-errors
docker compose exec airflow-scheduler airflow dags test inclusive_mobility_daily_pipeline 2026-05-05
```

### Stage 2: Spark and Kafka Infrastructure

Status: done.

Completed:

- Added `spark-master` and `spark-worker` using `apache/spark:3.5.1`.
- Added `zookeeper` using `confluentinc/cp-zookeeper:7.6.1`.
- Added `kafka` using `confluentinc/cp-kafka:7.6.1`.
- Added Kafka and Zookeeper named volumes.
- Added `spark/jobs`, `spark/notebooks`, `kafka/producers`, and `kafka/consumers`.
- Removed unused exited one-off Airflow worker containers from previous temporary runs.
- Kept `airflow-init` because an exited code `0` is normal for the init service.

Local ports:

| Service | Local URL or port |
| --- | --- |
| Airflow UI | <http://localhost:8080> |
| Spark master UI | <http://localhost:8081> |
| Spark worker UI | <http://localhost:8082> |
| Spark master endpoint | `spark://localhost:7077` |
| Kafka broker | `localhost:9092` |
| Zookeeper | `localhost:2181` |

Verification:

```bash
docker compose config --services
docker compose up -d zookeeper kafka spark-master spark-worker
docker compose ps
docker compose exec spark-master /opt/spark/bin/spark-submit --version
docker compose exec kafka kafka-topics --bootstrap-server localhost:9092 --list
```

### Stage 3: Raw Ingestion

Status: done.

Completed:

- Implemented AccesLibre raw ingestion through the public data.gouv tabular REST API.
- Kept the official AccesLibre API URL documented in the raw payload.
- Added a source note explaining that the direct AccesLibre endpoint currently requires an API key for anonymous calls.
- Implemented Open-Meteo raw daily weather ingestion through the official forecast REST API.
- Stored raw JSON responses without destructive transformation.
- Added configurable limits for AccesLibre page size and page count.
- Added configurable Open-Meteo forecast days and timezone.
- Added tests for request URL construction and Data Lake path convention.

Outputs produced during verification:

```text
datalake/raw/acces_libre/establishments/20260505/accessibility.json
datalake/raw/open_meteo/daily_weather/20260505/weather.json
```

Verification result:

```text
AccesLibre raw records: 100
Open-Meteo daily forecast days: 3
Airflow DAG test: success
Airflow import errors: none
```

### Stage 4: Spark Formatting

Status: planned.

Tasks:

- Create Spark formatting jobs in `spark/jobs`.
- Normalize AccesLibre records into a clean establishments table.
- Normalize Open-Meteo records into a clean daily weather table.
- Clean field names and date/time values.
- Write parquet files to the formatted layer.

Expected outputs:

```text
datalake/formatted/acces_libre/establishments/YYYYMMDD/establishments.snappy.parquet
datalake/formatted/open_meteo/daily_weather/YYYYMMDD/weather.snappy.parquet
```

### Stage 5: Spark Combination and Mobility Score

Status: planned.

Tasks:

- Join formatted accessibility and weather datasets.
- Define an accessibility score.
- Define a weather risk score.
- Compute a daily mobility score.
- Produce safer accessible places.
- Produce risky areas during bad weather.
- Produce accessibility improvement priorities.

Expected outputs:

```text
datalake/usage/inclusive_mobility/mobility_scores/YYYYMMDD/scores.snappy.parquet
datalake/usage/inclusive_mobility/risky_areas/YYYYMMDD/risky_areas.snappy.parquet
datalake/usage/inclusive_mobility/improvement_priorities/YYYYMMDD/priorities.snappy.parquet
```

Initial formula:

```text
mobility_score = accessibility_score * 0.7 + (100 - weather_risk_score) * 0.3
```

### Stage 6: Elasticsearch Indexing

Status: planned.

Tasks:

- Add Elasticsearch to Docker Compose.
- Create an indexing job triggered by Airflow.
- Index final usage outputs.
- Keep index names stable and documented.

Expected indices:

```text
inclusive_mobility_scores
inclusive_mobility_risky_areas
inclusive_mobility_improvement_priorities
```

### Stage 7: Kibana Dashboard

Status: planned.

Tasks:

- Add Kibana to Docker Compose.
- Create dashboard visualizations.
- Show mobility score distribution.
- Show risky places during bad weather.
- Show priority areas for local improvement.

### Stage 8: Kafka Realtime Bonus

Status: optional.

Tasks:

- Create Kafka topics.
- Add producer scripts under `kafka/producers`.
- Add consumer scripts under `kafka/consumers`.
- Optionally add Spark Structured Streaming.

Potential topics:

```text
accessibility.raw.establishments
weather.raw.daily
mobility.usage.scores
```

### Stage 9: Final Deliverables

Status: planned.

Tasks:

- Write final PDF report, maximum 10 pages.
- Record final video, maximum 10 minutes.
- Prepare code zip.
- Ensure one Airflow DAG can run the full pipeline.
- Update README with final status and troubleshooting.

## Data Lake Convention

All project outputs must follow this path convention:

```text
datalake/{layer}/{group}/{dataEntity}/{YYYYMMDD}/{filename}
```

Rules:

- `layer` is `raw`, `formatted`, or `usage`.
- `group` is the source name for raw/formatted data, or the usage name for final outputs.
- `dataEntity` is a stable table/data object name.
- `YYYYMMDD` is the partition date.
- Files in one `dataEntity` folder must share the same schema.
- Generated data files are local runtime outputs and should not be committed.

Examples:

```text
datalake/raw/acces_libre/establishments/20260505/accessibility.json
datalake/raw/open_meteo/daily_weather/20260505/weather.json
datalake/formatted/acces_libre/establishments/20260505/establishments.snappy.parquet
datalake/formatted/open_meteo/daily_weather/20260505/weather.snappy.parquet
datalake/usage/inclusive_mobility/mobility_scores/20260505/scores.snappy.parquet
```

## Repository Structure

```text
airflow/
  dags/
    inclusive_mobility_dag.py
    lib/
      acces_libre_fetcher.py
      config.py
      mobility_score.py
      open_meteo_fetcher.py
      paths.py
      raw_to_formatted_accessibility.py
      raw_to_formatted_weather.py
  datalake/
    raw/
    formatted/
    usage/
  spark/
    README.md
    jobs/
    notebooks/
  kafka/
    README.md
    producers/
    consumers/
  test/
    test_mobility_score.py
    test_paths.py
  docker-compose.yaml
  README.md
```

## Fixed Workflows

These workflows are fixed for the rest of the project. Every new stage must follow them unless the README is updated in the same commit.

| Workflow | When to Use | Fixed Steps | Required Output |
| --- | --- | --- | --- |
| Stage workflow | Every project stage | Read tracker; implement only current stage; update tracker; verify; commit; push. | One stage commit on GitHub. |
| Documentation workflow | Any plan/progress change | Update README; check diff; commit with `Docs:`; push. | README reflects reality. |
| Docker workflow | Service changes or startup | Validate compose; start needed services; inspect health. | Required services healthy. |
| Airflow workflow | DAG or pipeline changes | Check import errors; run DAG test; inspect outputs. | DAG loads and test run succeeds. |
| Spark workflow | Formatting/combination changes | Run Spark job; inspect schema/output. | Parquet output in correct layer. |
| Kafka workflow | Realtime bonus work | Create topic; produce sample; consume sample. | Messages flow through Kafka. |
| Git workflow | End of every stage | `git status`; `git add`; `git commit`; `git push`. | Clean branch synced with `origin/main`. |
| Cleanup workflow | Container clutter appears | Inspect containers first; remove only exited one-off project containers. | Needed services remain healthy. |

### Stage Workflow

```text
1. Read the progress tracker.
2. Implement only the current stage.
3. Update the tracker row: tasks, deliverable, verification, status and commit.
4. Run affected verification commands.
5. Commit and push.
```

### Local Development Workflow

Use the project root:

```bash
cd D:\airflow-pycharm-docker\airflow
```

Before changes:

```bash
git status --short --branch
docker compose ps
```

After changes:

```bash
git diff --stat
git status --short
```

### Docker Workflow

Start the complete stack:

```bash
docker compose up -d
```

Start only Spark/Kafka infrastructure:

```bash
docker compose up -d zookeeper kafka spark-master spark-worker
```

Expected long-running services:

```text
airflow-webserver
airflow-scheduler
airflow-worker
airflow-triggerer
postgres
redis
spark-master
spark-worker
zookeeper
kafka
```

Expected exited service:

```text
airflow-init
```

`airflow-init` exits with code `0` after initialization. This is normal and it should be kept.

### Verification Workflow

Airflow:

```bash
docker compose exec airflow-scheduler airflow dags list-import-errors
docker compose exec airflow-scheduler airflow dags test inclusive_mobility_daily_pipeline 2026-05-05
```

Spark:

```bash
docker compose exec spark-master /opt/spark/bin/spark-submit --version
```

Kafka:

```bash
docker compose exec kafka kafka-topics --bootstrap-server localhost:9092 --list
```

Git:

```bash
git status --short --branch
git log --oneline -5
```

### Git Workflow

Stage commit:

```bash
git status --short
git add .
git commit -m "Stage N: short description"
git push
```

Documentation commit:

```bash
git add README.md
git commit -m "Docs: short description"
git push
```

Remote:

```text
https://github.com/petitlang/inclusive-mobility-airflow.git
```

### Container Cleanup Workflow

Inspect first:

```bash
docker compose ps -a
docker ps -a --format "table {{.Names}}\t{{.Image}}\t{{.Status}}"
```

Safe to remove:

```text
Exited one-off containers with names like airflow-airflow-worker-run-*
```

Keep:

```text
Running project services
airflow-airflow-init-1
Named Docker volumes for Postgres, Kafka and Zookeeper
Non-project containers unless explicitly requested
```

Do not remove Docker volumes unless the goal is to reset stored data.
