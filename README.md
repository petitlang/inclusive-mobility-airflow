# Inclusive Mobility: Accessibility and Weather Risk Analysis

Airflow project for Groupe Lab 3 by Yuefan Liu and Mouzheng Li.

This project supports mobility and inclusion for people with disabilities by combining public place accessibility data with daily weather conditions. It produces daily mobility scores to identify safer accessible places, highlight risky areas during bad weather, and help local communities prioritize accessibility improvements.

## Project Sources

| Source | Purpose | Link |
| --- | --- | --- |
| AccesLibre API | French public accessibility database for public establishments, including wheelchair access, ramps, steps, accessible toilets and parking. | <https://www.data.gouv.fr/dataservices/api-acces-libre> |
| Open-Meteo API | Free weather API for temperature, precipitation, wind speed and weather conditions. | <https://open-meteo.com/en/docs> |

## Current Status

| Stage | Status | Summary |
| --- | --- | --- |
| Stage 0 | Done | Read project instructions, selected project topic and source APIs. |
| Stage 1 | Done | Removed classroom examples and scaffolded the Airflow project. |
| Stage 2 | Done | Added Spark and Kafka infrastructure to Docker Compose. |
| Stage 3 | Planned | Implement raw extraction from AccesLibre and Open-Meteo. |
| Stage 4 | Planned | Convert raw API data into formatted parquet datasets. |
| Stage 5 | Planned | Compute mobility scores and risk outputs in the usage layer. |
| Stage 6 | Planned | Add optional Kafka streaming path and Spark processing jobs. |
| Stage 7 | Planned | Final validation, documentation, and presentation outputs. |

## Stage Plan

### Stage 0: Topic and Requirements

Goal: define the project scope and map it to the class Big Data pipeline structure.

Tasks:

- Choose the project theme: inclusive mobility, accessibility and weather risk.
- Select AccesLibre as the public accessibility source.
- Select Open-Meteo as the daily weather source.
- Align the project with the class architecture: extraction, raw layer, formatted layer, usage layer.

Output:

- Project theme and API choices are defined.
- Data Lake convention is selected.

Status: done.

### Stage 1: Airflow Skeleton

Goal: replace classroom demo files with a clean project scaffold.

Tasks completed:

- Removed old classroom DAG files.
- Removed old demo helper and test files.
- Removed old Python caches and demo DAG logs.
- Added the `inclusive_mobility_daily_pipeline` DAG.
- Added reusable Python modules under `dags/lib`.
- Added basic tests for path convention and mobility score calculation.
- Added the local Data Lake folder with `raw`, `formatted`, and `usage` layers.
- Disabled Airflow example DAGs in Docker Compose.
- Mounted `datalake` into Airflow containers.
- Added `.env` with `AIRFLOW_UID=50000`.
- Deleted old classroom DAG metadata from the Airflow database.

Output:

- Airflow can load the project DAG without import errors.
- The DAG can run end-to-end with placeholder tasks.
- The local project is committed and pushed to GitHub.

Verification:

```bash
docker compose ps
docker compose exec airflow-scheduler airflow dags list-import-errors
docker compose exec airflow-scheduler airflow dags test inclusive_mobility_daily_pipeline 2026-05-05
```

Status: done.

Git commit:

```text
3b23dc4 Stage 1: scaffold inclusive mobility Airflow project
```

### Stage 2: Big Data Infrastructure

Goal: prepare the project for batch and streaming components while keeping Airflow as the orchestrator.

Tasks completed:

- Add Spark services to Docker Compose.
- Add Kafka services to Docker Compose.
- Add project folders for Spark jobs and Kafka producers/consumers.
- Add environment variables and volume mounts needed by Spark and Kafka.
- Keep Airflow responsible for scheduling, not heavy computation.
- Document how each service is used.
- Add Kafka and Zookeeper named volumes for local state.
- Add Spark and Kafka workspace README files.

Services added:

- `spark-master` using `apache/spark:3.5.1`
- `spark-worker` using `apache/spark:3.5.1`
- `zookeeper` using `confluentinc/cp-zookeeper:7.6.1`
- `kafka` using `confluentinc/cp-kafka:7.6.1`

Folders added:

```text
spark/
  README.md
  jobs/
  notebooks/
kafka/
  README.md
  producers/
  consumers/
```

Local ports:

| Service | Local URL or port |
| --- | --- |
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

Status: done.

### Stage 3: Raw Data Extraction

Goal: fetch source data and save raw untouched API responses into the Data Lake.

Tasks planned:

- Implement AccesLibre API extraction.
- Implement Open-Meteo API extraction.
- Save source responses as JSON in the raw layer.
- Add pagination or query limits where needed.
- Add simple tests around request construction and output paths.

Expected raw outputs:

```text
datalake/raw/acces_libre/establishments/YYYYMMDD/accessibility.json
datalake/raw/open_meteo/daily_weather/YYYYMMDD/weather.json
```

Status: planned.

### Stage 4: Formatted Data Layer

Goal: convert raw JSON into clean analysis-ready datasets.

Tasks planned:

- Normalize AccesLibre establishments into tabular records.
- Normalize Open-Meteo weather values into daily records.
- Convert formatted outputs to parquet.
- Keep one table schema per Data Lake table folder.
- Add data quality checks for required fields.

Expected formatted outputs:

```text
datalake/formatted/acces_libre/establishments/YYYYMMDD/establishments.snappy.parquet
datalake/formatted/open_meteo/daily_weather/YYYYMMDD/weather.snappy.parquet
```

Status: planned.

### Stage 5: Usage Layer and Mobility Score

Goal: create the final project value from accessibility and weather data.

Tasks planned:

- Join accessibility and weather datasets.
- Define an accessibility score.
- Define a weather risk score.
- Compute a daily mobility score from both inputs.
- Produce safer accessible places.
- Produce risky areas during bad weather.
- Produce local improvement priority outputs.

Expected usage outputs:

```text
datalake/usage/inclusive_mobility/mobility_scores/YYYYMMDD/scores.snappy.parquet
datalake/usage/inclusive_mobility/risky_areas/YYYYMMDD/risky_areas.snappy.parquet
datalake/usage/inclusive_mobility/improvement_priorities/YYYYMMDD/priorities.snappy.parquet
```

Initial score formula placeholder:

```text
mobility_score = accessibility_score * 0.7 + (100 - weather_risk_score) * 0.3
```

Status: planned.

### Stage 6: Spark and Kafka Extension

Goal: show a Big Data-ready architecture beyond simple local Python processing.

Spark tasks planned:

- Move heavier transformations into Spark jobs.
- Let Airflow trigger Spark jobs.
- Use Spark to produce formatted and usage layer parquet files.

Kafka tasks planned:

- Reserve a topic for weather events.
- Reserve a topic for accessibility updates.
- Add simple producers for simulated updates.
- Add consumers or Spark Structured Streaming jobs if time allows.

Potential Kafka topics:

```text
accessibility.raw.establishments
weather.raw.daily
mobility.usage.scores
```

Status: planned.

### Stage 7: Final Wrap-Up

Goal: make the project easy to run, explain and present.

Tasks planned:

- Add final setup instructions.
- Add pipeline diagram.
- Add sample output description.
- Add troubleshooting notes.
- Add final screenshots or validation logs if needed.
- Ensure every completed stage has a Git commit and push.

Status: planned.

## Architecture

```text
AccesLibre API       Open-Meteo API
      |                    |
      v                    v
Airflow extraction tasks
      |
      v
datalake/raw
      |
      v
Airflow / Spark formatting tasks
      |
      v
datalake/formatted
      |
      v
Airflow / Spark scoring tasks
      |
      v
datalake/usage
```

Spark and Kafka reserved architecture:

```text
Kafka producers -> Kafka topics -> Spark streaming or batch jobs -> Data Lake
                                      ^
                                      |
                                  Airflow orchestration
```

## Data Lake Convention

Every dataset follows this structure:

```text
/{layer}/{group}/{TableName}/{date}/filename
```

Where:

- `layer`: `raw`, `formatted`, or `usage`.
- `group`: source system name for raw/formatted data, or usage name for final outputs.
- `TableName`: data object with a stable schema.
- `date`: partition date in `YYYYMMDD` format.
- `filename`: source JSON, parquet output, or another explicit file name.

Examples:

```text
datalake/raw/acces_libre/establishments/20260505/accessibility.json
datalake/raw/open_meteo/daily_weather/20260505/weather.json
datalake/formatted/acces_libre/establishments/20260505/establishments.snappy.parquet
datalake/formatted/open_meteo/daily_weather/20260505/weather.snappy.parquet
datalake/usage/inclusive_mobility/mobility_scores/20260505/scores.json
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

## Run Airflow

Start the local stack:

```bash
docker compose up
```

Airflow UI:

```text
http://localhost:8080
```

Default local credentials:

```text
airflow / airflow
```

Spark UI:

```text
http://localhost:8081
```

Kafka broker:

```text
localhost:9092
```

## Verify the Current Stage

Check containers:

```bash
docker compose ps
```

Check DAG import errors:

```bash
docker compose exec airflow-scheduler airflow dags list-import-errors
```

Run the DAG once:

```bash
docker compose exec airflow-scheduler airflow dags test inclusive_mobility_daily_pipeline 2026-05-05
```

Check Spark:

```bash
docker compose exec spark-master /opt/spark/bin/spark-submit --version
```

Check Kafka:

```bash
docker compose exec kafka kafka-topics --bootstrap-server localhost:9092 --list
```

## Git Workflow

Each completed stage should be committed and pushed separately.

Current remote:

```text
https://github.com/petitlang/inclusive-mobility-airflow.git
```

Workflow:

```bash
git status
git add .
git commit -m "Stage N: short description"
git push
```
