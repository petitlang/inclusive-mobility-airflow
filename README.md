# Inclusive Mobility: Accessibility and Weather Risk Analysis

Airflow project for Groupe Lab 3 by Yuefan Liu and Mouzheng Li.

The pipeline combines public establishment accessibility data from AccesLibre with daily weather data from Open-Meteo. The goal is to produce daily mobility scores that identify safer accessible places, highlight risky areas during bad weather, and support local accessibility improvements.

## Current Stage

Stage 1 is complete:

- Removed classroom example DAG files.
- Added the `inclusive_mobility_daily_pipeline` Airflow DAG.
- Created reusable pipeline modules under `dags/lib`.
- Prepared the Data Lake structure with `raw`, `formatted`, and `usage` layers.
- Disabled Airflow example DAGs in Docker Compose.
- Added a Docker-mounted local `datalake` folder.

## Data Lake Convention

Every dataset follows this structure:

```text
/{layer}/{group}/{TableName}/{date}/filename
```

Example:

```text
datalake/raw/acces_libre/establishments/20260505/accessibility.json
datalake/raw/open_meteo/daily_weather/20260505/weather.json
datalake/usage/inclusive_mobility/mobility_scores/20260505/scores.json
```

## Run Airflow

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

## Verify DAG Loading

```bash
docker compose exec airflow-scheduler airflow dags list-import-errors
docker compose exec airflow-scheduler airflow dags test inclusive_mobility_daily_pipeline 2026-05-05
```
