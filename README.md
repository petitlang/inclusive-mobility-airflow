# Inclusive Mobility - Big Data Pipeline

English is the default README for this project. A Chinese version is available in [README_zh.md](README_zh.md).

## Overview

Inclusive Mobility is an end-to-end Big Data project that combines public accessibility data and weather forecast data to help users decide whether it is reasonable to go out to a public place on a given day.

The project uses:

- **AccesLibre** for accessibility information about French public establishments.
- **Open-Meteo** for daily weather forecasts by city/location.
- **Apache Airflow** to orchestrate the workflow.
- **Apache Spark** to format, join and score the data.
- **LocalStack S3** to simulate a cloud data lake.
- **Elasticsearch and Kibana** to index and visualize the final usage datasets.
- **Kafka** for a bonus real-time weather streaming module.
- **Docker Compose** to run the complete local environment.

The final dashboard is named **Should I Go Out Today?**. It lets the user filter by city and weather date, inspect recommendation cards, compare risky and safe places, and view real latitude/longitude points on a native Kibana map.

## Architecture

The project follows a data lake architecture with three layers:

```text
raw       -> untouched API responses
formatted -> cleaned Parquet datasets
usage     -> final analytical datasets for Elasticsearch and Kibana
```

The S3 path convention follows the course requirement:

```text
s3://{bucket}/{group}/{entity}/{YYYYMMDD}/...
```

Examples:

```text
s3://raw-data-mobility/acces_libre/establishments/20260608/establishments.json
s3://raw-data-mobility/open_meteo/daily_weather/20260608/daily_weather.json
s3://formatted-data-mobility/acces_libre/establishments/20260608/part-*.snappy.parquet
s3://usage-data-mobility/inclusive_mobility/mobility_scores/20260608/part-*.snappy.parquet
```

## Data Lake Structure

The real pipeline output is stored in **LocalStack S3**. The local `data/` folder is only a local mirror / placeholder and may not contain every generated usage dataset. For example, `city_daily_summary` is written to S3 even if it is not visible under the local `data/usage/` folder.

Current S3 data lake structure:

```text
raw-data-mobility/
+-- acces_libre/
|   +-- establishments/
|       +-- {YYYYMMDD}/
|           +-- establishments.json
+-- open_meteo/
    +-- daily_weather/
        +-- {YYYYMMDD}/
            +-- daily_weather.json

formatted-data-mobility/
+-- acces_libre/
|   +-- establishments/
|       +-- {YYYYMMDD}/
|           +-- part-*.snappy.parquet
+-- open_meteo/
    +-- daily_weather/
        +-- {YYYYMMDD}/
            +-- part-*.snappy.parquet

usage-data-mobility/
+-- inclusive_mobility/
    +-- mobility_scores/
    |   +-- {YYYYMMDD}/
    |       +-- part-*.snappy.parquet
    +-- risky_areas/
    |   +-- {YYYYMMDD}/
    |       +-- part-*.snappy.parquet
    +-- improvement_priorities/
    |   +-- {YYYYMMDD}/
    |       +-- part-*.snappy.parquet
    +-- city_daily_summary/
        +-- {YYYYMMDD}/
            +-- part-*.snappy.parquet
```

Commands to inspect the real S3 data lake:

```powershell
docker compose exec localstack awslocal s3 ls s3://raw-data-mobility --recursive
docker compose exec localstack awslocal s3 ls s3://formatted-data-mobility --recursive
docker compose exec localstack awslocal s3 ls s3://usage-data-mobility --recursive
```

## Project Structure

```text
inclusive-mobility-airflow/
+-- dags/              # Airflow DAG definition
+-- ingestion/         # AccesLibre and Open-Meteo API extraction
+-- transform/         # Spark formatting, scoring and verification jobs
+-- index/             # Elasticsearch import and Kibana dashboard setup
+-- kafka/             # Bonus weather streaming producer/consumer
+-- utils/             # Config, paths, S3 and Docker Spark helpers
+-- data/              # Local mirror of raw/formatted/usage data
+-- test/              # Unit tests
+-- reports/           # Project report drafts and image placeholders
+-- docker-compose.yaml
+-- .env.example
+-- README.md
+-- README_zh.md
```

## Airflow Pipeline

The main DAG is:

```text
inclusive_mobility_daily_pipeline
```

It runs daily and contains the following flow:

```text
init_s3_buckets
  -> extract_accessibility_data
     -> extract_weather_data -> format_weather_data
     -> format_accessibility_data
  -> compute_mobility_scores
  -> index_to_elasticsearch
  -> setup_kibana_dashboards
```

In code, `format_accessibility_data` and `format_weather_data` both have to finish before `compute_mobility_scores` starts. Weather extraction runs after AccesLibre extraction because the Open-Meteo task builds its weather locations from the cities and coordinates found in AccesLibre records. This allows the project to process multiple cities instead of only Paris.

## Data Sources

### AccesLibre

AccesLibre provides public accessibility information for establishments in France. The pipeline extracts fields such as:

- establishment name and activity;
- city, postal code, latitude and longitude;
- wheelchair accessible entrance;
- adapted toilets;
- disabled parking;
- flat entrance;
- entrance width.

### Open-Meteo

Open-Meteo provides free weather forecast data. The project fetches daily weather for the AccesLibre cities, including:

- maximum, minimum and mean temperature;
- apparent temperature;
- precipitation;
- precipitation hours;
- maximum wind speed;
- wind gusts;
- weather code.

The default forecast window is 3 days. The number of weather locations can be configured with `OPEN_METEO_MAX_LOCATIONS`.

## Spark Processing

Spark is used for the formatted and usage layers.

The accessibility formatting job reads raw AccesLibre JSON and writes a clean Parquet dataset with stable column names and types.

The weather formatting job reads the multi-city Open-Meteo JSON payload, explodes the daily arrays, and writes one row per city and weather date.

The scoring job joins accessibility and weather data by city and computes three main metrics:

```text
accessibility_score: 0-100, higher is better
weather_risk_score: 0-100, lower is better
mobility_score: 0-100, higher is better
```

The final formula is:

```text
mobility_score = accessibility_score * 0.7 + (100 - weather_risk_score) * 0.3
```

Accessibility has the strongest weight, but poor weather can still reduce the final score.

## Usage Outputs

The Spark scoring step writes four usage datasets:

```text
usage-data-mobility/
+-- inclusive_mobility/
    +-- mobility_scores/{YYYYMMDD}/
    +-- risky_areas/{YYYYMMDD}/
    +-- improvement_priorities/{YYYYMMDD}/
    +-- city_daily_summary/{YYYYMMDD}/
```

These datasets mean:

- `mobility_scores`: detailed place-level score records.
- `risky_areas`: places where `mobility_score < 40`.
- `improvement_priorities`: places where `accessibility_score < 50`.
- `city_daily_summary`: city/date aggregates used by the dashboard recommendation cards.

## Elasticsearch and Kibana

The indexing step imports usage Parquet outputs into Elasticsearch:

```text
inclusive_mobility_scores
inclusive_mobility_risky_areas
inclusive_mobility_improvement_priorities
inclusive_mobility_city_daily_summary
```

Latitude and longitude are converted into a `location` field with type `geo_point`, so Kibana can display places on a map.

Kibana is configured automatically by `index/setup_kibana.py`. It creates:

- data views for all mobility indices;
- score-class aliases for map layers:
  - `inclusive_mobility_scores_low`: `mobility_score < 40`;
  - `inclusive_mobility_scores_medium`: `40 <= mobility_score < 70`;
  - `inclusive_mobility_scores_high`: `mobility_score >= 70`;
- the dashboard **Should I Go Out Today?**.

The dashboard includes:

- City filter;
- Weather date filter;
- recommendation card;
- average mobility score;
- risky places count;
- safe places count;
- top safe places table;
- places to avoid table;
- mobility score distribution;
- native Kibana map with fit-to-data support.

## Kafka Bonus Module

The `kafka/` folder contains an optional real-time weather streaming module:

- `producers/open_meteo_current_producer.py` publishes current weather events.
- `consumers/weather_stream_to_raw.py` consumes events and persists them as JSONL records.
- Topic: `weather.raw.current`.

This module is separate from the main daily Airflow pipeline.

## Environment

Copy `.env.example` to `.env`:

```powershell
copy .env.example .env
```

Important variables:

```text
AIRFLOW_UID=50000
_PIP_ADDITIONAL_REQUIREMENTS=docker elasticsearch pandas pyarrow boto3
OPEN_METEO_FORECAST_DAYS=3
OPEN_METEO_MAX_LOCATIONS=25
OPEN_METEO_TIMEZONE=Europe/Paris
```

No API key is required for AccesLibre or Open-Meteo.

## Start the Project

From the project root:

```powershell
cd "D:\OD-ISEP\OneDrive - ISEP\ISEP_A2\BigData_A2\project\myProject\inclusive-mobility-airflow"
docker compose up -d
```

Check services:

```powershell
docker compose ps
```

Open the UIs:

```text
Airflow:       http://localhost:8080
Kibana:        http://localhost:5601
Elasticsearch: http://localhost:9200
Spark Master:  http://localhost:8081
Spark Worker:  http://localhost:8082
LocalStack:    http://localhost:4566
Kafka:         localhost:9092
```

Default Airflow login:

```text
username: airflow
password: airflow
```

## Run the Pipeline

In Airflow, trigger:

```text
inclusive_mobility_daily_pipeline
```

Or run a command-line test:

```powershell
docker compose exec airflow-scheduler airflow dags test inclusive_mobility_daily_pipeline 2026-06-08
```

After the DAG finishes, open Kibana and go to:

```text
Dashboard -> Should I Go Out Today?
```

## Useful Verification Commands

List S3 data:

```powershell
docker compose exec localstack awslocal s3 ls s3://raw-data-mobility --recursive
docker compose exec localstack awslocal s3 ls s3://formatted-data-mobility --recursive
docker compose exec localstack awslocal s3 ls s3://usage-data-mobility --recursive
```

Check Elasticsearch indices:

```powershell
curl http://localhost:9200/_cat/indices?v
```

Verify usage outputs:

```powershell
docker compose exec spark-master /opt/spark/bin/spark-submit `
  --packages org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262 `
  /opt/spark/transform/verify_usage_outputs.py
```

Run unit tests:

```powershell
python -m pytest test
```

## Notes

- `weather_risk_score` is a risk metric, so lower is better.
- `mobility_score` and `accessibility_score` are quality metrics, so higher is better.
- The dashboard map uses native Kibana Maps, not the old tile map visualization.
- The map layers are split into low, medium and high score places to keep colors stable and easy to understand.
