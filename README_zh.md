# Inclusive Mobility - 大数据项目流水线

默认 README 是英文版：[README.md](README.md)。本文件是中文说明版，内容与英文版保持一致，方便理解项目和准备答辩。

## 项目概述

Inclusive Mobility 是一个端到端的大数据项目。项目把法国公共场所无障碍数据和天气预报数据结合起来，帮助用户判断某一天是否适合去某个城市的公共场所。

项目使用的主要技术包括：

- **AccesLibre**：法国公共场所无障碍信息。
- **Open-Meteo**：按城市/地点获取每日天气预报。
- **Apache Airflow**：编排整条数据流水线。
- **Apache Spark**：负责格式化、关联和评分计算。
- **LocalStack S3**：在本地模拟云端 S3 数据湖。
- **Elasticsearch + Kibana**：索引和展示最终 usage 数据。
- **Kafka**：作为 bonus 的实时天气流模块。
- **Docker Compose**：一键启动本地完整环境。

最终的 Kibana dashboard 名为 **Should I Go Out Today?**。用户可以按城市和天气日期筛选数据，查看推荐卡片、风险地点、安全地点，并在原生 Kibana 地图上查看 latitude/longitude 点位。

## 项目架构

项目采用三层数据湖结构：

```text
raw       -> 原始 API 响应
formatted -> 清洗后的 Parquet 数据
usage     -> 面向分析和可视化的最终数据
```

S3 路径命名遵循课程要求：

```text
s3://{bucket}/{group}/{entity}/{YYYYMMDD}/...
```

示例：

```text
s3://raw-data-mobility/acces_libre/establishments/20260608/establishments.json
s3://raw-data-mobility/open_meteo/daily_weather/20260608/daily_weather.json
s3://formatted-data-mobility/acces_libre/establishments/20260608/part-*.snappy.parquet
s3://usage-data-mobility/inclusive_mobility/mobility_scores/20260608/part-*.snappy.parquet
```

## Data Lake Structure

真实流水线输出保存在 **LocalStack S3** 中。本地 `data/` 目录只是本地镜像/占位目录，不一定包含每一个已经生成的 usage 数据集。例如，`city_daily_summary` 已经写入 S3，即使它没有出现在本地 `data/usage/` 目录下。

当前 S3 data lake 结构如下：

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

查看真实 S3 data lake 的命令：

```powershell
docker compose exec localstack awslocal s3 ls s3://raw-data-mobility --recursive
docker compose exec localstack awslocal s3 ls s3://formatted-data-mobility --recursive
docker compose exec localstack awslocal s3 ls s3://usage-data-mobility --recursive
```

## 项目结构

```text
inclusive-mobility-airflow/
+-- dags/              # Airflow DAG 定义
+-- ingestion/         # AccesLibre 和 Open-Meteo 数据抽取
+-- transform/         # Spark 格式化、评分和验证任务
+-- index/             # Elasticsearch 导入和 Kibana dashboard 创建
+-- kafka/             # bonus 实时天气流 producer/consumer
+-- utils/             # 配置、路径、S3、Docker Spark 工具
+-- data/              # 本地 raw/formatted/usage 数据副本
+-- test/              # 单元测试
+-- reports/           # 项目报告草稿和图片占位资源
+-- docker-compose.yaml
+-- .env.example
+-- README.md
+-- README_zh.md
```

## Airflow 流水线

主 DAG 名称：

```text
inclusive_mobility_daily_pipeline
```

整体流程：

```text
init_s3_buckets
  -> extract_accessibility_data
     -> extract_weather_data -> format_weather_data
     -> format_accessibility_data
  -> compute_mobility_scores
  -> index_to_elasticsearch
  -> setup_kibana_dashboards
```

在代码里，`format_accessibility_data` 和 `format_weather_data` 都完成之后，`compute_mobility_scores` 才会开始。现在天气抽取任务在 AccesLibre 抽取之后执行，因为 Open-Meteo 会根据 AccesLibre 中的城市和坐标批量抓天气。因此项目不再只支持 Paris，而是可以处理多个城市。

## 数据源

### AccesLibre

AccesLibre 提供法国公共场所的无障碍信息。项目主要使用：

- 场所名称和 activity；
- city、postal code、latitude、longitude；
- 是否有轮椅可进入入口；
- 是否有无障碍厕所；
- 是否有残疾人停车位；
- 是否平坦入口；
- 入口宽度。

### Open-Meteo

Open-Meteo 提供免费天气预报。项目根据 AccesLibre 城市列表抓取每日天气，包括：

- 最高、最低、平均温度；
- 体感温度；
- 降水量；
- 降水小时数；
- 最大风速；
- 最大阵风；
- 天气代码。

默认抓取 3 天天气预报。天气地点数量可以通过 `OPEN_METEO_MAX_LOCATIONS` 控制。

## Spark 处理

Spark 负责 formatted 层和 usage 层。

无障碍格式化任务读取 AccesLibre raw JSON，并输出字段稳定、类型清晰的 Parquet 数据。

天气格式化任务读取多城市 Open-Meteo JSON，把 daily 数组展开，最后形成一行一个 city + weather_date 的天气数据。

评分任务按 city 将无障碍数据和天气数据关联起来，并计算三个核心指标：

```text
accessibility_score: 0-100，越高越好
weather_risk_score: 0-100，越低越好
mobility_score: 0-100，越高越好
```

最终公式：

```text
mobility_score = accessibility_score * 0.7 + (100 - weather_risk_score) * 0.3
```

也就是说，无障碍条件权重更高，但坏天气也会降低最终出行评分。

## Usage 输出

Spark 评分任务会输出 4 个 usage 数据集：

```text
usage-data-mobility/
+-- inclusive_mobility/
    +-- mobility_scores/{YYYYMMDD}/
    +-- risky_areas/{YYYYMMDD}/
    +-- improvement_priorities/{YYYYMMDD}/
    +-- city_daily_summary/{YYYYMMDD}/
```

含义如下：

- `mobility_scores`：每个地点的详细评分记录。
- `risky_areas`：`mobility_score < 40` 的地点。
- `improvement_priorities`：`accessibility_score < 50` 的地点。
- `city_daily_summary`：按 city + weather_date 聚合，用于 dashboard 推荐卡片。

## Elasticsearch 和 Kibana

索引任务会把 usage 层 Parquet 数据导入 Elasticsearch：

```text
inclusive_mobility_scores
inclusive_mobility_risky_areas
inclusive_mobility_improvement_priorities
inclusive_mobility_city_daily_summary
```

项目会把 latitude 和 longitude 转成 `geo_point` 类型的 `location` 字段，供 Kibana 地图使用。

`index/setup_kibana.py` 会自动创建：

- 所有 mobility index 的 data views；
- 地图使用的 score-class aliases：
  - `inclusive_mobility_scores_low`：`mobility_score < 40`；
  - `inclusive_mobility_scores_medium`：`40 <= mobility_score < 70`；
  - `inclusive_mobility_scores_high`：`mobility_score >= 70`；
- dashboard：**Should I Go Out Today?**。

Dashboard 包含：

- City 筛选；
- Weather date 筛选；
- recommendation 卡片；
- average mobility score；
- risky places 数量；
- safe places 数量；
- top safe places 表格；
- places to avoid 表格；
- mobility score 分布图；
- 支持 fit-to-data 的 Kibana 原生地图。

## Kafka Bonus 模块

`kafka/` 目录是可选的实时天气流模块：

- `producers/open_meteo_current_producer.py` 发布实时天气事件。
- `consumers/weather_stream_to_raw.py` 消费事件并写成 JSONL。
- Kafka topic：`weather.raw.current`。

这个模块和主 Airflow 日批流水线是分开的。

## 环境配置

复制 `.env.example` 为 `.env`：

```powershell
copy .env.example .env
```

常用变量：

```text
AIRFLOW_UID=50000
_PIP_ADDITIONAL_REQUIREMENTS=docker elasticsearch pandas pyarrow boto3
OPEN_METEO_FORECAST_DAYS=3
OPEN_METEO_MAX_LOCATIONS=25
OPEN_METEO_TIMEZONE=Europe/Paris
```

AccesLibre 和 Open-Meteo 都不需要 API key。

## 启动项目

在项目根目录运行：

```powershell
cd "D:\OD-ISEP\OneDrive - ISEP\ISEP_A2\BigData_A2\project\myProject\inclusive-mobility-airflow"
docker compose up -d
```

查看服务状态：

```powershell
docker compose ps
```

常用页面：

```text
Airflow:       http://localhost:8080
Kibana:        http://localhost:5601
Elasticsearch: http://localhost:9200
Spark Master:  http://localhost:8081
Spark Worker:  http://localhost:8082
LocalStack:    http://localhost:4566
Kafka:         localhost:9092
```

Airflow 默认登录：

```text
username: airflow
password: airflow
```

## 运行流水线

在 Airflow 页面手动触发：

```text
inclusive_mobility_daily_pipeline
```

或者命令行测试：

```powershell
docker compose exec airflow-scheduler airflow dags test inclusive_mobility_daily_pipeline 2026-06-08
```

DAG 完成后，打开 Kibana：

```text
Dashboard -> Should I Go Out Today?
```

## 常用验证命令

查看 S3 数据：

```powershell
docker compose exec localstack awslocal s3 ls s3://raw-data-mobility --recursive
docker compose exec localstack awslocal s3 ls s3://formatted-data-mobility --recursive
docker compose exec localstack awslocal s3 ls s3://usage-data-mobility --recursive
```

查看 Elasticsearch indices：

```powershell
curl http://localhost:9200/_cat/indices?v
```

验证 usage 输出：

```powershell
docker compose exec spark-master /opt/spark/bin/spark-submit `
  --packages org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262 `
  /opt/spark/transform/verify_usage_outputs.py
```

运行单元测试：

```powershell
python -m pytest test
```

## 注意点

- `weather_risk_score` 是风险分数，所以越低越好。
- `mobility_score` 和 `accessibility_score` 是质量分数，所以越高越好。
- Dashboard 地图使用的是 Kibana 原生 Maps，不是旧版 tile map。
- 地图分为低/中/高三层，颜色更稳定，也更容易解释。
