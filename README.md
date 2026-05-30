# Inclusive Mobility — Accessibility & Weather Risk Analysis

## 项目概述

Inclusive Mobility 是一个端到端的大数据处理项目，结合法国公共场所无障碍数据（AccesLibre）和天气数据（Open-Meteo），为残障人士提供每日出行安全评估。

项目构建了基于数据湖架构的完整数据处理流程，通过 Apache Airflow 编排、Apache Spark 分布式计算、Kafka 实时流处理、Elasticsearch + Kibana 分析展示，并采用 LocalStack 模拟 AWS S3 作为数据湖存储，模拟真实的云原生大数据平台。

项目采用分布式架构设计，包含以下主要组件：

- **Airflow**：负责调度整个数据工作流（Docker 部署）
- **Apache Spark**：负责数据清洗、格式化和评分计算（Docker 部署）
- **Kafka + Zookeeper**：负责实时天气事件流处理（Docker 部署）
- **Elasticsearch + Kibana**：用于数据索引与可视化展示（Docker 部署）
- **LocalStack**：本地模拟 AWS S3，作为分层数据湖的存储媒介（Docker 部署）

所有模块通过共享卷和 Docker 内部网络协同工作，构建了完整的数据采集 → 处理 → 分析 → 可视化的链路。

## 项目结构

```plaintext
inclusive-mobility-airflow/
├── dags/                          # Airflow DAG 定义文件
│   ├── inclusive_mobility_dag.py  # 主 DAG：完整数据处理流水线
│   └── lib/                       # Airflow 专属工具（预留）
├── ingestion/                     # 数据采集模块
│   ├── acces_libre_fetcher.py     # AccesLibre 无障碍数据采集
│   └── open_meteo_fetcher.py      # Open-Meteo 天气数据采集
├── transform/                     # Spark 转换与评分模块
│   ├── format_accessibility.py    # 格式化无障碍数据（JSON → Parquet）
│   ├── format_weather.py          # 格式化天气数据（JSON → Parquet）
│   ├── combine_mobility_data.py   # 组合数据 + 计算出行评分
│   ├── mobility_score.py          # Airflow 评分任务入口
│   ├── raw_to_formatted_accessibility.py  # Airflow 格式化任务入口
│   ├── raw_to_formatted_weather.py        # Airflow 格式化任务入口
│   ├── verify_formatted_outputs.py        # 格式化输出验证
│   └── verify_usage_outputs.py            # 评分输出验证
├── index/                         # Elasticsearch 索引与可视化
│   ├── bulk_import_to_es.py       # 从 S3 批量导入数据到 ES
│   └── setup_kibana.py            # 自动创建 Kibana Data Views
├── utils/                         # 工具函数
│   ├── config.py                  # 全局配置（API、路径）
│   ├── paths.py                   # 数据湖路径工具
│   ├── s3_utils.py                # S3（LocalStack）上传与管理
│   └── docker_spark.py            # Docker SDK spark-submit 助手
├── kafka/                         # Kafka 实时流模块
│   ├── common.py                  # Kafka 通用工具
│   ├── producers/                 # 天气数据生产者
│   └── consumers/                 # 天气数据消费者（持久化到 Data Lake）
├── test/                          # 单元测试
│   ├── test_mobility_score.py
│   ├── test_paths.py
│   ├── test_raw_ingestion.py
│   ├── test_spark_formatting_paths.py
│   └── test_streaming_ingestion.py
├── spark/                         # Spark 工作区（notebooks 预留）
├── data/                          # 数据湖本地目录（分层）
│   ├── raw/                       # 原始数据
│   ├── formatted/                 # 格式化数据
│   └── usage/                     # 最终分析数据
├── .env                           # 环境变量配置（不要提交）
├── .env.example                   # 环境变量模板
├── docker-compose.yaml            # Docker 服务编排
├── .gitignore
└── README.md
```

## 主要模块及功能

### 1. dags 目录

包含 Airflow DAG 文件，定义数据处理工作流。主 DAG `inclusive_mobility_daily_pipeline` 包含 8 个任务节点：

```
init_s3_buckets
    ↓
[extract_accessibility_data, extract_weather_data]
    ↓
[format_accessibility_data, format_weather_data]
    ↓
compute_mobility_scores
    ↓
index_to_elasticsearch
    ↓
setup_kibana_dashboards
```

### 2. ingestion 目录

负责从不同 API 采集数据，并将原始数据双写至本地文件和 S3 数据湖。

- `acces_libre_fetcher.py`：从法国政府开放数据平台（data.gouv.fr）的 AccesLibre API 获取公共场所无障碍数据（轮椅通道、无障碍厕所、残疾人停车位等）。
- `open_meteo_fetcher.py`：从 Open-Meteo 免费天气 API 获取每日天气预报数据（温度、降水、风速、天气代码等）。

### 3. transform 目录

使用 Apache Spark 对原始数据进行格式化、转换和评分计算。

- `format_accessibility.py`：将原始 AccesLibre JSON 格式化为 Parquet，规范化字段名和数据类型。
- `format_weather.py`：将原始 Open-Meteo 天气 JSON 格式化为 Parquet，展平日期的数组结构。
- `combine_mobility_data.py`：连接无障碍数据与天气数据，计算三项指标：
  - **accessibility_score**（0-100）：基于轮椅通道、无障碍厕所、残疾人停车位、平坦入口、入口宽度。
  - **weather_risk_score**（0-100）：基于降水量、风速、极端温度和恶劣天气代码。
  - **mobility_score** = `accessibility_score × 0.7 + (100 - weather_risk_score) × 0.3`
  - 输出三张 usage 表：`mobility_scores`、`risky_areas`（mobility_score < 40）、`improvement_priorities`（accessibility_score < 50）。

### 4. index 目录

将处理后的数据索引到 Elasticsearch 中，并配置 Kibana 可视化。

- `bulk_import_to_es.py`：从 S3 读取 usage 层的 Parquet 文件，批量索引到 ES（3 个索引：scores、risky_areas、improvement_priorities）。
- `setup_kibana.py`：通过 Kibana API 自动创建 Data Views，使数据可在 Kibana 中直接查询和可视化。

### 5. utils 目录

包含工具函数和助手模块。

- `config.py`：全局配置常量（API URL、分页参数、天气变量、Kafka 配置等）。
- `paths.py`：数据湖路径工具，遵循 `{layer}/{group}/{entity}/{YYYYMMDD}/{entity}.{ext}` 命名规范。
- `s3_utils.py`：基于 boto3 的 S3（LocalStack）操作工具，包括 bucket 创建和 JSON 上传。
- `docker_spark.py`：通过 Docker SDK 在 Spark 容器内执行 `spark-submit` 命令。

### 6. kafka 目录

Kafka 实时天气事件流处理模块（Bonus 功能）。

- `producers/open_meteo_current_producer.py`：定时轮询 Open-Meteo 实时天气 API，将天气事件发布到 Kafka topic `weather.raw.current`。
- `consumers/weather_stream_to_raw.py`：消费 Kafka 中的天气事件，持久化为 JSONL 文件存入 Data Lake raw 层。

### 7. data 目录

作为数据湖的存储目录，采用三层架构：raw（原始数据）、formatted（格式化数据）、usage（最终分析数据）。实际运行时，数据通过 LocalStack 模拟的 S3 进行存储，本地 `data/` 目录保留一份副本。

## 环境配置

### 环境变量配置（.env 文件）

复制 `.env.example` 为 `.env`，按需修改配置：

```dotenv
AIRFLOW_UID=50000
_PIP_ADDITIONAL_REQUIREMENTS=docker elasticsearch pandas pyarrow boto3
```

本项目使用的 API 均为公开免费接口，无需额外申请 API Key。

### 数据源

| 数据源 | 用途 | 刷新频率 | 链接 |
|--------|------|---------|------|
| AccesLibre API | 法国公共场所无障碍数据库 | 公开参考数据 | https://www.data.gouv.fr/dataservices/api-acces-libre |
| Open-Meteo API | 免费天气预报 API | 每日/每小时 | https://open-meteo.com/en/docs |

## 启动步骤

### 清理旧容器（首次设置建议执行）

```bash
docker compose down --volumes --remove-orphans
```

### 启动全部服务

```bash
docker compose up -d
```

### 创建 S3 存储桶（仅首次，或由 DAG 自动创建）

```bash
docker compose exec localstack awslocal s3 mb s3://raw-data-mobility
docker compose exec localstack awslocal s3 mb s3://formatted-data-mobility
docker compose exec localstack awslocal s3 mb s3://usage-data-mobility
```

### 本地服务端口

| 服务 | 地址 |
|------|------|
| Airflow UI | http://localhost:8080 |
| Spark Master UI | http://localhost:8081 |
| Spark Worker UI | http://localhost:8082 |
| Elasticsearch | http://localhost:9200 |
| Kibana | http://localhost:5601 |
| Kafka Broker | localhost:9092 |
| LocalStack (S3) | http://localhost:4566 |

### 运行 DAG 测试

```bash
docker compose exec airflow-scheduler airflow dags test inclusive_mobility_daily_pipeline 2026-05-30
```

### 查看 S3 数据

```bash
docker compose exec localstack awslocal s3 ls s3://raw-data-mobility --recursive
docker compose exec localstack awslocal s3 ls s3://formatted-data-mobility --recursive
docker compose exec localstack awslocal s3 ls s3://usage-data-mobility --recursive
```

### 查看 ES 索引

```bash
curl http://localhost:9200/_cat/indices?v
```

### 验证 Spark 作业

```bash
docker compose exec spark-master /opt/spark/bin/spark-submit \
  /opt/spark/transform/format_accessibility.py \
  --raw-path s3a://raw-data-mobility/acces_libre/establishments/20260530/establishments.json \
  --output-path s3a://formatted-data-mobility/acces_libre/establishments/20260530
```

## 数据湖命名规范

所有项目输出遵循统一的路径规范：

```text
s3://{bucket}/{group}/{entity}/{YYYYMMDD}/{entity}.{ext}
```

- `bucket`：`raw-data-mobility`、`formatted-data-mobility` 或 `usage-data-mobility`
- `group`：数据来源名称（`acces_libre`、`open_meteo`、`inclusive_mobility`）
- `entity`：数据实体名称（与文件名一致）
- `YYYYMMDD`：分区日期
- `{entity}.{ext}`：文件名与 entity 同名，扩展名为 `json` 或 `parquet`

示例：

```text
s3://raw-data-mobility/acces_libre/establishments/20260530/establishments.json
s3://raw-data-mobility/open_meteo/daily_weather/20260530/daily_weather.json
s3://formatted-data-mobility/acces_libre/establishments/20260530/part-*.snappy.parquet
s3://usage-data-mobility/inclusive_mobility/mobility_scores/20260530/part-*.snappy.parquet
```

## 评分公式

### accessibility_score（无障碍评分，0-100）

| 条件 | 分值 |
|------|------|
| 轮椅通道（entrance_wheelchair_accessible） | 30 |
| 无障碍厕所（accessible_toilets） | 25 |
| 残疾人停车位（external_disabled_parking） | 20 |
| 平坦入口（entrance_flat_access） | 15 |
| 入口宽度（entrance_min_width_cm），120cm+ 得满分 | 10 |

### weather_risk_score（天气风险评分，0-100，越高越危险）

| 条件 | 分值 |
|------|------|
| 降水量 ≥ 10mm | 35 |
| 风速 ≥ 50km/h | 25 |
| 极端温度（<0°C 或 >35°C） | 25 |
| 恶劣天气代码（≥70） | 15 |

### mobility_score（综合出行评分，0-100）

```
mobility_score = accessibility_score × 0.7 + (100 - weather_risk_score) × 0.3
```
