# Spark Workspace

This folder is reserved for Spark jobs used by the inclusive mobility pipeline.

Planned usage:

- `jobs/`: batch or streaming Spark jobs triggered by Airflow.
- `notebooks/`: local exploration notebooks if needed.

The Docker Compose stack mounts:

```text
spark/jobs -> /opt/spark/jobs
datalake -> /opt/spark/datalake
```

Spark UI:

```text
http://localhost:8081
```
