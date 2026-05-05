# Kafka Workspace

This folder is reserved for Kafka producers and consumers.

Planned usage:

- `producers/`: scripts that publish source events or simulated updates.
- `consumers/`: scripts that read Kafka topics or feed Spark streaming jobs.

Kafka is exposed locally on:

```text
localhost:9092
```

Planned topics:

```text
accessibility.raw.establishments
weather.raw.daily
mobility.usage.scores
```
