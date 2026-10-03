# Real-Time Orders Data Pipeline
![CI](https://github.com/laxmi345/realtime-orders-pipeline/actions/workflows/ci.yml/badge.svg)

![Dashboard](docs/dashboard.png)

Kafka -> Spark Structured Streaming -> PostgreSQL -> Streamlit dashboard, fully Dockerized with GitHub Actions CI.

## Architecture
```
Python producer --> Kafka (topic: orders) --> Spark Structured Streaming
   (fake orders)                               (parse JSON, validate, dedupe, watermark)
                                                         |
                                                         v
                                      PostgreSQL (orders table + SQL views)
                                                         |
                                                         v
                                        Streamlit live dashboard (5s refresh)
```

## Run
```bash
docker compose up --build
```
- Dashboard: http://localhost:8501  (first data appears in ~1-2 min; Spark downloads packages on first run)
- PostgreSQL: localhost:5432 (user/pass: analytics / analytics, db: ordersdb)

Reset everything: `docker compose down -v`

## Useful commands
```bash
docker compose logs -f spark
docker compose exec postgres psql -U analytics -d ordersdb -c "SELECT COUNT(*) FROM orders;"
docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic orders --from-beginning --max-messages 3
```

## Engineering highlights
- Data quality: invalid records (negative amount, null id) filtered in Spark (producer sends ~2% bad events on purpose)
- Deduplication with `dropDuplicates` + 10-min watermark, checkpointing for fault tolerance
- SQL views for KPIs, per-minute sales, category/city revenue
- CI: flake8 lint + pytest + docker build on every push

## Next upgrades
- Push images to GHCR and deploy on AWS EC2 with a CD job
- Windowed aggregations in Spark (tumbling 1-min windows)
- Anomaly detection on order amount, Slack alerts
- Grafana dashboards
