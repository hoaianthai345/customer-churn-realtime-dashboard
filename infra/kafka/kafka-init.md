# Kafka Init

Create topics after Kafka is healthy:

```bash
docker compose exec kafka bash /opt/project/infra/kafka/create_topics.sh
```

If `/opt/project` is not mounted in Kafka container, run from host:

```bash
docker compose exec kafka kafka-topics --bootstrap-server kafka:9092 --list
```
