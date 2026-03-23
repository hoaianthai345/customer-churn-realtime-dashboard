# AI Agent Guide (Compact)

See details in `ai_agent_guide.md` and `implementation_guide.md`.

Core execution order:

1. Bring up infra (`docker compose up -d`)
2. Create Kafka topics
3. Clean raw data into `data/processed`
4. Start Spark jobs
5. Replay transaction and user log events
6. Validate ClickHouse KPI tables
7. Build Superset dashboards
