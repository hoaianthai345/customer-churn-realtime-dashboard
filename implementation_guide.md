# Big Data Realtime BI Project  
Stack:  
- Apache Kafka  
- Spark Structured Streaming  
- ClickHouse  
- Apache Superset  
  
Goal:  
Build a near real-time BI pipeline using historical batch CSV files replayed as streaming events.  
  
Source datasets:  
- members_v3.csv  
- transactions_v2.csv  
- user_logs_v2.csv  
- train_v2.csv  
  
Core flow:  
CSV Replay Producer -> Kafka -> Spark Structured Streaming -> ClickHouse -> Superset Dashboard  
  
---  
  
# 1. System Scope  
  
This project simulates a production-style realtime analytics pipeline using historical datasets.  
  
Main objectives:  
1. Replay historical CSV data as event streams.  
2. Ingest events into Kafka topics.  
3. Process events with Spark Structured Streaming.  
4. Store serving-layer aggregates in ClickHouse.  
5. Visualize KPI dashboards in Superset.  
  
This is near real-time simulation, not true live streaming.  
  
---  
  
# 2. High-Level Architecture  
  
## 2.1 Pipeline  
  
- members_v3.csv:  
  used as member dimension / reference data  
  
- transactions_v2.csv:  
  replayed into Kafka as transaction events  
  
- user_logs_v2.csv:  
  replayed into Kafka as user activity events  
  
- Spark Structured Streaming:  
  consumes Kafka topics  
  parses event payloads  
  computes KPIs  
  writes serving tables into ClickHouse  
  
- Superset:  
  reads ClickHouse tables  
  renders realtime dashboards  
  
---  
  
# 3. Repository Structure  
  
```text  
bigdata-realtime-bi/  
│  
├── docker-compose.yml  
├── .env  
├── README.md  
├── IMPLEMENTATION_GUIDE.md  
│  
├── infra/  
│   ├── kafka/  
│   │   ├── create_topics.sh  
│   │   └── kafka-init.md  
│   │  
│   ├── clickhouse/  
│   │   ├── init/  
│   │   │   ├── 001_create_database.sql  
│   │   │   ├── 002_dim_members.sql  
│   │   │   ├── 003_fact_transactions_rt.sql  
│   │   │   ├── 004_fact_user_logs_rt.sql  
│   │   │   ├── 005_kpi_revenue.sql  
│   │   │   ├── 006_kpi_activity.sql  
│   │   │   └── 007_kpi_churn_risk.sql  
│   │   └── clickhouse-notes.md  
│   │  
│   ├── spark/  
│   │   ├── Dockerfile  
│   │   ├── jars/  
│   │   └── spark-defaults.conf  
│   │  
│   └── superset/  
│       ├── superset-init.sh  
│       ├── dashboards/  
│       │   └── dashboard_design.md  
│       └── superset-notes.md  
│  
├── data/  
│   ├── raw/  
│   │   ├── members_v3.csv  
│   │   ├── transactions_v2.csv  
│   │   ├── user_logs_v2.csv  
│   │   └── train_v2.csv  
│   │  
│   ├── processed/  
│   │   ├── members_clean.csv  
│   │   ├── transactions_clean.csv  
│   │   ├── user_logs_clean.csv  
│   │   └── train_clean.csv  
│   │  
│   └── sample/  
│       └── small_demo_slice.csv  
│  
├── apps/  
│   ├── producers/  
│   │   ├── common/  
│   │   │   ├── config.py  
│   │   │   ├── utils.py  
│   │   │   └── serializers.py  
│   │   │  
│   │   ├── bootstrap_members.py  
│   │   ├── replay_transactions.py  
│   │   ├── replay_user_logs.py  
│   │   └── run_all_producers.py  
│   │  
│   ├── streaming/  
│   │   ├── common/  
│   │   │   ├── spark_session.py  
│   │   │   ├── schemas.py  
│   │   │   ├── transforms.py  
│   │   │   ├── clickhouse_writer.py  
│   │   │   └── checkpointing.py  
│   │   │  
│   │   ├── jobs/  
│   │   │   ├── transaction_kpi_job.py  
│   │   │   ├── activity_kpi_job.py  
│   │   │   ├── churn_risk_job.py  
│   │   │   └── member_bootstrap_job.py  
│   │   │  
│   │   └── run/  
│   │       ├── run_transaction_job.sh  
│   │       ├── run_activity_job.sh  
│   │       ├── run_churn_risk_job.sh  
│   │       └── run_all_jobs.sh  
│   │  
│   └── batch/  
│       ├── clean_members.py  
│       ├── clean_transactions.py  
│       ├── clean_user_logs.py  
│       ├── clean_train.py  
│       └── build_member_dimension.py  
│  
├── notebooks/  
│   ├── 01_data_understanding.ipynb  
│   ├── 02_feature_ideas.ipynb  
│   └── 03_clickhouse_validation.ipynb  
│  
├── tests/  
│   ├── test_schemas.py  
│   ├── test_transformations.py  
│   ├── test_producer_payloads.py  
│   └── test_clickhouse_writer.py  
│  
├── scripts/  
│   ├── setup_local.sh  
│   ├── run_pipeline.sh  
│   ├── stop_pipeline.sh  
│   ├── reset_pipeline.sh  
│   └── validate_stack.sh  
│  
└── docs/  
    ├── architecture.md  
    ├── data_dictionary.md  
    ├── event_contracts.md  
    ├── kpi_definitions.md  
    └── demo_script.md
```

---

# 4. Role of Each Folder

## infra/

Contains infrastructure setup and initialization scripts.

## data/raw/

Stores original source CSV files.

## data/processed/

Stores cleaned CSV files after preprocessing.

## apps/producers/

Contains replay producers that publish events into Kafka.

## apps/streaming/

Contains Spark Structured Streaming jobs.

## apps/batch/

Contains preprocessing and dimension-building scripts.

## docs/

Contains architecture, event contracts, KPI definitions, and demo materials.

## scripts/

Contains local startup and orchestration scripts.

* * *

# 5. Kafka Design

## 5.1 Topics

Create these Kafka topics:

* member_events
    
* transaction_events
    
* user_log_events
    

Optional:

* churn_label_events
    

## 5.2 Event Key

Use:

* key = msno
    

## 5.3 Event Payload Format

JSON payload example for transaction event:

JSON{  
  "msno": "user_001",  
  "payment_method_id": 41,  
  "payment_plan_days": 30,  
  "plan_list_price": 149,  
  "actual_amount_paid": 149,  
  "is_auto_renew": 1,  
  "transaction_date": "2017-03-23",  
  "membership_expire_date": "2017-04-23",  
  "is_cancel": 0  
}

JSON payload example for user log event:

JSON{  
  "msno": "user_001",  
  "date": "2017-03-31",  
  "num_25": 3,  
  "num_50": 0,  
  "num_75": 1,  
  "num_985": 1,  
  "num_100": 181,  
  "num_unq": 150,  
  "total_secs": 46240.281  
}

JSON payload example for member event:

JSON{  
  "msno": "user_001",  
  "city": 5,  
  "bd": 19,  
  "gender": "male",  
  "registered_via": 9,  
  "registration_init_time": "2011-09-17"  
}

* * *

# 6. Data Cleaning Rules

## members_v3

* convert registration_init_time from YYYYMMDD to date
    
* replace bd = 0 with null
    
* replace missing gender with "unknown"
    

## transactions_v2

* convert transaction_date to date
    
* convert membership_expire_date to date
    
* validate numeric fields
    
* drop impossible rows if needed
    

## user_logs_v2

* convert date to date
    
* validate total_secs >= 0
    
* validate play counters >= 0
    

## train_v2

* validate is_churn in {0, 1}
    

* * *

# 7. ClickHouse Table Design

## 7.1 Dimension Table

### dim_members

Columns:

* msno String
    
* city Nullable(Int32)
    
* bd Nullable(Int32)
    
* gender String
    
* registered_via Nullable(Int32)
    
* registration_init_time Date
    

Engine:  
MergeTree  
Order by:  
(msno)

## 7.2 Realtime Fact Tables

### fact_transactions_rt

Columns:

* msno String
    
* payment_method_id Int32
    
* payment_plan_days Int32
    
* plan_list_price Float64
    
* actual_amount_paid Float64
    
* is_auto_renew UInt8
    
* transaction_date Date
    
* membership_expire_date Date
    
* is_cancel UInt8
    
* processed_at DateTime
    

### fact_user_logs_rt

Columns:

* msno String
    
* log_date Date
    
* num_25 Int32
    
* num_50 Int32
    
* num_75 Int32
    
* num_985 Int32
    
* num_100 Int32
    
* num_unq Int32
    
* total_secs Float64
    
* processed_at DateTime
    

## 7.3 KPI Serving Tables

### kpi_revenue

Columns:

* event_date Date
    
* total_revenue Float64
    
* total_transactions UInt64
    
* cancel_count UInt64
    
* auto_renew_count UInt64
    
* processed_at DateTime
    

### kpi_activity

Columns:

* event_date Date
    
* active_users UInt64
    
* total_listening_secs Float64
    
* avg_unique_songs Float64
    
* processed_at DateTime
    

### kpi_churn_risk

Columns:

* event_date Date
    
* high_risk_users UInt64
    
* avg_risk_score Float64
    
* processed_at DateTime
    

* * *

# 8. Spark Streaming Jobs

## 8.1 member_bootstrap_job.py

Purpose:

* load cleaned members CSV
    
* write into ClickHouse dim_members
    
* this is batch-like bootstrap, not continuous streaming
    

## 8.2 transaction_kpi_job.py

Purpose:

* consume transaction_events from Kafka
    
* parse payloads
    
* write raw rows to fact_transactions_rt
    
* aggregate revenue KPI
    
* write aggregated KPI into kpi_revenue
    

Metrics:

* total revenue
    
* total transaction count
    
* cancel count
    
* auto renew count
    

## 8.3 activity_kpi_job.py

Purpose:

* consume user_log_events from Kafka
    
* parse payloads
    
* write raw rows to fact_user_logs_rt
    
* aggregate activity KPI
    
* write aggregated KPI into kpi_activity
    

Metrics:

* active users
    
* total listening seconds
    
* average unique songs
    

## 8.4 churn_risk_job.py

Purpose:

* derive churn-risk-oriented KPI from transactions + activity + member dimension
    

Suggested demo rule-based score:

risk_score =  
0.4 * is_cancel

* 0.3 * (1 - is_auto_renew)
    
* 0.3 * low_activity_flag
    

Where:

* low_activity_flag = 1 if total_secs below threshold
    
* otherwise 0
    

Output:

* high-risk users
    
* average risk score by date
    

* * *

# 9. Replay Logic

## 9.1 Replay Principle

Replay historical batch data as near real-time stream.

Recommended mapping:

* 1 historical day = 2 seconds in demo runtime
    

## 9.2 Producer Behavior

### bootstrap_members.py

* load processed members file
    
* write to ClickHouse dim_members directly
    
* optionally publish to member_events topic for audit/demo
    

### replay_transactions.py

* sort by transaction_date
    
* for each date:
    
    * publish all rows for that date to Kafka topic transaction_events
        
    * wait 2 seconds
        
* continue until all dates are published
    

### replay_user_logs.py

* sort by date
    
* for each date:
    
    * publish all rows for that date to Kafka topic user_log_events
        
    * wait 2 seconds
        
* continue until all dates are published
    

* * *

# 10. Superset Dashboard Design

Build at least 3 dashboards.

## 10.1 Revenue Dashboard

Charts:

* total revenue over time
    
* total transactions over time
    
* cancel count over time
    
* auto renew count over time
    

## 10.2 Activity Dashboard

Charts:

* active users over time
    
* total listening seconds over time
    
* average unique songs over time
    

## 10.3 Churn Risk Dashboard

Charts:

* high-risk users over time
    
* average risk score over time
    
* risk by city
    
* risk by registration channel
    

* * *

# 11. Recommended Execution Order

## Phase 1: Infrastructure

1. create docker-compose.yml
    
2. start Kafka, ClickHouse, Superset, Spark
    
3. validate container health
    

## Phase 2: Data Preparation

4. clean source CSV files
    
5. write cleaned files into data/processed
    

## Phase 3: Serving Layer

6. create ClickHouse database and tables
    
7. validate inserts manually
    

## Phase 4: Producers

8. build transaction replay producer
    
9. build user log replay producer
    
10. validate messages in Kafka
    

## Phase 5: Streaming Jobs

11. implement Spark schemas
    
12. implement transaction KPI job
    
13. implement activity KPI job
    
14. implement churn risk job
    

## Phase 6: BI Layer

15. connect Superset to ClickHouse
    
16. create datasets
    
17. build dashboards
    
18. configure auto refresh
    

* * *

# 12. Minimum Deliverable

The minimum working demo must include:

1. docker-compose starts core services
    
2. cleaned CSV files exist
    
3. transaction_events and user_log_events are published to Kafka
    
4. Spark consumes and computes KPIs
    
5. ClickHouse stores KPI tables
    
6. Superset displays at least:
    
    * revenue over time
        
    * active users over time
        
    * high-risk users over time
        

* * *

# 13. AI Agent Implementation Prompt

Use the following prompt for the coding agent.

* * *

## PROMPT FOR AI AGENT

You are implementing a production-style near real-time BI pipeline.

Tech stack:

* Apache Kafka
    
* Spark Structured Streaming
    
* ClickHouse
    
* Apache Superset
    
* Python
    
* Docker Compose
    

Project goal:  
Build a local demo pipeline that replays historical CSV data as Kafka events, processes the events using Spark Structured Streaming, stores realtime KPI tables in ClickHouse, and visualizes them in Superset.

You must follow this repository structure exactly:

[PASTE THE REPOSITORY STRUCTURE FROM SECTION 3]

Implementation requirements:

1. Create a working docker-compose.yml that starts:
    
    * zookeeper
        
    * kafka
        
    * spark
        
    * clickhouse
        
    * superset
        
2. Create ClickHouse init SQL files for:
    
    * dim_members
        
    * fact_transactions_rt
        
    * fact_user_logs_rt
        
    * kpi_revenue
        
    * kpi_activity
        
    * kpi_churn_risk
        
3. Implement preprocessing scripts:
    
    * clean_members.py
        
    * clean_transactions.py
        
    * clean_user_logs.py
        
    * clean_train.py
        
4. Implement replay producers:
    
    * replay_transactions.py
        
    * replay_user_logs.py
        
    * run_all_producers.py
        

Replay behavior:

* sort records by event date
    
* send each day as one replay batch
    
* wait 2 seconds between historical days
    

5. Implement Spark Structured Streaming jobs:
    
    * transaction_kpi_job.py
        
    * activity_kpi_job.py
        
    * churn_risk_job.py
        
6. Spark jobs must:
    
    * read Kafka topics
        
    * parse JSON payloads with explicit schemas
        
    * write raw rows to ClickHouse fact tables where appropriate
        
    * compute daily KPI aggregates
        
    * write KPI aggregates to ClickHouse serving tables
        
7. Churn risk logic must be rule-based for demo:
    
    * risk_score = 0.4 * is_cancel + 0.3 * (1 - is_auto_renew) + 0.3 * low_activity_flag
        
    * low_activity_flag = 1 when total_secs is below a chosen threshold
        
    * produce high-risk users KPI by day
        
8. Create helper modules for:
    
    * Spark session creation
        
    * reusable schemas
        
    * ClickHouse writer
        
    * shared transforms
        
9. Create shell scripts:
    
    * setup_local.sh
        
    * run_pipeline.sh
        
    * stop_pipeline.sh
        
    * reset_pipeline.sh
        
    * validate_stack.sh
        
10. Create docs:
    

* architecture.md
    
* data_dictionary.md
    
* event_contracts.md
    
* kpi_definitions.md
    
* demo_script.md
    

Coding rules:

* Use clean Python structure.
    
* Use environment variables for hostnames, ports, topic names, and credentials.
    
* Add comments only when they clarify non-obvious logic.
    
* Do not use pandas inside streaming jobs.
    
* Use explicit schemas in Spark.
    
* Make code runnable locally.
    
* Prefer self-contained files with clear imports.
    
* Add basic error handling and logging.
    

Output rules:

* Generate complete file contents, not snippets.
    
* Keep naming consistent with the repository structure.
    
* Prioritize a working end-to-end demo over advanced optimization.
    
* Build the code incrementally so each phase can be tested independently.
    

Execution plan:  
Step 1: generate docker-compose.yml and infra files  
Step 2: generate ClickHouse SQL init files  
Step 3: generate preprocessing scripts  
Step 4: generate replay producers  
Step 5: generate Spark common modules  
Step 6: generate Spark jobs  
Step 7: generate shell scripts  
Step 8: generate docs

Important:

* Do not redesign the architecture.
    
* Do not replace Kafka, Spark, ClickHouse, or Superset with other tools.
    
* Do not collapse everything into one script.
    
* Maintain production-like separation between ingestion, processing, storage, and BI.
    

Return files in a clear order grouped by folder.

* * *

# 14. Notes for the Agent

The agent should not:

* replace ClickHouse with PostgreSQL
    
* replace Spark with pandas or pure Python aggregation
    
* bypass Kafka
    
* read CSV directly from Superset
    
* write everything into one monolithic notebook
    

The agent should:

* preserve modularity
    
* preserve service boundaries
    
* make the demo reproducible
    
* keep local startup easy
    

* * *

# 15. Final Build Target

A successful implementation means:

* one command starts infra
    
* one command runs preprocessing
    
* one command starts Spark jobs
    
* one command starts replay producers
    
* Superset dashboards visibly update as historical days are replayed
    

That is the expected end state.

