# Real-Time Data Streaming with Apache Flink and Debezium

A comprehensive Change Data Capture (CDC) pipeline that demonstrates real-time data replication and stream processing using Apache Flink, Debezium, Apache Kafka, and PostgreSQL.

## 📋 Table of Contents

![Demo](./imgs_demo.gif)

- [Overview](#overview)
- [Architecture](#architecture)
- [Components](#components)
- [Prerequisites](#prerequisites)
- [Installation & Setup](#installation--setup)
- [How It Works](#how-it-works)
- [Usage](#usage)
- [Project Structure](#project-structure)
- [Configuration](#configuration)
- [Monitoring](#monitoring)
- [Troubleshooting](#troubleshooting)

---

## Overview

This project implements a real-time CDC pipeline that:
1. **Captures** database changes from a PostgreSQL source database using Debezium
2. **Streams** changes to Apache Kafka topics
3. **Processes** the data stream with Apache Flink (with schema transformation)
4. **Replicates** data to a PostgreSQL destination database (with Python consumer)

The system demonstrates two parallel data replication strategies:
- **Flink-based**: Processes data from Kafka and writes to `orders_flink` table (drops the `name` column)
- **Python-based**: Consumes from Kafka and writes complete records to `orders` table

---

## Architecture

```
┌─────────────────┐
│  PostgreSQL     │
│  Source DB      │◄──────────┐
│  (sourcedb)     │           │
└────────┬────────┘           │
         │                    │
         │ WAL/Logical        │ Mock Stream
         │ Replication        │ (INSERT/UPDATE/DELETE)
         │                    │
         ▼                    │
┌─────────────────┐    ┌──────────────┐
│  Debezium CDC   │    │ mock_stream  │
│  Connector      │    │   .py        │
└────────┬────────┘    └──────────────┘
         │
         │ CDC Events
         │
         ▼
┌─────────────────────────────────────┐
│         Apache Kafka                │
│  Topic: dbserver1.public.orders     │
└──────────┬──────────────────┬───────┘
           │                  │
           │                  │
    ┌──────▼────────┐  ┌─────▼──────────┐
    │ Apache Flink  │  │  Python Kafka  │
    │ Stream        │  │  Consumer      │
    │ Processing    │  │                │
    └──────┬────────┘  └─────┬──────────┘
           │                  │
           │ Transform        │ Full Record
           │ (drop name)      │ Replication
           │                  │
           ▼                  ▼
    ┌──────────────────────────────────┐
    │      PostgreSQL Dest DB          │
    │                                  │
    │  ┌─────────────┐ ┌──────────────┐│
    │  │orders_flink │ │   orders     ││
    │  │(id,user_id) │ │(id,user,name)││
    │  └─────────────┘ └──────────────┘│
    └──────────────────────────────────┘
```

---

## Components

### 1. **PostgreSQL Source Database** (`postgres-source`)
- **Port**: 5432
- **Purpose**: Primary database where source data originates
- **Features**: 
  - Configured with WAL level set to `logical` for CDC
  - Replica identity set to FULL
  - Logical replication slot created for Debezium

### 2. **PostgreSQL Destination Database** (`postgres-dest`)
- **Port**: 5433 (mapped from 5432)
- **Purpose**: Target database for replicated data
- **Tables**:
  - `orders`: Full replica with all columns (order_id, user_id, name)
  - `orders_flink`: Flink-processed data without the `name` column (order_id, user_id)

### 3. **Apache Kafka & Zookeeper**
- **Kafka Port**: 9092 (external), 29092 (internal)
- **Zookeeper Port**: 2181
- **Purpose**: Message broker for CDC events
- **Topic**: `dbserver1.public.orders` (auto-created by Debezium)

### 4. **Debezium Connect** (`debezium`)
- **Port**: 8083 (REST API)
- **Purpose**: Captures database changes and publishes to Kafka
- **Connector**: PostgreSQL CDC connector
- **Format**: JSON (without schemas for simplicity)

### 5. **Apache Flink Cluster**
- **JobManager Port**: 8081 (Web UI)
- **Components**:
  - `flink-jobmanager`: Job coordinator
  - `flink-taskmanager`: Task executor
  - `flink-sql-client`: SQL job submission
- **Purpose**: Stream processing and transformation
  - Reads from Kafka topic
  - Drops the `name` column
  - Writes to `orders_flink` table via JDBC

### 6. **Python Services**

#### a. `postgres-consumer` (Kafka → PostgreSQL)
- Consumes CDC events from Kafka
- Replicates full records to `orders` table
- Handles INSERT, UPDATE, and DELETE operations

#### b. `mock_stream.py` (Data Generator)
- Generates synthetic order data
- Performs random INSERT/UPDATE/DELETE operations
- Configurable interval (default: 10 seconds)

---

## Prerequisites

- **Docker** (version 20.10 or higher)
- **Docker Compose** (version 2.0 or higher)
- **8GB+ RAM** recommended for running all services
- **Ports available**: 5432, 5433, 8081, 8083, 9092, 2181

---

## Installation & Setup

### Step 1: Clone the Repository

```bash
git clone <repository-url>
cd rewrite
```

### Step 2: Start All Services

Start all containers in detached mode:

```bash
docker compose up -d
```

This command will:
- Build custom Docker images (Flink, consumer, dbsender)
- Start all services defined in `docker-compose.yaml`
- Initialize databases with schema from `init_source.sql` and `init_dest.sql`
- Automatically configure Debezium connector via `debezium-setup.sh`
- Submit Flink SQL job via `flink-init.py`

**Note**: Initial startup takes 1-2 minutes as services wait for dependencies to be ready.

### Step 3: Verify Services are Running

Check that all containers are up:

```bash
docker compose ps
```

Expected output shows all services as "running" or "exited 0" (for setup containers).

### Step 4: Verify Debezium Connector

Check that the Debezium connector is registered:

```bash
curl http://localhost:8083/connectors
```

Expected output:
```json
["postgres-source-connector"]
```

Check connector status:

```bash
curl http://localhost:8083/connectors/postgres-source-connector/status
```

The connector should show `"state": "RUNNING"`.

### Step 5: Start the Data Generator

Run the mock data generator to create streaming data:

```bash
docker compose run --rm -d dbsender
```

This will start continuously generating random INSERT, UPDATE, and DELETE operations on the source database.

---

## How It Works

### 1. **Change Data Capture (CDC)**

When data changes in the source PostgreSQL database:

1. PostgreSQL writes changes to Write-Ahead Log (WAL)
2. Debezium reads the WAL via logical replication
3. Debezium transforms changes to JSON events with this structure:

```json
{
  "before": {
    "order_id": 1,
    "user_id": 100,
    "name": "John Doe"
  },
  "after": {
    "order_id": 1,
    "user_id": 100,
    "name": "John Smith"
  },
  "op": "u",
  "ts_ms": 1704312345678
}
```

Operations:
- `c`: CREATE (INSERT)
- `u`: UPDATE
- `d`: DELETE
- `r`: READ (initial snapshot)

### 2. **Kafka Streaming**

Debezium publishes CDC events to Kafka topic `dbserver1.public.orders`:
- Each event represents one database operation
- Events are ordered and partitioned by primary key
- Kafka provides durable, fault-tolerant message storage

### 3. **Flink Stream Processing**

The Flink job (`flink-init.py`) performs the following:

```sql
-- Source: Read from Kafka
CREATE TABLE kafka_orders (
    `before` ROW<order_id INT, user_id INT, name STRING>,
    `after` ROW<order_id INT, user_id INT, name STRING>,
    `op` STRING,
    `ts_ms` BIGINT
) WITH (
    'connector' = 'kafka',
    'topic' = 'dbserver1.public.orders',
    ...
);

-- Sink: Write to PostgreSQL
CREATE TABLE orders_flink_sink (
    order_id INT,
    user_id INT,
    PRIMARY KEY (order_id) NOT ENFORCED
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:postgresql://postgres-dest:5432/destdb',
    'table-name' = 'orders_flink',
    ...
);

-- Transform and Insert (dropping 'name' column)
INSERT INTO orders_flink_sink
SELECT 
    COALESCE(`after`.order_id, `before`.order_id) as order_id,
    COALESCE(`after`.user_id, `before`.user_id) as user_id
FROM kafka_orders
WHERE `op` IN ('c', 'r', 'u');
```

**Key Operations**:
- Filters out DELETE operations (only processes INSERT, UPDATE, READ)
- Drops the `name` column
- Uses UPSERT semantics (INSERT or UPDATE)

### 4. **Python Consumer**

The Python consumer (`kafka-consumer.py`) provides an alternative replication path:

```python
def process_message(message, cursor):
    op = payload.get('op')
    
    if op in ['c', 'r']:  # INSERT or READ
        after = payload.get('after')
        cursor.execute("""
            INSERT INTO orders (order_id, user_id, name)
            VALUES (%s, %s, %s)
            ON CONFLICT (order_id) DO UPDATE ...
        """)
    
    elif op == 'u':  # UPDATE
        after = payload.get('after')
        cursor.execute("UPDATE orders SET ...")
    
    elif op == 'd':  # DELETE
        before = payload.get('before')
        cursor.execute("DELETE FROM orders WHERE ...")
```

**Features**:
- Handles all operation types (INSERT, UPDATE, DELETE)
- Preserves all columns including `name`
- Direct Kafka → PostgreSQL replication

### 5. **Data Flow Summary**

```
Source DB Change → Debezium → Kafka → [Flink + Python Consumer] → Dest DB
                                            ↓            ↓
                                      orders_flink   orders
                                      (transformed)  (full)
```

---

## Usage

### Monitoring Data Flow

#### 1. Check Source Database

Connect to source database:

```bash
docker exec -it postgres-source psql -U sourceuser -d sourcedb
```

Query orders:

```sql
SELECT * FROM orders ORDER BY order_id;
```

#### 2. Check Kafka Topics

List topics:

```bash
docker exec -it kafka kafka-topics --bootstrap-server localhost:29092 --list
```

Consume messages from CDC topic:

```bash
docker exec -it kafka kafka-console-consumer \
  --bootstrap-server localhost:29092 \
  --topic dbserver1.public.orders \
  --from-beginning
```

#### 3. Check Flink Web UI

Open browser: [http://localhost:8081](http://localhost:8081)

- View running jobs
- Monitor task metrics
- Check job logs and failures

#### 4. Check Destination Database

Connect to destination database:

```bash
docker exec -it postgres-dest psql -U destuser -d destdb
```

Compare tables:

```sql
-- Full records from Python consumer
SELECT * FROM orders ORDER BY order_id;

-- Transformed records from Flink (no 'name' column)
SELECT * FROM orders_flink ORDER BY order_id;

-- Compare counts
SELECT 
    (SELECT COUNT(*) FROM orders) as orders_count,
    (SELECT COUNT(*) FROM orders_flink) as orders_flink_count;
```

### Manual Data Insertion

Insert data manually into source database:

```bash
docker exec -it postgres-source psql -U sourceuser -d sourcedb -c \
  "INSERT INTO orders (user_id, name) VALUES (999, 'Test User');"
```

Watch the data propagate to both destination tables within seconds.

### Viewing Logs

View logs for specific services:

```bash
# Debezium connector logs
docker logs debezium -f

# Flink job logs
docker logs flink-sql-client -f

# Python consumer logs
docker logs postgres-consumer -f

# Data generator logs
docker logs dbsender -f
```

### Stopping and Cleaning Up

Stop all services:

```bash
docker compose down
```

Stop and remove volumes (deletes all data):

```bash
docker compose down -v
```

---

## Project Structure

```
rewrite/
├── docker-compose.yaml           # Orchestrates all services
├── debezium-setup.sh            # Auto-configures Debezium connector
├── init_source.sql              # Source DB schema initialization
├── init_dest.sql                # Destination DB schema initialization
├── requirements.txt             # Python dependencies (currently empty)
│
├── Dockerfile.flink             # Custom Flink image with Python & JDBC
├── Dockerfile.consumer          # Python Kafka consumer image
├── Dockerfile.dbsender          # Data generator image
│
├── flink-job/
│   └── flink-init.py           # Flink SQL job submission script
│
└── scripts/
    ├── kafka-consumer.py        # Python Kafka → PostgreSQL consumer
    └── mock_stream.py           # Synthetic data generator
```

---

## Configuration

### Environment Variables

#### Source Database
- `POSTGRES_USER=sourceuser`
- `POSTGRES_PASSWORD=sourcepass`
- `POSTGRES_DB=sourcedb`
- `POSTGRES_PORT=5432`

#### Destination Database
- `POSTGRES_USER=destuser`
- `POSTGRES_PASSWORD=destpass`
- `POSTGRES_DB=destdb`
- `POSTGRES_PORT=5433` (external mapping)

#### Kafka
- `KAFKA_BOOTSTRAP_SERVERS=kafka:29092` (internal)
- `KAFKA_ADVERTISED_LISTENERS=localhost:9092` (external)

#### Debezium
- `CONNECTOR_NAME=postgres-source-connector`
- `DATABASE_SERVER_NAME=dbserver1`
- `TABLE_INCLUDE_LIST=public.orders`

#### Mock Data Generator
- `INTERVAL_SECONDS=10` (time between operations)

### Customization

To modify data generation frequency, edit in `docker-compose.yaml`:

```yaml
dbsender:
  environment:
    INTERVAL_SECONDS: 5  # Generate data every 5 seconds
```

To change Flink parallelism:

```yaml
flink-taskmanager:
  environment:
    - |
      FLINK_PROPERTIES=
      taskmanager.numberOfTaskSlots: 4  # Increase parallelism
```

---

## Monitoring

### Key Metrics to Monitor

1. **Debezium Connector Health**
   - URL: http://localhost:8083/connectors/postgres-source-connector/status
   - Check: `state: "RUNNING"`

2. **Flink Job Status**
   - URL: http://localhost:8081
   - Navigate to: "Running Jobs"
   - Check for active streaming jobs

3. **Kafka Topic Lag**
   ```bash
   docker exec -it kafka kafka-consumer-groups \
     --bootstrap-server localhost:29092 \
     --describe --group flink-consumer-group
   ```

4. **Database Replication Lag**
   ```sql
   -- Compare record counts
   SELECT 'source' as db, COUNT(*) FROM orders;  -- in source DB
   SELECT 'dest' as db, COUNT(*) FROM orders;    -- in dest DB
   SELECT 'flink' as db, COUNT(*) FROM orders_flink;  -- in dest DB
   ```

---

## Troubleshooting

### Issue: Debezium Connector Not Starting

**Symptoms**: Connector status shows `FAILED` or not found

**Solutions**:
```bash
# Check Debezium logs
docker logs debezium

# Verify source database accepts replication
docker exec postgres-source psql -U sourceuser -d sourcedb \
  -c "SELECT * FROM pg_replication_slots;"

# Recreate connector
docker compose restart debezium-setup
```

### Issue: Flink Job Not Running

**Symptoms**: No job visible in Flink UI (http://localhost:8081)

**Solutions**:
```bash
# Check Flink SQL client logs
docker logs flink-sql-client -f

# Restart Flink job submission
docker compose restart flink-sql-client

# Check JobManager logs
docker logs flink-jobmanager -f
```

### Issue: Data Not Appearing in Destination

**Symptoms**: Source has data, but destination tables are empty

**Solutions**:
```bash
# 1. Verify Kafka has messages
docker exec kafka kafka-console-consumer \
  --bootstrap-server localhost:29092 \
  --topic dbserver1.public.orders --from-beginning --max-messages 5

# 2. Check Python consumer logs
docker logs postgres-consumer -f

# 3. Verify database connectivity
docker exec postgres-consumer ping -c 2 postgres-dest

# 4. Check for errors in destination DB
docker exec postgres-dest psql -U destuser -d destdb \
  -c "SELECT * FROM orders LIMIT 5;"
```

### Issue: High Resource Usage

**Symptoms**: Docker containers using excessive CPU/memory

**Solutions**:
- Reduce Flink parallelism in `docker-compose.yaml`
- Increase `INTERVAL_SECONDS` in data generator
- Limit Kafka retention: add `KAFKA_LOG_RETENTION_HOURS: 1`

### Issue: Port Already in Use

**Symptoms**: Container fails to start due to port conflict

**Solutions**:
```bash
# Check which process is using the port
sudo lsof -i :5432  # or 8081, 9092, etc.

# Change port mapping in docker-compose.yaml
ports:
  - "15432:5432"  # Use alternative external port
```

---

## Advanced Topics

### Scaling Flink

To handle higher throughput, increase TaskManager instances:

```yaml
# In docker-compose.yaml
flink-taskmanager:
  deploy:
    replicas: 3  # Run 3 TaskManager instances
```

### Custom Transformations

Modify `flink-job/flink-init.py` to add custom SQL transformations:

```sql
-- Example: Filter orders above certain user_id
INSERT INTO orders_flink_sink
SELECT 
    COALESCE(`after`.order_id, `before`.order_id) as order_id,
    COALESCE(`after`.user_id, `before`.user_id) as user_id
FROM kafka_orders
WHERE `op` IN ('c', 'r', 'u')
  AND COALESCE(`after`.user_id, `before`.user_id) > 1000;
```

### Enabling Schema Registry

For production, consider using Confluent Schema Registry with Avro format instead of JSON for better schema evolution support.

---

## Performance Considerations

- **Kafka**: Default configuration suitable for development; tune `segment.bytes` and `retention.ms` for production
- **Flink**: Adjust checkpoint intervals, state backend (RocksDB for large state)
- **PostgreSQL**: Connection pooling, index optimization on frequently queried columns
- **Network**: All services use bridge network; consider host network for better performance

---

## License

[Specify your license here]

## Contributing

[Specify contribution guidelines here]

## Support

For issues or questions, please [create an issue](link-to-issues) or contact [maintainer].

---

**Happy Streaming! 🚀**
