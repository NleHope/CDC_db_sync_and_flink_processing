#!/bin/sh

echo "Waiting for Debezium Connect to be ready..."
sleep 30

# Wait for Debezium to be fully started
until curl -f -s http://debezium:8083/connectors > /dev/null 2>&1; do
    echo "Waiting for Debezium Connect..."
    sleep 5
done

echo "Debezium Connect is ready. Creating connector..."

# Create Debezium PostgreSQL connector
curl -i -X POST -H "Accept:application/json" -H "Content-Type:application/json" \
  http://debezium:8083/connectors/ -d '{
  "name": "postgres-source-connector",
  "config": {
    "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
    "database.hostname": "postgres-source",
    "database.port": "5432",
    "database.user": "sourceuser",
    "database.password": "sourcepass",
    "database.dbname": "sourcedb",
    "database.server.name": "dbserver1",
    "table.include.list": "public.orders",
    "plugin.name": "pgoutput",
    "publication.autocreate.mode": "filtered",
    "slot.name": "debezium",
    "topic.prefix": "dbserver1",
    "key.converter": "org.apache.kafka.connect.json.JsonConverter",
    "value.converter": "org.apache.kafka.connect.json.JsonConverter",
    "key.converter.schemas.enable": "false",
    "value.converter.schemas.enable": "false"
  }
}'

echo ""
echo "Debezium connector created successfully!"