-- Initialize source database with orders table

CREATE TABLE IF NOT EXISTS orders (
    order_id SERIAL PRIMARY KEY,
    user_id INTEGER NOT NULL,
    name VARCHAR(255) NOT NULL
);

CREATE ROLE debezium WITH LOGIN PASSWORD 'dbz' REPLICATION;
GRANT CONNECT ON DATABASE sourcedb TO debezium;
GRANT USAGE ON SCHEMA public TO debezium;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO debezium;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO debezium;

-- Insert some sample data
INSERT INTO orders (user_id, name) VALUES
    (1, 'John Doe'),
    (2, 'Jane Smith'),
    (3, 'Bob Johnson'),
    (4, 'Alice Williams'),
    (5, 'Charlie Brown');

-- Create a publication for logical replication (required by Debezium)
ALTER TABLE orders REPLICA IDENTITY FULL;

SELECT pg_create_logical_replication_slot('debezium', 'pgoutput');