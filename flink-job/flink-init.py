import time
import os
import requests
import json
import sys

# Force unbuffered output to see logs in Docker
os.environ['PYTHONUNBUFFERED'] = '1'
sys.stdout.flush()
sys.stderr.flush()

FLINK_JOBMANAGER_HOST = os.getenv('FLINK_JOBMANAGER_HOST', 'flink-jobmanager')
KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:29092')
POSTGRES_HOST = os.getenv('POSTGRES_HOST', 'postgres-dest')
POSTGRES_PORT = os.getenv('POSTGRES_PORT', '5432')
POSTGRES_USER = os.getenv('POSTGRES_USER', 'destuser')
POSTGRES_PASSWORD = os.getenv('POSTGRES_PASSWORD', 'destpass')
POSTGRES_DB = os.getenv('POSTGRES_DB', 'destdb')

print("=" * 60)
print("STARTING FLINK SQL CLIENT INITIALIZATION")
print("=" * 60)
print(f"FLINK_JOBMANAGER_HOST: {FLINK_JOBMANAGER_HOST}")
print(f"KAFKA_BOOTSTRAP_SERVERS: {KAFKA_BOOTSTRAP_SERVERS}")
print(f"POSTGRES_HOST: {POSTGRES_HOST}")
print("=" * 60)
sys.stdout.flush()

def wait_for_services():
    """Wait for all services to be ready"""
    print("Waiting for services to be ready...")
    sys.stdout.flush()
    time.sleep(30)
    
    # Wait for Flink JobManager
    max_retries = 30
    retry_count = 0
    
    while retry_count < max_retries:
        try:
            response = requests.get(f'http://{FLINK_JOBMANAGER_HOST}:8081/overview')
            if response.status_code == 200:
                print("Flink JobManager is ready")
                sys.stdout.flush()
                break
        except Exception as e:
            print(f"  Error connecting to Flink: {type(e).__name__}: {str(e)}")
            sys.stdout.flush()
        
        retry_count += 1
        print(f"Waiting for Flink JobManager (attempt {retry_count}/{max_retries})...")
        sys.stdout.flush()
        time.sleep(5)
    
    if retry_count >= max_retries:
        print("WARNING: Flink JobManager did not respond after max retries")
        sys.stdout.flush()
    
    time.sleep(10)

def submit_flink_sql_job():
    """Submit Flink SQL job using SQL Gateway (Table API)"""
    
    # Flink SQL statements to create tables and run the job
    sql_statements = f"""
-- Create source table reading from Kafka
CREATE TABLE IF NOT EXISTS kafka_orders (
    `before` ROW<order_id INT, user_id INT, name STRING>,
    `after` ROW<order_id INT, user_id INT, name STRING>,
    `op` STRING,
    `ts_ms` BIGINT
) WITH (
    'connector' = 'kafka',
    'topic' = 'dbserver1.public.orders',
    'properties.bootstrap.servers' = '{KAFKA_BOOTSTRAP_SERVERS}',
    'properties.group.id' = 'flink-consumer-group',
    'scan.startup.mode' = 'earliest-offset',
    'format' = 'json',
    'json.fail-on-missing-field' = 'false',
    'json.ignore-parse-errors' = 'true'
);

-- Create sink table for orders_flink (without name column)
CREATE TABLE IF NOT EXISTS orders_flink_sink (
    order_id INT,
    user_id INT,
    PRIMARY KEY (order_id) NOT ENFORCED
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:postgresql://{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}',
    'table-name' = 'orders_flink',
    'username' = '{POSTGRES_USER}',
    'password' = '{POSTGRES_PASSWORD}',
    'driver' = 'org.postgresql.Driver',
    'connection.max-retry-timeout' = '300000',
    'sink.max-retries' = '3',
    'sink.buffer-flush.max-rows' = '100',
    'sink.buffer-flush.interval' = '1000'
);

-- Insert data from Kafka to orders_flink, dropping the name column
INSERT INTO orders_flink_sink
SELECT 
    COALESCE(`after`.order_id, `before`.order_id) as order_id,
    COALESCE(`after`.user_id, `before`.user_id) as user_id
FROM kafka_orders
WHERE `op` IN ('c', 'r', 'u');
"""
    
    # Write SQL to file for execution
    sql_file = '/tmp/flink_job.sql'
    with open(sql_file, 'w') as f:
        f.write(sql_statements)
    
    print("\nFlink SQL job file created at:", sql_file)
    print("=" * 60)
    print("SQL STATEMENTS:")
    print("=" * 60)
    print(sql_statements)
    print("=" * 60)
    sys.stdout.flush()
    
    # Execute using Flink SQL client
    import subprocess
    
    try:
        # Note: In production, you would use the Flink SQL Gateway or submit via REST API
        # For this setup, we'll use the sql-client in embedded mode with remote execution
        print(f"\nAttempting to run: /opt/flink/bin/sql-client.sh embedded -f {sql_file}")
        print("Using execution.target=remote to connect to cluster...")
        sys.stdout.flush()
        
        result = subprocess.run([
            '/opt/flink/bin/sql-client.sh',
            'embedded',
            '-D', 'execution.target=remote',
            '-D', f'rest.address=flink-jobmanager',
            '-D', f'rest.port=8081',
            '-f',
            sql_file
        ], capture_output=True, text=True, timeout=300)
        
        print("\n" + "=" * 60)
        print("FLINK SQL CLIENT OUTPUT:")
        print("=" * 60)
        print("STDOUT:")
        print(result.stdout)
        print("\nSTDERR:")
        print(result.stderr)
        print(f"\nReturn code: {result.returncode}")
        print("=" * 60)
        sys.stdout.flush()
        
        # Check for errors in output
        if "ERROR" in result.stdout or "ERROR" in result.stderr:
            print("\n⚠️  ERRORS DETECTED IN EXECUTION")
            print("\nDiagnostic Information:")
            print(f"  - PostgreSQL Host: {POSTGRES_HOST}")
            print(f"  - PostgreSQL Port: {POSTGRES_PORT}")
            print(f"  - PostgreSQL Database: {POSTGRES_DB}")
            print(f"  - Kafka Bootstrap Servers: {KAFKA_BOOTSTRAP_SERVERS}")
            print("\nTroubleshooting Steps:")
            print("  1. Verify JDBC driver: /opt/flink/lib/postgresql-*.jar exists")
            print("  2. Check PostgreSQL connectivity: python3 socket test")
            print("  3. Try manual SQL execution in SQL Client")
            sys.stdout.flush()
        
    except subprocess.TimeoutExpired:
        print("Flink SQL job is running (timeout is expected for streaming jobs)")
        sys.stdout.flush()
    except Exception as e:
        print(f"Error submitting Flink job: {e}")
        import traceback
        traceback.print_exc()
        sys.stdout.flush()

def create_flink_job_via_api():
    """Alternative: Create and submit Flink job programmatically"""
    print("\n" + "=" * 60)
    print("FLINK JOB SETUP COMPLETE")
    print("=" * 60)
    print("\nTo manually submit the Flink job, you can:")
    print("1. Access Flink Web UI at http://localhost:8081")
    print("2. Use Flink SQL Client:")
    print("   docker exec -it flink-jobmanager /opt/flink/bin/sql-client.sh")
    print("\n3. Or use the following SQL statements:")
    print("-" * 60)
    
    sql = f"""
CREATE TABLE kafka_orders (
    `before` ROW<order_id INT, user_id INT, name STRING>,
    `after` ROW<order_id INT, user_id INT, name STRING>,
    `op` STRING,
    `ts_ms` BIGINT
) WITH (
    'connector' = 'kafka',
    'topic' = 'dbserver1.public.orders',
    'properties.bootstrap.servers' = '{KAFKA_BOOTSTRAP_SERVERS}',
    'properties.group.id' = 'flink-consumer-group',
    'scan.startup.mode' = 'earliest-offset',
    'format' = 'json',
    'json.fail-on-missing-field' = 'false',
    'json.ignore-parse-errors' = 'true'
);

CREATE TABLE orders_flink_sink (
    order_id INT,
    user_id INT,
    PRIMARY KEY (order_id) NOT ENFORCED
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:postgresql://{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}',
    'table-name' = 'orders_flink',
    'username' = '{POSTGRES_USER}',
    'password' = '{POSTGRES_PASSWORD}',
    'driver' = 'org.postgresql.Driver',
    'connection.max-retry-timeout' = '300000',
    'sink.max-retries' = '3',
    'sink.buffer-flush.max-rows' = '100',
    'sink.buffer-flush.interval' = '1000'
);

INSERT INTO orders_flink_sink
SELECT 
    COALESCE(`after`.order_id, `before`.order_id) as order_id,
    COALESCE(`after`.user_id, `before`.user_id) as user_id
FROM kafka_orders
WHERE `op` IN ('c', 'r', 'u');
"""
    
    print(sql)
    print("-" * 60)
    
    # Save to file for user reference
    with open('/tmp/flink_setup_instructions.sql', 'w') as f:
        f.write(sql)
    
    print("\nSQL statements saved to: /tmp/flink_setup_instructions.sql")
    print("=" * 60)
    sys.stdout.flush()

def main():
    print("Starting Flink SQL Client initialization...")
    sys.stdout.flush()
    
    wait_for_services()
    
    # Try to submit the job, but provide manual instructions as well
    try:
        print("\n" + "=" * 60)
        print("SUBMITTING FLINK SQL JOB")
        print("=" * 60)
        sys.stdout.flush()
        submit_flink_sql_job()
    except Exception as e:
        print(f"Automatic job submission failed: {e}")
        import traceback
        traceback.print_exc()
        sys.stdout.flush()
    
    # Always provide manual instructions
    create_flink_job_via_api()
    
    # Keep container running so user can access it
    print("\nContainer will keep running for manual job submission...")
    print("Press Ctrl+C to exit")
    sys.stdout.flush()
    
    try:
        while True:
            time.sleep(60)
    except KeyboardInterrupt:
        print("\nShutting down...")
        sys.stdout.flush()

if __name__ == "__main__":
    main()