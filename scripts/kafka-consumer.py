import json
import os
import time
from kafka import KafkaConsumer
import psycopg2
from psycopg2.extras import execute_values

# Configuration from environment variables
KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:29092')
KAFKA_TOPIC = os.getenv('KAFKA_TOPIC', 'dbserver1.public.orders')
POSTGRES_HOST = os.getenv('POSTGRES_HOST', 'postgres-dest')
POSTGRES_PORT = os.getenv('POSTGRES_PORT', '5432')
POSTGRES_USER = os.getenv('POSTGRES_USER', 'destuser')
POSTGRES_PASSWORD = os.getenv('POSTGRES_PASSWORD', 'destpass')
POSTGRES_DB = os.getenv('POSTGRES_DB', 'destdb')


def wait_for_services():
    """Wait for Kafka and PostgreSQL to be ready"""
    print("Waiting for services to be ready...")
    time.sleep(20)

def get_db_connection():
    """Create PostgreSQL connection"""
    max_retries = 10
    retry_count = 0
    
    while retry_count < max_retries:
        try:
            conn = psycopg2.connect(
                host=POSTGRES_HOST,
                port=POSTGRES_PORT,
                user=POSTGRES_USER,
                password=POSTGRES_PASSWORD,
                database=POSTGRES_DB
            )
            print("Connected to PostgreSQL destination database")
            return conn
        except Exception as e:
            retry_count += 1
            print(f"Failed to connect to PostgreSQL (attempt {retry_count}/{max_retries}): {e}")
            time.sleep(5)
    
    raise Exception("Could not connect to PostgreSQL after multiple attempts")

def process_message(message, cursor):
    value = message.value
    if not value:
        return

    payload = value   # ← critical fix
    op = payload.get('op')

    if op in ['c', 'r']:
        after = payload.get('after')
        if after:
            cursor.execute("""
                INSERT INTO orders (order_id, user_id, name)
                VALUES (%s, %s, %s)
                ON CONFLICT (order_id) DO UPDATE
                SET user_id = EXCLUDED.user_id,
                    name = EXCLUDED.name
            """, (
                after['order_id'],
                after['user_id'],
                after['name']
            ))

    elif op == 'u':
        after = payload.get('after')
        if after:
            cursor.execute("""
                UPDATE orders
                SET user_id = %s, name = %s
                WHERE order_id = %s
            """, (
                after['user_id'],
                after['name'],
                after['order_id']
            ))

    elif op == 'd':
        before = payload.get('before')
        if before:
            cursor.execute(
                "DELETE FROM orders WHERE order_id = %s",
                (before['order_id'],)
            )

    

def main():
    """Main consumer loop"""
    wait_for_services()
    
    # Connect to PostgreSQL
    conn = get_db_connection()
    conn.autocommit = True
    
    # Create Kafka consumer
    max_retries = 10
    retry_count = 0
    consumer = None
    
    while retry_count < max_retries:
        try:
            consumer = KafkaConsumer(
                KAFKA_TOPIC,
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                auto_offset_reset='earliest',
                enable_auto_commit=True,
                group_id='postgres-consumer-group',
                value_deserializer=lambda x: json.loads(x.decode('utf-8')) if x else None
            )
            print(f"Connected to Kafka and subscribed to topic: {KAFKA_TOPIC}")
            break
        except Exception as e:
            retry_count += 1
            print(f"Failed to connect to Kafka (attempt {retry_count}/{max_retries}): {e}")
            time.sleep(5)
    
    if not consumer:
        raise Exception("Could not connect to Kafka after multiple attempts")
    
    print("Starting to consume messages...")
    
    try:
        for message in consumer:
            try:
                cursor = conn.cursor()
                process_message(message, cursor)
                cursor.close()
            except Exception as e:
                print(f"Error processing message: {e}")
                conn.rollback()
    
    except KeyboardInterrupt:
        print("Shutting down consumer...")
    finally:
        if consumer:
            consumer.close()
        if conn:
            conn.close()

if __name__ == "__main__":
    main()