import psycopg2
import time
import random
import os
from datetime import datetime

# Configuration from environment variables
POSTGRES_HOST = os.getenv('POSTGRES_HOST', 'postgres-source')
POSTGRES_PORT = os.getenv('POSTGRES_PORT', '5432')
POSTGRES_USER = os.getenv('POSTGRES_USER', 'sourceuser')
POSTGRES_PASSWORD = os.getenv('POSTGRES_PASSWORD', 'sourcepass')
POSTGRES_DB = os.getenv('POSTGRES_DB', 'sourcedb')
INTERVAL_SECONDS = int(os.getenv('INTERVAL_SECONDS', '10'))

# Sample data for generating synthetic records
FIRST_NAMES = [
    'John', 'Jane', 'Michael', 'Emily', 'David', 'Sarah', 'Robert', 'Emma',
    'William', 'Olivia', 'James', 'Ava', 'Richard', 'Isabella', 'Thomas', 'Sophia',
    'Daniel', 'Mia', 'Matthew', 'Charlotte', 'Christopher', 'Amelia', 'Andrew', 'Harper',
    'Joseph', 'Evelyn', 'Ryan', 'Abigail', 'Nicholas', 'Elizabeth', 'Kevin', 'Sofia',
    'Brian', 'Avery', 'George', 'Ella', 'Edward', 'Scarlett', 'Ronald', 'Grace',
    'Timothy', 'Chloe', 'Jason', 'Victoria', 'Jeffrey', 'Riley', 'Frank', 'Aria'
]

LAST_NAMES = [
    'Smith', 'Johnson', 'Williams', 'Brown', 'Jones', 'Garcia', 'Miller', 'Davis',
    'Rodriguez', 'Martinez', 'Hernandez', 'Lopez', 'Gonzalez', 'Wilson', 'Anderson', 'Thomas',
    'Taylor', 'Moore', 'Jackson', 'Martin', 'Lee', 'Perez', 'Thompson', 'White',
    'Harris', 'Sanchez', 'Clark', 'Ramirez', 'Lewis', 'Robinson', 'Walker', 'Young',
    'Allen', 'King', 'Wright', 'Scott', 'Torres', 'Nguyen', 'Hill', 'Flores',
    'Green', 'Adams', 'Nelson', 'Baker', 'Hall', 'Rivera', 'Campbell', 'Mitchell'
]

def wait_for_database():
    """Wait for PostgreSQL to be ready"""
    print("Waiting for PostgreSQL source database to be ready...")
    max_retries = 30
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
            conn.close()
            print("✓ PostgreSQL is ready!")
            return True
        except Exception as e:
            retry_count += 1
            print(f"Waiting for database (attempt {retry_count}/{max_retries})...")
            time.sleep(5)
    
    raise Exception("Could not connect to PostgreSQL after multiple attempts")

def get_db_connection():
    """Create PostgreSQL connection"""
    return psycopg2.connect(
        host=POSTGRES_HOST,
        port=POSTGRES_PORT,
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD,
        database=POSTGRES_DB
    )

def generate_synthetic_order():
    """Generate a synthetic order with random data"""
    first_name = random.choice(FIRST_NAMES)
    last_name = random.choice(LAST_NAMES)
    full_name = f"{first_name} {last_name}"
    
    # Generate user_id between 1 and 10000
    user_id = random.randint(1, 10000)
    
    return {
        'user_id': user_id,
        'name': full_name
    }

def insert_order(cursor, order):
    """Insert a new order into the database"""
    cursor.execute(
        "INSERT INTO orders (user_id, name) VALUES (%s, %s) RETURNING order_id",
        (order['user_id'], order['name'])
    )
    order_id = cursor.fetchone()[0]
    return order_id

def perform_random_operation(cursor):
    """
    Randomly decide whether to INSERT, UPDATE, or DELETE
    70% INSERT, 20% UPDATE, 10% DELETE
    """
    operation = random.choices(
        ['insert', 'update', 'delete'],
        weights=[70, 20, 10],
        k=1
    )[0]
    
    if operation == 'insert':
        order = generate_synthetic_order()
        order_id = insert_order(cursor, order)
        print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] ✓ INSERTED order_id={order_id}, user_id={order['user_id']}, name='{order['name']}'")
    
    elif operation == 'update':
        # Get a random existing order
        cursor.execute("SELECT order_id FROM orders ORDER BY RANDOM() LIMIT 1")
        result = cursor.fetchone()
        
        if result:
            order_id = result[0]
            order = generate_synthetic_order()
            cursor.execute(
                "UPDATE orders SET user_id = %s, name = %s WHERE order_id = %s",
                (order['user_id'], order['name'], order_id)
            )
            print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] ✓ UPDATED order_id={order_id}, user_id={order['user_id']}, name='{order['name']}'")
        else:
            # If no orders exist, insert one instead
            order = generate_synthetic_order()
            order_id = insert_order(cursor, order)
            print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] ✓ INSERTED order_id={order_id} (no orders to update)")
    
    elif operation == 'delete':
        # Get a random existing order
        cursor.execute("SELECT order_id FROM orders ORDER BY RANDOM() LIMIT 1")
        result = cursor.fetchone()
        
        if result:
            order_id = result[0]
            cursor.execute("DELETE FROM orders WHERE order_id = %s", (order_id,))
            print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] ✓ DELETED order_id={order_id}")
        else:
            # If no orders exist, insert one instead
            order = generate_synthetic_order()
            order_id = insert_order(cursor, order)
            print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] ✓ INSERTED order_id={order_id} (no orders to delete)")

def get_statistics(cursor):
    """Get current database statistics"""
    cursor.execute("SELECT COUNT(*) FROM orders")
    count = cursor.fetchone()[0]
    return count

def main():
    """Main loop to generate synthetic data"""
    print("=" * 60)
    print("Synthetic Data Generator")
    print("=" * 60)
    print(f"Target: {POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}")
    print(f"Interval: {INTERVAL_SECONDS} seconds")
    print(f"Operations: 70% INSERT, 20% UPDATE, 10% DELETE")
    print("=" * 60)
    print()
    
    # Wait for database to be ready
    wait_for_database()
    
    # Get database connection
    conn = get_db_connection()
    conn.autocommit = True
    
    print("Starting data generation...")
    print()
    
    counter = 0
    
    try:
        while True:
            counter += 1
            cursor = conn.cursor()
            
            try:
                # Perform random operation
                perform_random_operation(cursor)
                
                # Every 10 operations, show statistics
                if counter % 10 == 0:
                    total_orders = get_statistics(cursor)
                    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] --- Total orders in database: {total_orders} ---")
                    print()
                
                cursor.close()
                
            except Exception as e:
                print(f"Error in operation: {e}")
                cursor.close()
            
            # Wait for next iteration
            time.sleep(INTERVAL_SECONDS)
    
    except KeyboardInterrupt:
        print("\n" + "=" * 60)
        print("Shutting down data generator...")
        print("=" * 60)
    
    finally:
        if conn:
            conn.close()

if __name__ == "__main__":
    main()