import psycopg2
import random
import time
import logging
from datetime import datetime

# --- Configuration ---
# Replace with your PostgreSQL connection details
DB_CONFIG = {
    "dbname": "postgres",
    "host": "localhost",
    "port": "5432"
}

# --- Logging Setup ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# --- Schema Definition ---
SCHEMA_SQL = """
DROP TABLE IF EXISTS order_line, "order", item, store CASCADE;

CREATE TABLE store (
    store_id SERIAL PRIMARY KEY,
    name VARCHAR(255) NOT NULL
);

CREATE TABLE item (
    item_id SERIAL PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    description TEXT,
    price NUMERIC(10, 2) NOT NULL CHECK (price > 0)
);

CREATE TABLE "order" (
    order_id SERIAL PRIMARY KEY,
    store_id INT NOT NULL,
    status VARCHAR(50) NOT NULL DEFAULT 'open', -- e.g., 'open', 'closed', 'cancelled'
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT fk_store
        FOREIGN KEY(store_id)
        REFERENCES store(store_id)
);

CREATE TABLE order_line (
    order_line_id SERIAL PRIMARY KEY,
    order_id INT NOT NULL,
    item_id INT NOT NULL,
    quantity INT NOT NULL DEFAULT 1,
    CONSTRAINT fk_order
        FOREIGN KEY(order_id)
        REFERENCES "order"(order_id) ON DELETE CASCADE,
    CONSTRAINT fk_item
        FOREIGN KEY(item_id)
        REFERENCES item(item_id)
);

-- Create an index on status for faster lookups of open orders
CREATE INDEX idx_order_status ON "order"(status);
CREATE INDEX idx_order_updated_at_status on "order"(updated_at, status);

-- Create a trigger to automatically update the updated_at timestamp
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
   NEW.updated_at = NOW();
   RETURN NEW;
END;
$$ language 'plpgsql';

CREATE TRIGGER update_order_updated_at
BEFORE UPDATE ON "order"
FOR EACH ROW
EXECUTE FUNCTION update_updated_at_column();
"""

def get_db_connection():
    """Establishes and returns a connection to the PostgreSQL database."""
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        return conn
    except psycopg2.OperationalError as e:
        logging.error(f"Could not connect to the database: {e}")
        raise

def create_schema(conn):
    """Creates the database schema by executing the SCHEMA_SQL."""
    try:
        with conn.cursor() as cur:
            logging.info("Dropping old schema and creating a new one...")
            cur.execute(SCHEMA_SQL)
        conn.commit()
        logging.info("Schema created successfully.")
    except psycopg2.Error as e:
        logging.error(f"Error creating schema: {e}")
        conn.rollback()
        raise

def populate_initial_data(conn):
    """Populates the database with initial stores and items."""
    try:
        with conn.cursor() as cur:
            # --- Populate Stores ---
            stores_to_add = [
                ("Downtown Central",),
                ("Uptown Galleria",),
                ("Westside Market",),
                ("Eastville Plaza",)
            ]
            logging.info(f"Populating {len(stores_to_add)} stores...")
            cur.executemany("INSERT INTO store (name) VALUES (%s);", stores_to_add)

            # --- Populate Items ---
            logging.info("Populating 1000 items...")
            items_to_add = []
            for i in range(1000):
                item_name = f"Item #{i+1}"
                description = f"This is the detailed description for item number {i+1}."
                price = round(random.uniform(0.99, 299.99), 2)
                items_to_add.append((item_name, description, price))

            cur.executemany(
                "INSERT INTO item (name, description, price) VALUES (%s, %s, %s);",
                items_to_add
            )

        conn.commit()
        logging.info("Initial data populated successfully.")
    except psycopg2.Error as e:
        logging.error(f"Error populating data: {e}")
        conn.rollback()
        raise

def place_new_order(conn, store_id, all_item_ids):
    """Simulates placing a new order with a random number of items."""
    try:
        with conn.cursor() as cur:
            # Create a new order and get its ID
            cur.execute(
                "INSERT INTO \"order\" (store_id, status) VALUES (%s, 'open') RETURNING order_id;",
                (store_id,)
            )
            order_id = cur.fetchone()[0]

            # Add a random number of items to the order
            num_items_in_order = random.randint(2, 10)
            order_lines = []
            for _ in range(num_items_in_order):
                item_id = random.choice(all_item_ids)
                order_lines.append((order_id, item_id))

            cur.executemany(
                "INSERT INTO order_line (order_id, item_id) VALUES (%s, %s);",
                order_lines
            )
            conn.commit()
            logging.info(f"Placed new order {order_id} at store {store_id} with {num_items_in_order} items.")
    except psycopg2.Error as e:
        logging.error(f"Error placing order: {e}")
        conn.rollback()

def process_open_orders(conn):
    """Finds a batch of 'open' orders and updates their status to 'closed'."""
    try:
        with conn.cursor() as cur:
            # Find up to 10 open orders to process
            cur.execute(
                "SELECT order_id FROM \"order\" WHERE status = 'open' LIMIT 10;"
            )
            open_orders = cur.fetchall()

            if not open_orders:
                logging.info("No open orders to process.")
                return

            order_ids_to_close = [order[0] for order in open_orders]
            logging.info(f"Found {len(order_ids_to_close)} open orders to close: {order_ids_to_close}")

            # Update their status to 'closed'
            cur.execute(
                "UPDATE \"order\" SET status = 'closed' WHERE order_id = ANY(%s);",
                (order_ids_to_close,)
            )
            conn.commit()
            logging.info(f"Successfully closed {cur.rowcount} orders.")
    except psycopg2.Error as e:
        logging.error(f"Error processing open orders: {e}")
        conn.rollback()


def main():
    """Main function to set up the database and run the workload simulation."""
    conn = None
    try:
        conn = get_db_connection()

        # --- Initial Setup ---
        create_schema(conn)
        populate_initial_data(conn)

        # --- Get IDs for workload simulation ---
        with conn.cursor() as cur:
            cur.execute("SELECT store_id FROM store;")
            store_ids = [row[0] for row in cur.fetchall()]
            cur.execute("SELECT item_id FROM item;")
            item_ids = [row[0] for row in cur.fetchall()]

        if not store_ids or not item_ids:
            logging.error("No stores or items available to generate workload. Exiting.")
            return

        # --- Workload Simulation Loop ---
        logging.info("\n--- Starting Workload Simulation ---")
        while True:
            # Place a new order every 1-3 seconds
            time.sleep(random.uniform(0.0, 0.01))
            random_store_id = random.choice(store_ids)
            place_new_order(conn, random_store_id, item_ids)

            # Process open orders every 10 seconds
            if int(time.time()) % 10 == 0:
                process_open_orders(conn)
                time.sleep(1) # Avoid running multiple times in the same second

    except (Exception, KeyboardInterrupt) as e:
        logging.error(f"An error occurred: {e}")
        if isinstance(e, KeyboardInterrupt):
            print("\nSimulation stopped by user.")
    finally:
        if conn:
            conn.close()
            logging.info("Database connection closed.")

if __name__ == "__main__":
    main()
