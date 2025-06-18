import os
import io
import duckdb
import psycopg2
import boto3
import pandas as pd
import sqlalchemy
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
from dotenv import load_dotenv

load_dotenv()

# Configuration
DUCKDB_PATH = "/opt/airflow/data/e_commerce.db"
POSTGRES_CONFIG = {
    "host": os.getenv("POSTGRES_HOST", "postgres"),
    "port": int(os.getenv("POSTGRES_PORT", 5432)),
    "user": os.getenv("POSTGRES_USER", "airflow"),
    "password": os.getenv("POSTGRES_PASSWORD", "airflow"),
    "dbname": "e_commerce"
}

SCHEMAS = ["bronze", "silver", "gold"]
TABLES_DDL = {
    'raw_customers': """
        CREATE TABLE IF NOT EXISTS bronze.raw_customers (
            customer_id TEXT PRIMARY KEY,
            customer_unique_id TEXT UNIQUE,
            customer_signup_date TIMESTAMP,
            customer_name TEXT,
            customer_age INTEGER,
            customer_gender TEXT,
            customer_city TEXT,
            customer_region TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )""",
    'raw_orders': """
        CREATE TABLE IF NOT EXISTS bronze.raw_orders (
            order_id TEXT PRIMARY KEY,
            customer_id TEXT,
            order_status TEXT,
            order_purchase_timestamp TIMESTAMP,
            order_approved_at TIMESTAMP,
            order_delivered_carrier_date TIMESTAMP,
            order_delivered_customer_date TIMESTAMP,
            order_estimated_delivery_date TIMESTAMP,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )""",
    'raw_products': """
        CREATE TABLE IF NOT EXISTS bronze.raw_products (
            product_id TEXT PRIMARY KEY,
            product_category_name TEXT,
            product_name TEXT,
            product_description TEXT,
            product_price DECIMAL(10,2),
            product_name_lenght DOUBLE PRECISION,
            product_description_lenght DOUBLE PRECISION,
            product_photos_qty INTEGER,
            product_weight_g DOUBLE PRECISION,
            product_length_cm DOUBLE PRECISION,
            product_height_cm DOUBLE PRECISION,
            product_width_cm DOUBLE PRECISION,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )""",
    'raw_sellers': """
        CREATE TABLE IF NOT EXISTS bronze.raw_sellers (
            seller_id TEXT PRIMARY KEY,
            seller_signup_date TIMESTAMP,
            seller_category_specialization TEXT,
            seller_rating DECIMAL(3,2),
            seller_fulfillment_type TEXT,
            seller_city TEXT,
            seller_state TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )""",
    'raw_order_items': """
        CREATE TABLE IF NOT EXISTS bronze.raw_order_items (
            order_id TEXT,
            order_item_id INTEGER,
            product_id TEXT,
            seller_id TEXT,
            shipping_limit_date TIMESTAMP,
            price DECIMAL(10,2),
            freight_value DECIMAL(10,2),
            discount_pct DECIMAL(5,2),
            coupon_applied BOOLEAN,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (order_id, order_item_id)
        )""",
    'raw_payments': """
        CREATE TABLE IF NOT EXISTS bronze.raw_payments (
            order_id TEXT,
            payment_sequential INTEGER,
            payment_type TEXT,
            payment_installments INTEGER,
            payment_value DECIMAL(10,2),
            payment_status TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (order_id, payment_sequential)
        )""",
    'raw_geolocations': """
        CREATE TABLE IF NOT EXISTS bronze.raw_geolocations (
            geolocation_city TEXT,
            geolocation_region TEXT,
            geolocation_lat DECIMAL(10,6),
            geolocation_lng DECIMAL(10,6),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (geolocation_city, geolocation_region)
        )"""
}

# Foreign key constraints for PostgreSQL (applied after table creation)
POSTGRES_FOREIGN_KEYS = [
    "ALTER TABLE bronze.raw_orders ADD CONSTRAINT IF NOT EXISTS fk_orders_customer FOREIGN KEY (customer_id) REFERENCES bronze.raw_customers(customer_id)",
    "ALTER TABLE bronze.raw_order_items ADD CONSTRAINT IF NOT EXISTS fk_order_items_order FOREIGN KEY (order_id) REFERENCES bronze.raw_orders(order_id)",
    "ALTER TABLE bronze.raw_order_items ADD CONSTRAINT IF NOT EXISTS fk_order_items_product FOREIGN KEY (product_id) REFERENCES bronze.raw_products(product_id)",
    "ALTER TABLE bronze.raw_order_items ADD CONSTRAINT IF NOT EXISTS fk_order_items_seller FOREIGN KEY (seller_id) REFERENCES bronze.raw_sellers(seller_id)",
    "ALTER TABLE bronze.raw_payments ADD CONSTRAINT IF NOT EXISTS fk_payments_order FOREIGN KEY (order_id) REFERENCES bronze.raw_orders(order_id)"
]


def create_tables_with_constraints(cursor, db_type="postgresql"):
    """Create tables and optionally add foreign key constraints."""
    # Create all tables first
    for table_name, ddl in TABLES_DDL.items():
        try:
            cursor.execute(ddl)
            print(f"✅ Created/verified table: bronze.{table_name}")
        except Exception as table_error:
            print(f"❌ Error creating table {table_name}: {table_error}")
            raise

    # Add foreign key constraints only for PostgreSQL
    if db_type == "postgresql":
        for fk_sql in POSTGRES_FOREIGN_KEYS:
            try:
                cursor.execute(fk_sql)
                constraint_name = fk_sql.split("CONSTRAINT")[
                    1].split("FOREIGN")[0].strip()
                print(f"✅ Added foreign key constraint: {constraint_name}")
            except Exception as fk_error:
                # Some constraints might already exist, that's okay
                if "already exists" not in str(fk_error).lower():
                    print(f"⚠️  Foreign key constraint warning: {fk_error}")


def setup_postgres_infrastructure(dbname="e_commerce"):
    """Setup PostgreSQL infrastructure with proper error handling."""
    print("🐘 Setting up PostgreSQL infrastructure...")

    try:
        admin_config = POSTGRES_CONFIG.copy()
        admin_config["dbname"] = "postgres"
        conn_admin = psycopg2.connect(**admin_config)
        conn_admin.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        cur_admin = conn_admin.cursor()

        cur_admin.execute(
            f"SELECT 1 FROM pg_database WHERE datname = '{dbname}';")
        exists = cur_admin.fetchone()

        if not exists:
            cur_admin.execute(f"CREATE DATABASE {dbname};")
            print(f"✅ Created database: {dbname}")
        else:
            print(f"ℹ️ Database already exists: {dbname}")

        cur_admin.close()
        conn_admin.close()

        db_config = POSTGRES_CONFIG.copy()
        db_config["dbname"] = dbname
        conn = psycopg2.connect(**db_config)
        conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        cur = conn.cursor()

        # Create schemas
        for schema in SCHEMAS:
            cur.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")
            print(f"✅ Created/verified schema: {schema}")

        # Create tables with foreign key constraints
        create_tables_with_constraints(cur, "postgresql")

        # Create indexes for better performance
        indexes = [
            "CREATE INDEX IF NOT EXISTS idx_customers_city ON bronze.raw_customers(customer_city)",
            "CREATE INDEX IF NOT EXISTS idx_customers_region ON bronze.raw_customers(customer_region)",
            "CREATE INDEX IF NOT EXISTS idx_orders_customer ON bronze.raw_orders(customer_id)",
            "CREATE INDEX IF NOT EXISTS idx_orders_status ON bronze.raw_orders(order_status)",
            "CREATE INDEX IF NOT EXISTS idx_orders_purchase_date ON bronze.raw_orders(order_purchase_timestamp)",
            "CREATE INDEX IF NOT EXISTS idx_products_category ON bronze.raw_products(product_category_name)",
            "CREATE INDEX IF NOT EXISTS idx_sellers_city ON bronze.raw_sellers(seller_city)",
            "CREATE INDEX IF NOT EXISTS idx_order_items_order ON bronze.raw_order_items(order_id)",
            "CREATE INDEX IF NOT EXISTS idx_order_items_product ON bronze.raw_order_items(product_id)",
            "CREATE INDEX IF NOT EXISTS idx_payments_order ON bronze.raw_payments(order_id)",
            "CREATE INDEX IF NOT EXISTS idx_geolocations_city ON bronze.raw_geolocations(geolocation_city)"
        ]

        for index_sql in indexes:
            try:
                cur.execute(index_sql)
                print(
                    f"✅ Created index: {index_sql.split('idx_')[1].split(' ')[0]}")
            except Exception as idx_error:
                print(f"⚠️  Index creation warning: {idx_error}")

        cur.close()
        conn.close()
        print("🎉 PostgreSQL infrastructure setup completed!")

    except Exception as e:
        print(f"❌ Failed to setup PostgreSQL infrastructure: {e}")
        raise


def setup_duckdb_infrastructure():
    """Setup DuckDB infrastructure locally and upload to S3 (MinIO)."""
    print("🦆 Setting up DuckDB infrastructure and syncing to S3...")

    try:
        s3_bucket = "data-pipeline-storage"
        s3_key = "warehouse/e_commerce.db"
        s3_uri = f"s3://{s3_bucket}/{s3_key}"

        # Step 1: Create DuckDB locally
        conn = duckdb.connect(DUCKDB_PATH)

        for schema in SCHEMAS:
            conn.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")
            print(f"✅ Created schema: {schema}")

        for table_name, ddl in TABLES_DDL.items():
            try:
                conn.execute(ddl)
                print(f"✅ Created table: {table_name}")
            except Exception as e:
                print(f"❌ Failed to create table: {table_name}, {e}")
                raise

        indexes = [
            "CREATE INDEX IF NOT EXISTS idx_customers_city ON bronze.raw_customers(customer_city)",
            "CREATE INDEX IF NOT EXISTS idx_customers_region ON bronze.raw_customers(customer_region)",
            "CREATE INDEX IF NOT EXISTS idx_orders_customer ON bronze.raw_orders(customer_id)",
            "CREATE INDEX IF NOT EXISTS idx_orders_status ON bronze.raw_orders(order_status)",
            "CREATE INDEX IF NOT EXISTS idx_products_category ON bronze.raw_products(product_category_name)",
            "CREATE INDEX IF NOT EXISTS idx_sellers_city ON bronze.raw_sellers(seller_city)",
            "CREATE INDEX IF NOT EXISTS idx_order_items_order ON bronze.raw_order_items(order_id)",
            "CREATE INDEX IF NOT EXISTS idx_order_items_product ON bronze.raw_order_items(product_id)",
            "CREATE INDEX IF NOT EXISTS idx_payments_order ON bronze.raw_payments(order_id)"
        ]

        for index_sql in indexes:
            try:
                conn.execute(index_sql)
                print(
                    f"✅ Created index: {index_sql.split('idx_')[1].split(' ')[0]}")
            except Exception as idx_error:
                print(f"⚠️ Index creation warning: {idx_error}")

        conn.close()
        print(f"✅ Local DuckDB created at: {DUCKDB_PATH}")

        s3 = boto3.client(
            "s3",
            endpoint_url=os.getenv("MINIO_ENDPOINT_URL"),
            aws_access_key_id=os.getenv("MINIO_ACCESS_KEY"),
            aws_secret_access_key=os.getenv("MINIO_SECRET_KEY"),
            region_name=os.getenv("MINIO_REGION")
        )

        s3.upload_file(DUCKDB_PATH, s3_bucket, s3_key)
        print(f"🎯 Uploaded DuckDB to S3: {s3_uri}")

        print("🎉 DuckDB infrastructure setup and upload completed!")

    except Exception as e:
        print(f"❌ Failed to setup DuckDB infrastructure: {e}")
        raise


def verify_setup():
    """Verify both database setups."""
    print("🔍 Verifying database setups...")

    # Verify PostgreSQL
    try:
        conn = psycopg2.connect(**POSTGRES_CONFIG)
        cur = conn.cursor()

        cur.execute("""
            SELECT schemaname, tablename
            FROM pg_tables
            WHERE schemaname IN ('bronze', 'silver', 'gold')
            ORDER BY schemaname, tablename
        """)

        tables = cur.fetchall()
        print(f"✅ PostgreSQL: Found {len(tables)} tables")
        for schema, table in tables:
            print(f"   - {schema}.{table}")

        # Check foreign key constraints
        cur.execute("""
            SELECT tc.constraint_name, tc.table_name, kcu.column_name, ccu.table_name AS foreign_table_name
            FROM information_schema.table_constraints AS tc
            JOIN information_schema.key_column_usage AS kcu ON tc.constraint_name = kcu.constraint_name
            JOIN information_schema.constraint_column_usage AS ccu ON ccu.constraint_name = tc.constraint_name
            WHERE tc.constraint_type = 'FOREIGN KEY' AND tc.table_schema = 'bronze'
        """)

        fks = cur.fetchall()
        print(f"✅ PostgreSQL: Found {len(fks)} foreign key constraints")

        cur.close()
        conn.close()

    except Exception as e:
        print(f"❌ PostgreSQL verification failed: {e}")

    # Verify DuckDB
    try:
        conn = duckdb.connect(DUCKDB_PATH)

        tables = conn.execute("""
            SELECT table_schema, table_name
            FROM information_schema.tables
            WHERE table_schema IN ('bronze', 'silver', 'gold')
            ORDER BY table_schema, table_name
        """).fetchall()

        print(f"✅ DuckDB: Found {len(tables)} tables")
        for schema, table in tables:
            print(f"   - {schema}.{table}")

        conn.close()

    except Exception as e:
        print(f"❌ DuckDB verification failed: {e}")


def ingestion():
    s3_client = boto3.client(
        "s3",
        endpoint_url=os.getenv("MINIO_ENDPOINT_URL"),
        aws_access_key_id=os.getenv("MINIO_ACCESS_KEY"),
        aws_secret_access_key=os.getenv("MINIO_SECRET_KEY"),
        region_name=os.getenv("MINIO_REGION")
    )

    S3_BUCKET = "data-pipeline-storage"
    S3_KEY = "warehouse/e_commerce.db"
    DB_URL = "postgresql://airflow:airflow@postgres:5432/e_commerce"

    data_list = s3_client.list_objects_v2(
        Bucket=S3_BUCKET, Prefix="bronze/data")
    for obj in data_list.get("Contents", []):
        key = obj["Key"]
        response = s3_client.get_object(Bucket=S3_BUCKET, Key=key)
        df = pd.read_csv(io.BytesIO(response["Body"].read()))
        engine = sqlalchemy.create_engine(DB_URL)

        csv_to_sql_table = key.split("/")[-1].replace('.csv', '')
        df.to_sql(f"raw_{csv_to_sql_table}", engine,
                  if_exists="replace", index=False, schema="bronze")
        print(
            f"Loaded from csv to postgresql done. Table: raw_{csv_to_sql_table}")

        duck_con = duckdb.connect(DUCKDB_PATH)
        duck_con.execute(
            f"CREATE OR REPLACE TABLE bronze.raw_{csv_to_sql_table} AS SELECT * FROM df")
        print(f"✅ Loaded to DuckDB: bronze.raw_{csv_to_sql_table}")

    s3_client.upload_file(DUCKDB_PATH, S3_BUCKET, S3_KEY)
