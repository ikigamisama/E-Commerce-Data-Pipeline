from DataRandomizer import RandomDatasetGenerator
from faker import Faker
from io import StringIO
from botocore.exceptions import ClientError, NoCredentialsError
import pandas as pd
import numpy as np
import random
import boto3

from datetime import datetime, timedelta


def initialize_generators():
    """Initialize the data generators with proper error handling."""
    try:
        generator = RandomDatasetGenerator(seed=42, locale='en_US')
        faker = Faker()
        faker.seed_instance(42)
        return generator, faker
    except Exception as e:
        print(f"Failed to initialize generators: {e}")
        raise


def create_s3_client():
    """Create S3 client with proper error handling."""
    try:
        s3_client = boto3.client(
            's3',
            endpoint_url='http://minio:9000',
            aws_access_key_id='minioLocalAccessKey',
            aws_secret_access_key='minioLocalSecretKey123',
            region_name='us-east-1'
        )
        return s3_client
    except NoCredentialsError:
        print("AWS credentials not found")
        raise
    except Exception as e:
        print(f"Failed to create S3 client: {e}")
        raise


def ensure_bucket_exists(s3_client, bucket_name):
    """Ensure the S3 bucket exists, create if it doesn't."""
    try:
        s3_client.head_bucket(Bucket=bucket_name)
        print(f"Bucket '{bucket_name}' exists")
    except ClientError as e:
        error_code = e.response['Error']['Code']
        if error_code == '404':
            try:
                s3_client.create_bucket(Bucket=bucket_name)
                print(f"Created bucket '{bucket_name}'")
            except Exception as create_error:
                print(f"Failed to create bucket: {create_error}")
                raise
        else:
            print(f"Error checking bucket: {e}")
            raise


def generate_geolocation_data(generator):
    """Generate geolocation data with improved error handling."""
    print("1. Generating Geolocation data...")

    city_region_mapping = {
        "Manila": {"region": "NCR", "lat_range": (14.55, 14.62), "lng_range": (120.97, 121.02)},
        "Quezon City": {"region": "NCR", "lat_range": (14.63, 14.74), "lng_range": (121.02, 121.11)},
        "Antipolo": {"region": "NCR", "lat_range": (14.55, 14.62), "lng_range": (121.15, 121.25)},
        "Caloocan": {"region": "NCR", "lat_range": (14.65, 14.73), "lng_range": (120.95, 121.01)},
        "Pasig": {"region": "NCR", "lat_range": (14.56, 14.60), "lng_range": (121.06, 121.10)},
        "Taguig": {"region": "NCR", "lat_range": (14.50, 14.55), "lng_range": (121.03, 121.08)},
        "Makati": {"region": "NCR", "lat_range": (14.54, 14.57), "lng_range": (121.01, 121.06)},
        "Parañaque": {"region": "NCR", "lat_range": (14.47, 14.52), "lng_range": (120.98, 121.03)},
        "Las Piñas": {"region": "NCR", "lat_range": (14.43, 14.48), "lng_range": (120.96, 121.03)},
        "Muntinlupa": {"region": "NCR", "lat_range": (14.37, 14.43), "lng_range": (121.02, 121.08)},
        "Valenzuela": {"region": "NCR", "lat_range": (14.70, 14.75), "lng_range": (120.95, 121.00)},
        "Marikina": {"region": "NCR", "lat_range": (14.63, 14.68), "lng_range": (121.09, 121.13)},
        "Mandaluyong": {"region": "NCR", "lat_range": (14.57, 14.60), "lng_range": (121.02, 121.05)},
        "Pasay": {"region": "NCR", "lat_range": (14.53, 14.57), "lng_range": (120.99, 121.03)},
        "Cebu City": {"region": "Region VII", "lat_range": (10.26, 10.36), "lng_range": (123.85, 123.95)},
        "Lapu-Lapu": {"region": "Region VII", "lat_range": (10.27, 10.33), "lng_range": (123.95, 124.02)},
        "Mandaue": {"region": "Region VII", "lat_range": (10.33, 10.36), "lng_range": (123.91, 123.95)},
        "Talisay": {"region": "Region VII", "lat_range": (10.23, 10.27), "lng_range": (123.82, 123.88)},
        "Bacolod": {"region": "Region VI", "lat_range": (10.62, 10.72), "lng_range": (122.90, 123.00)},
        "Iloilo City": {"region": "Region VI", "lat_range": (10.65, 10.72), "lng_range": (122.53, 122.59)},
        "Davao City": {"region": "Region XI", "lat_range": (7.02, 7.25), "lng_range": (125.45, 125.65)},
        "General Santos City": {"region": "Region XI", "lat_range": (6.06, 6.15), "lng_range": (125.10, 125.20)},
        "Zamboanga City": {"region": "Region IX", "lat_range": (6.90, 6.98), "lng_range": (122.00, 122.15)},
        "Cagayan de Oro": {"region": "Region X", "lat_range": (8.45, 8.53), "lng_range": (124.62, 124.70)},
        "Dasmariñas": {"region": "Region IV-A", "lat_range": (14.28, 14.35), "lng_range": (120.91, 120.98)},
        "Bacoor": {"region": "Region IV-A", "lat_range": (14.43, 14.49), "lng_range": (120.95, 121.00)}
    }

    cities = list(city_region_mapping.keys())

    try:
        geoloc_df = generator.generate_dataset(
            n_rows=5000,
            columns_config=[{"name": "geolocation_city",
                             "type": "category", "choices": cities}]
        )

        geoloc_df["geolocation_region"] = geoloc_df["geolocation_city"].map(
            lambda c: city_region_mapping[c]["region"]
        )

        np.random.seed(42)
        geoloc_df["geolocation_lat"] = geoloc_df["geolocation_city"].apply(
            lambda c: np.round(np.random.uniform(
                *city_region_mapping[c]["lat_range"]), 6)
        )
        geoloc_df["geolocation_lng"] = geoloc_df["geolocation_city"].apply(
            lambda c: np.round(np.random.uniform(
                *city_region_mapping[c]["lng_range"]), 6)
        )

        print(f"   ✓ Generated {len(geoloc_df)} geolocation records")
        return geoloc_df, city_region_mapping

    except Exception as e:
        print(f"Failed to generate geolocation data: {e}")
        raise


def generate_customers_data(generator, cities, regions):
    """Generate customer data with proper validation."""
    print("2. Generating Customers data...")

    try:
        customers_config = [
            {"name": "customer_id", "type": "custom", "params": {
                "prefix": "CUST", "delimiter": "-", "start": 10000001}},
            {"name": "customer_unique_id", "type": "uuid4"},
            {"name": "customer_signup_date", "type": "datetime", "params": {
                "start": "2000-01-01T00:00:00",
                "end": "2024-12-31T23:59:59"
            }},
            {"name": "customer_name", "type": "name"},
            {"name": "customer_age", "type": "integer",
                "params": {"min": 18, "max": 65}},
            {"name": "customer_gender", "type": "gender"},
            {"name": "customer_city", "type": "category", "choices": cities},
            {"name": "customer_region", "type": "category", "choices": regions}
        ]

        customers_df = generator.generate_dataset(
            n_rows=20000, columns_config=customers_config)

        city_to_region = dict(zip(cities, regions))
        customers_df["customer_region"] = customers_df["customer_city"].map(
            city_to_region)

        print(f"   ✓ Generated {len(customers_df)} customer records")
        return customers_df

    except Exception as e:
        print(f"Failed to generate customer data: {e}")
        raise


def generate_sellers_data(generator, cities, regions, product_categories):
    """Generate seller data with validation."""
    print("3. Generating Sellers data...")

    fulfillment_types = ['FBS', 'FBP', 'Dropship', 'Pickup', 'COD-FBS',
                         'COD-FBP', 'CrossBorder', 'Consignment', 'HubDrop', 'LocalCourier']

    try:
        sellers_config = [
            {"name": "seller_id", "type": "custom", "params": {
                "prefix": "SELL", "delimiter": "_", "start": 100001}},
            {"name": "seller_signup_date", "type": "datetime", "params": {
                "start": "2002-01-01T00:00:00",
                "end": "2024-12-31T23:59:59"
            }},
            {"name": "seller_category_specialization",
                "type": "category", "choices": product_categories},
            {"name": "seller_rating", "type": "float",
                "params": {"min": 0, "max": 5}},
            {"name": "seller_fulfillment_type",
                "type": "category", "choices": fulfillment_types},
            {"name": "seller_city", "type": "category", "choices": cities},
            {"name": "seller_state", "type": "category", "choices": regions}
        ]

        sellers_df = generator.generate_dataset(
            n_rows=5000, columns_config=sellers_config)
        sellers_df = sellers_df.round(2)

        city_to_region = dict(zip(cities, regions))
        sellers_df["seller_state"] = sellers_df["seller_city"].map(
            city_to_region)

        print(f"   ✓ Generated {len(sellers_df)} seller records")
        return sellers_df

    except Exception as e:
        print(f"Failed to generate seller data: {e}")
        raise


def category_based_product_name(row, faker):
    """Generate product names based on category with better error handling."""
    product_name_templates = {
        "Groceries": [
            "Organic {fruit}", "Pack of {number} Canned {vegetable}", "{brand} Whole Wheat Bread",
            "{brand} Brown Rice 1kg", "Fresh {vegetable}", "{brand} Almond Milk"
        ],
        "Health & Personal Care": [
            "{brand} Toothpaste", "{brand} Vitamin C 1000mg", "Men's Razor Kit", "Herbal Shampoo 500ml",
            "Antibacterial Hand Gel", "Body Lotion with Aloe Vera"
        ],
        "Beauty & Cosmetics": [
            "{brand} Lipstick", "Waterproof Mascara", "BB Cream SPF 30", "Facial Cleanser 150ml",
            "Compact Powder", "Makeup Remover Wipes"
        ],
        "Household Essentials": [
            "Multi-purpose Cleaner", "Dishwashing Liquid", "Laundry Detergent 3L",
            "Paper Towels (6 Rolls)", "Garbage Bags 30L", "{brand} Toilet Paper"
        ],
        "Home Improvement & Tools": [
            "Cordless Drill", "Hammer Set", "LED Light Bulb Pack", "Paint Roller Kit",
            "Screwdriver Set", "Measuring Tape 5m"
        ],
        "Furniture": [
            "Ergonomic Office Chair", "Wooden Coffee Table", "Modern Sofa Set",
            "Bookshelf - 5 Tier", "Dining Set for 4", "Queen Size Mattress"
        ],
        "Electronics & Accessories": [
            "{brand} Bluetooth Headphones", "Wireless Mouse", "4K LED Monitor", "USB-C Hub Adapter",
            "Phone Charging Cable", "{brand} Power Bank 10000mAh"
        ],
        "Appliances": [
            "Air Fryer 3L", "Microwave Oven 25L", "{brand} Washing Machine", "Portable Air Conditioner",
            "Electric Kettle", "Mini Refrigerator"
        ],
        "Clothing & Apparel": [
            "Men's Slim Fit Jeans", "Women's Summer Dress", "Unisex Hoodie", "Cotton T-Shirt Pack",
            "Winter Jacket", "Activewear Leggings"
        ],
        "Shoes & Footwear": [
            "Running Shoes - Men", "Leather Loafers", "Heeled Sandals", "Canvas Sneakers",
            "Kids' Rain Boots", "Flip Flops Pack"
        ]
    }

    try:
        category = row["product_category_name"]
        templates = product_name_templates.get(
            category, ["Generic {brand} Product"])
        template = random.choice(templates)

        return template.format(
            brand=faker.company(),
            fruit=random.choice(["Apples", "Bananas", "Oranges", "Mangoes"]),
            vegetable=random.choice(
                ["Carrots", "Spinach", "Peas", "Broccoli"]),
            number=random.randint(2, 6)
        )
    except Exception as e:
        print(
            f"Error generating product name for category {row.get('product_category_name', 'Unknown')}: {e}")
        return f"Generic Product - {faker.word().capitalize()}"


def generate_products_data(generator, faker):
    """Generate product data with improved error handling."""
    print("4. Generating Products data...")

    product_categories = [
        "Groceries", "Health & Personal Care", "Beauty & Cosmetics", "Household Essentials",
        "Home Improvement & Tools", "Furniture", "Electronics & Accessories", "Appliances",
        "Clothing & Apparel", "Shoes & Footwear"
    ]

    try:
        products_config = [
            {"name": "product_id", "type": "custom", "params": {
                "prefix": "PROD", "delimiter": "_", "start": 100000001}},
            {"name": "product_category_name", "type": "category",
                "choices": product_categories},
            {"name": "product_description", "type": "paragraph",
                "params": {"nb_sentences": 3}},
            {"name": "product_price", "type": "float", "params": {
                "min": 20, "max": 100000, "round": 2}},
            {"name": "product_photos_qty", "type": "integer",
                "params": {"min": 1, "max": 10}},
            {"name": "product_weight_g", "type": "float",
                "params": {"min": 50, "max": 5000}},
            {"name": "product_length_cm", "type": "float",
                "params": {"min": 5, "max": 100}},
            {"name": "product_height_cm", "type": "float",
                "params": {"min": 1, "max": 50}},
            {"name": "product_width_cm", "type": "float",
                "params": {"min": 3, "max": 80}}
        ]

        products_df = generator.generate_dataset(
            n_rows=20000, columns_config=products_config)
        products_df["product_name"] = products_df.apply(
            lambda row: category_based_product_name(row, faker), axis=1
        )

        print(f"   ✓ Generated {len(products_df)} product records")
        return products_df, product_categories

    except Exception as e:
        print(f"Failed to generate product data: {e}")
        raise


def generate_orders_data(generator, customer_ids):
    """Generate orders data with improved date handling."""
    print("5. Generating Orders data...")

    order_statuses = ["delivered", "shipped",
                      "processing", "canceled", "pending"]

    try:
        orders_config = [
            {"name": "order_id", "type": "custom", "params": {
                "prefix": "ORD", "delimiter": "_", "start": 1000000001}},
            {"name": "customer_id", "type": "category", "choices": customer_ids},
            {"name": "order_status", "type": "category", "choices": order_statuses},
            {"name": "order_purchase_timestamp", "type": "datetime", "params": {
                "start": "2000-01-01T00:00:00",
                "end": "2024-12-31T23:59:59"
            }},
            {"name": "order_estimated_delivery_date", "type": "datetime", "params": {
                "start": "2000-01-01T00:00:00",
                "end": "2025-01-31T23:59:59"
            }}
        ]

        orders_df = generator.generate_dataset(
            n_rows=5000, columns_config=orders_config)

        orders_df['order_delivered_carrier_date'] = pd.NaT
        orders_df['order_delivered_customer_date'] = pd.NaT

        delivered_mask = orders_df['order_status'].isin(
            ['delivered', 'shipped'])
        if delivered_mask.any():
            orders_df.loc[delivered_mask, 'order_delivered_carrier_date'] = (
                pd.to_datetime(orders_df.loc[delivered_mask, 'order_purchase_timestamp']) +
                pd.to_timedelta(np.random.randint(
                    1, 6, size=delivered_mask.sum()), unit='D')
            )

            orders_df.loc[delivered_mask, 'order_delivered_customer_date'] = (
                pd.to_datetime(orders_df.loc[delivered_mask, 'order_delivered_carrier_date']) +
                pd.to_timedelta(np.random.randint(
                    0, 4, size=delivered_mask.sum()), unit='D')
            )

        print(f"   ✓ Generated {len(orders_df)} order records")
        return orders_df

    except Exception as e:
        print(f"Failed to generate order data: {e}")
        raise


def generate_order_items_data(orders_df, products_df, sellers_df):
    """Generate order items with improved price calculations."""
    print("6. Generating Order Items data...")

    try:
        order_items_data = []
        order_ids = orders_df['order_id'].tolist()
        product_ids = products_df['product_id'].tolist()
        seller_ids = sellers_df['seller_id'].tolist()

        price_lookup = dict(
            zip(products_df['product_id'], products_df['product_price']))

        for order_id in order_ids:
            n_items = np.random.randint(1, 5)

            for item_id in range(1, n_items + 1):
                product_id = np.random.choice(product_ids)
                seller_id = np.random.choice(seller_ids)
                price = price_lookup.get(product_id, 100.0)

                discount_pct = np.random.uniform(0, 0.2)
                final_price = price * (1 - discount_pct)

                freight_value = final_price * np.random.uniform(0.05, 0.15)

                order_date = orders_df[orders_df['order_id'] ==
                                       order_id]['order_purchase_timestamp'].iloc[0]
                shipping_limit = pd.to_datetime(
                    order_date) + pd.Timedelta(days=np.random.randint(1, 8))

                order_items_data.append({
                    'order_id': order_id,
                    'order_item_id': item_id,
                    'product_id': product_id,
                    'seller_id': seller_id,
                    'shipping_limit_date': shipping_limit,
                    'price': round(final_price, 2),
                    'freight_value': round(freight_value, 2),
                    'discount_pct': round(discount_pct, 2),
                    'coupon_applied': random.choice([True, False])
                })

        order_items_df = pd.DataFrame(order_items_data)
        print(f"   ✓ Generated {len(order_items_df)} order item records")
        return order_items_df

    except Exception as e:
        print(f"Failed to generate order items data: {e}")
        raise


def generate_payments_data(orders_df, order_items_df):
    """Generate payments data with improved installment logic."""
    print("7. Generating Payments data...")

    payment_types = ["credit_card", "debit_card", "gcash", "paymaya",
                     "bank_transfer", "cod", "installment"]

    try:
        payments_data = []

        for _, order in orders_df.iterrows():
            order_id = order['order_id']
            order_total = order_items_df[order_items_df['order_id']
                                         == order_id]['price'].sum()
            if order_total == 0:
                order_total = 100.0  # Default minimum order value

            payment_type = np.random.choice(payment_types)

            if payment_type == "installment":
                max_installments = max(1, min(12, int(order_total / 1000)))
                installments = (
                    np.random.randint(2, max_installments + 1)
                    if max_installments >= 2
                    else 1
                )
            elif payment_type in ["credit_card", "debit_card"]:
                installments = np.random.choice(
                    [1, 3, 6, 12], p=[0.7, 0.15, 0.1, 0.05])
            else:
                installments = 1

            # Create payment records
            for seq in range(1, installments + 1):
                payment_value = order_total / installments

                payments_data.append({
                    'order_id': order_id,
                    'payment_sequential': seq,
                    'payment_type': payment_type,
                    'payment_installments': installments,
                    'payment_value': round(payment_value, 2),
                    'payment_status': random.choice(['success', 'pending'])
                })

        payments_df = pd.DataFrame(payments_data)
        print(f"   ✓ Generated {len(payments_df)} payment records")
        return payments_df

    except Exception as e:
        print(f"Failed to generate payment data: {e}")
        raise


def upload_to_s3(s3_client, bucket_name, datasets):
    """Upload datasets to S3 with proper error handling."""
    print("8. Uploading datasets to MinIO...")

    uploaded_count = 0
    skipped_count = 0

    for name, df in datasets.items():
        filename = f"{name.lower().replace(' ', '_')}.csv"
        path = f"bronze/data/{filename}"

        try:
            s3_client.head_object(Bucket=bucket_name,
                                  Key=path)
            print(
                f"⚠️  File already exists in MinIO, skipping: {filename}")
            skipped_count += 1

        except ClientError as e:
            if e.response['Error']['Code'] == '404':
                try:
                    csv_buffer = StringIO()
                    df.to_csv(csv_buffer, index=False)
                    csv_buffer.seek(0)

                    s3_client.put_object(
                        Bucket=bucket_name,
                        Key=path,
                        Body=csv_buffer.getvalue(),
                        ContentType='text/csv'
                    )
                    print(
                        f"✓ Uploaded to MinIO: {filename} ({len(df)} records)")
                    uploaded_count += 1

                except Exception as upload_error:
                    print(
                        f"❌ Failed to upload {filename}: {upload_error}")
                    raise
            else:
                print(f"❌ Error checking {filename}: {e}")
                raise

    print(
        f"Upload summary: {uploaded_count} uploaded, {skipped_count} skipped")


def print_summary(datasets):
    """Print a summary of generated datasets."""
    print("\n=== Dataset Summary ===")
    total_records = 0

    for name, df in datasets.items():
        record_count = len(df)
        column_count = len(df.columns)
        print(
            f"{name:<20}: {record_count:>8,} records | {column_count:>2} columns")
        total_records += record_count

    print(f"{'TOTAL':<20}: {total_records:>8,} records")


if __name__ == "__main__":
    """Main function to orchestrate the data generation process."""
    try:
        generator, faker = initialize_generators()

        s3_client = create_s3_client()
        bucket_name = 'data-pipeline-storage'
        ensure_bucket_exists(s3_client, bucket_name)

        geoloc_df, city_region_mapping = generate_geolocation_data(generator)

        cities = list(city_region_mapping.keys())
        regions = list(set(info["region"]
                       for info in city_region_mapping.values()))

        customers_df = generate_customers_data(generator, cities, regions)
        products_df, product_categories = generate_products_data(
            generator, faker)
        sellers_df = generate_sellers_data(
            generator, cities, regions, product_categories)

        customer_ids = customers_df['customer_id'].tolist()
        orders_df = generate_orders_data(generator, customer_ids)

        order_items_df = generate_order_items_data(
            orders_df, products_df, sellers_df)
        payments_df = generate_payments_data(orders_df, order_items_df)

        datasets = {
            "Geolocation": geoloc_df,
            "Customers": customers_df,
            "Sellers": sellers_df,
            "Products": products_df,
            "Orders": orders_df,
            "Order Items": order_items_df,
            "Payments": payments_df
        }

        print_summary(datasets)
        upload_to_s3(s3_client, bucket_name, datasets)

        print("✅ Data generation and upload completed successfully!")

    except Exception as e:
        print(f"❌ Fatal error in main process: {e}")
        raise
