# transform.py
# Job: Clean and enrich the staging data
# Fixes issues found in quality checks
# Adds new useful columns for analysis
# Output: 4 clean DataFrames ready for the warehouse

import pandas as pd
from db_connection import get_engine

# -----------------------------------------------
# STEP 1: Load staging tables from PostgreSQL
# -----------------------------------------------
# Same as we did in quality_checks.py
# We always work from the staging tables
# never from the original CSV files

def load_staging_tables(engine):
    print("📂 Loading staging tables...\n")

    customers   = pd.read_sql("SELECT * FROM stg_customers",   engine)
    products    = pd.read_sql("SELECT * FROM stg_products",    engine)
    orders      = pd.read_sql("SELECT * FROM stg_orders",      engine)
    order_items = pd.read_sql("SELECT * FROM stg_order_items", engine)

    print(f"   stg_customers   : {len(customers):,} rows")
    print(f"   stg_products    : {len(products):,} rows")
    print(f"   stg_orders      : {len(orders):,} rows")
    print(f"   stg_order_items : {len(order_items):,} rows")
    print()

    return customers, products, orders, order_items

# -----------------------------------------------
# STEP 2: Transform Products
# -----------------------------------------------
def transform_products(products):
    print("=" * 50)
    print("  TRANSFORMING: products")
    print("=" * 50)

    # Make a copy so we never modify the original
    # This is best practice — always work on a copy
    df = products.copy()

    # --- Fix 1: Fill null category names ---
    # We found 610 nulls in quality checks
    # We fill them with 'Unknown' so they don't
    # break our category analysis later
    null_before = df["product_category_name"].isnull().sum()
    df["product_category_name"] = df["product_category_name"].fillna("Unknown")
    null_after = df["product_category_name"].isnull().sum()
    print(f"  ✅ Filled {null_before} null categories with 'Unknown'")
    print(f"     Nulls remaining: {null_after}")

    # --- Fix 2: Fix zero or negative weights ---
    # We found 4 products with bad weights
    # We replace them with the average weight
    # of all valid products — this is called
    # "mean imputation" — a standard technique
    avg_weight = df[df["product_weight_g"] > 0]["product_weight_g"].mean()
    bad_weights = df[df["product_weight_g"] <= 0].shape[0]
    df.loc[df["product_weight_g"] <= 0, "product_weight_g"] = avg_weight
    print(f"\n  ✅ Replaced {bad_weights} bad weights with avg: {avg_weight:.2f}g")

    # --- New Column: weight_category ---
    # Group products into weight buckets
    # This helps with delivery time and cost analysis
    def categorise_weight(weight):
        if weight <= 500:
            return "Light"
        elif weight <= 5000:
            return "Medium"
        else:
            return "Heavy"

    df["weight_category"] = df["product_weight_g"].apply(categorise_weight)
    print(f"\n  ✅ Added 'weight_category' column")
    print(f"     {df['weight_category'].value_counts().to_dict()}")

    # --- Show final shape ---
    print(f"\n  Shape: {df.shape[0]:,} rows, {df.shape[1]} columns")
    print(f"  Columns: {list(df.columns)}\n")

    return df

# -----------------------------------------------
# STEP 3: Transform Orders
# -----------------------------------------------
def transform_orders(orders):
    print("=" * 50)
    print("  TRANSFORMING: orders")
    print("=" * 50)

    df = orders.copy()

    # --- Fix 1: Parse all date columns ---
    # In staging, dates are stored as plain text strings
    # We convert them to proper datetime objects
    # errors="coerce" turns unparseable dates into NaT
    date_columns = [
        "order_purchase_timestamp",
        "order_approved_at",
        "order_delivered_carrier_date",
        "order_delivered_customer_date",
        "order_estimated_delivery_date"
    ]

    for col in date_columns:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce")

    print(f"  ✅ Parsed {len(date_columns)} date columns to datetime")

    # --- New Columns: Extract date parts ---
    # We extract year, month, day, hour from purchase date
    # These become very useful for time-based analysis
    # For example: "show me revenue by month" needs order_month
    df["order_year"]  = df["order_purchase_timestamp"].dt.year
    df["order_month"] = df["order_purchase_timestamp"].dt.month
    df["order_day"]   = df["order_purchase_timestamp"].dt.day
    df["order_hour"]  = df["order_purchase_timestamp"].dt.hour

    print(f"  ✅ Extracted year, month, day, hour from purchase date")

    # --- New Column: delivery_time_days ---
    # How many days did delivery take?
    # = delivered date - purchase date
    # This is useful for delivery performance analysis
    # Some orders won't have delivery date (cancelled etc)
    # For those, delivery_time_days will be NaT — that's fine
    df["delivery_time_days"] = (
        df["order_delivered_customer_date"] -
        df["order_purchase_timestamp"]
    ).dt.days

    delivered = df["delivery_time_days"].notnull().sum()
    print(f"  ✅ Calculated delivery_time_days")
    print(f"     {delivered:,} orders have delivery time data")
    avg_delivery = df["delivery_time_days"].mean()
    print(f"     Average delivery time: {avg_delivery:.1f} days")

    # --- Fix 2: Standardise order_status ---
    # Convert to title case for consistency
    # "delivered" → "Delivered"
    # "canceled"  → "Canceled"
    df["order_status"] = df["order_status"].str.title()
    print(f"\n  ✅ Standardised order_status to title case")
    print(f"     Status counts: {df['order_status'].value_counts().to_dict()}")

    # --- Show final shape ---
    print(f"\n  Shape: {df.shape[0]:,} rows, {df.shape[1]} columns")
    print(f"  New columns added: order_year, order_month, order_day,")
    print(f"                     order_hour, delivery_time_days\n")

    return df

# -----------------------------------------------
# STEP 4: Transform Order Items
# -----------------------------------------------
def transform_order_items(order_items):
    print("=" * 50)
    print("  TRANSFORMING: order_items")
    print("=" * 50)

    df = order_items.copy()

    # --- New Column: total_amount ---
    # This is what the customer actually paid
    # price = product price
    # freight_value = shipping cost
    # total_amount = price + freight_value
    df["total_amount"] = df["price"] + df["freight_value"]
    print(f"  ✅ Calculated total_amount = price + freight_value")
    print(f"     Total revenue across all orders: R${df['total_amount'].sum():,.2f}")
    print(f"     Average order item value: R${df['total_amount'].mean():,.2f}")

    # --- New Column: price_category ---
    # Group items into price buckets
    # Useful for understanding what price range sells most
    def categorise_price(price):
        if price <= 50:
            return "Budget"
        elif price <= 200:
            return "Mid-Range"
        elif price <= 500:
            return "Premium"
        else:
            return "Luxury"

    df["price_category"] = df["price"].apply(categorise_price)
    print(f"\n  ✅ Added price_category column")
    print(f"     {df['price_category'].value_counts().to_dict()}")

    # --- Show final shape ---
    print(f"\n  Shape: {df.shape[0]:,} rows, {df.shape[1]} columns")
    print(f"  New columns added: total_amount, price_category\n")

    return df

# -----------------------------------------------
# STEP 5: Transform Customers
# -----------------------------------------------
def transform_customers(customers):
    print("=" * 50)
    print("  TRANSFORMING: customers")
    print("=" * 50)

    df = customers.copy()

    # --- Fix 1: Standardise city names ---
    # Convert to title case for consistency
    # "sao paulo" → "Sao Paulo"
    # "SAO PAULO" → "Sao Paulo"
    df["customer_city"] = df["customer_city"].str.title()
    print(f"  ✅ Standardised customer_city to title case")

    # --- Fix 2: Standardise state names ---
    # Convert to uppercase for consistency
    # "sp" → "SP"
    df["customer_state"] = df["customer_state"].str.upper()
    print(f"  ✅ Standardised customer_state to uppercase")

    # --- New Column: customer_region ---
    # Group Brazilian states into 5 regions
    # This makes regional analysis much cleaner
    region_map = {
        # Southeast
        "SP": "Southeast", "RJ": "Southeast",
        "MG": "Southeast", "ES": "Southeast",
        # South
        "PR": "South", "SC": "South", "RS": "South",
        # Northeast
        "BA": "Northeast", "PE": "Northeast", "CE": "Northeast",
        "MA": "Northeast", "PB": "Northeast", "RN": "Northeast",
        "AL": "Northeast", "SE": "Northeast", "PI": "Northeast",
        # North
        "AM": "North", "PA": "North", "RO": "North",
        "AC": "North", "AP": "North", "RR": "North", "TO": "North",
        # Central-West
        "GO": "Central-West", "MT": "Central-West",
        "MS": "Central-West", "DF": "Central-West"
    }

    # .map() replaces each state code with its region name
    # If a state code is not in our map, it becomes "Other"
    df["customer_region"] = df["customer_state"].map(region_map).fillna("Other")
    print(f"\n  ✅ Added customer_region column")
    print(f"     {df['customer_region'].value_counts().to_dict()}")

    # --- Show final shape ---
    print(f"\n  Shape: {df.shape[0]:,} rows, {df.shape[1]} columns")
    print(f"  New columns added: customer_region\n")

    return df

# -----------------------------------------------
# STEP 6: Save clean tables to PostgreSQL
# -----------------------------------------------
def save_clean_tables(clean_customers, clean_products,
                      clean_orders, clean_order_items, engine):
    print("=" * 50)
    print("  SAVING CLEAN TABLES TO POSTGRESQL")
    print("=" * 50)

    tables = {
        "clean_customers"   : clean_customers,
        "clean_products"    : clean_products,
        "clean_orders"      : clean_orders,
        "clean_order_items" : clean_order_items
    }

    for table_name, df in tables.items():
        print(f"\n  ⏳ Saving {table_name}...")
        df.to_sql(
            name      = table_name,
            con       = engine,
            if_exists = "replace",
            index     = False
        )
        print(f"  ✅ {table_name} saved — {len(df):,} rows")

    print("\n" + "=" * 50)
    print("  ✅ All clean tables saved to PostgreSQL!")
    print("=" * 50)

if __name__ == "__main__":
    engine = get_engine()

    # Load staging tables
    customers, products, orders, order_items = load_staging_tables(engine)

    # Transform all 4 tables
    clean_products    = transform_products(products)
    clean_orders      = transform_orders(orders)
    clean_order_items = transform_order_items(order_items)
    clean_customers   = transform_customers(customers)

    # Save clean tables to PostgreSQL
    save_clean_tables(
        clean_customers,
        clean_products,
        clean_orders,
        clean_order_items,
        engine
    )

    print("\n✅ Step 4 — Transformation complete!")