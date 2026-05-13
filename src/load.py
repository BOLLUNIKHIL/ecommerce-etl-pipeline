# load.py
# Job: Load transformed data into the star schema
# Order: dimensions first, then fact table

import pandas as pd
from db_connection import get_engine
from sqlalchemy import text

# -----------------------------------------------
# STEP 1: Load clean tables from PostgreSQL
# -----------------------------------------------
def load_clean_tables(engine):
    print("📂 Loading clean tables...\n")

    customers   = pd.read_sql("SELECT * FROM clean_customers",   engine)
    products    = pd.read_sql("SELECT * FROM clean_products",    engine)
    orders      = pd.read_sql("SELECT * FROM clean_orders",      engine)
    order_items = pd.read_sql("SELECT * FROM clean_order_items", engine)

    print(f"   clean_customers   : {len(customers):,} rows")
    print(f"   clean_products    : {len(products):,} rows")
    print(f"   clean_orders      : {len(orders):,} rows")
    print(f"   clean_order_items : {len(order_items):,} rows")
    print()

    return customers, products, orders, order_items

# -----------------------------------------------
# HELPER: Truncate a table before loading
# This prevents duplicate rows if script runs twice
# -----------------------------------------------
def truncate_table(table_name, engine):
    with engine.connect() as conn:
        conn.execute(text(f"TRUNCATE TABLE {table_name} CASCADE;"))
        conn.commit()
    print(f"  🗑️  {table_name} truncated")

# -----------------------------------------------
# STEP 2: Load dim_customer
# -----------------------------------------------
def load_dim_customer(customers, engine):
    print("=" * 50)
    print("  LOADING: dim_customer")
    print("=" * 50)
    truncate_table("dim_customer", engine)

    # Select only the columns we need
    # We drop customer_unique_id — not needed for analysis
    dim_customer = customers[[
        "customer_id",
        "customer_city",
        "customer_state",
        "customer_region"
    ]].copy()

    # Remove any duplicate customer_ids
    # Keep the first occurrence of each
    before = len(dim_customer)
    dim_customer = dim_customer.drop_duplicates(subset=["customer_id"])
    after = len(dim_customer)
    print(f"  Removed {before - after} duplicate customer_ids")

    # Load into PostgreSQL
    # if_exists="append" means ADD rows to existing table
    # We already created the empty table in create_schema.sql
    # So we append data into it — not replace the structure
    dim_customer.to_sql(
        name      = "dim_customer",
        con       = engine,
        if_exists = "append",
        index     = False
    )

    print(f"  ✅ dim_customer loaded — {len(dim_customer):,} rows")
    print()
    return dim_customer
# -----------------------------------------------
# STEP 3: Load dim_product
# -----------------------------------------------
def load_dim_product(products, engine):
    print("=" * 50)
    print("  LOADING: dim_product")
    print("=" * 50)
    truncate_table("dim_product", engine)

    # Select only columns needed for analysis
    # We drop dimension columns not useful for warehouse
    dim_product = products[[
        "product_id",
        "product_category_name",
        "product_weight_g",
        "weight_category"
    ]].copy()

    # Remove duplicate product_ids
    before = len(dim_product)
    dim_product = dim_product.drop_duplicates(subset=["product_id"])
    after = len(dim_product)
    print(f"  Removed {before - after} duplicate product_ids")

    # Load into PostgreSQL
    dim_product.to_sql(
        name      = "dim_product",
        con       = engine,
        if_exists = "append",
        index     = False
    )

    print(f"  ✅ dim_product loaded — {len(dim_product):,} rows")
    print()
    return dim_product

# -----------------------------------------------
# STEP 4: Load dim_date
# -----------------------------------------------
def load_dim_date(orders, engine):
    print("=" * 50)
    print("  LOADING: dim_date")
    print("=" * 50)
    truncate_table("dim_date", engine)

    # Parse the purchase timestamp to datetime
    # We need proper datetime to extract date parts
    orders["order_purchase_timestamp"] = pd.to_datetime(
        orders["order_purchase_timestamp"], errors="coerce"
    )

    # Get all unique order timestamps
    # We only keep non-null timestamps
    unique_dates = orders["order_purchase_timestamp"].dropna().unique()
    print(f"  Unique order timestamps found: {len(unique_dates):,}")

    # Build dim_date DataFrame from unique timestamps
    # For each unique timestamp we extract all date parts
    dim_date = pd.DataFrame({
        "full_date" : unique_dates
    })

    # Extract date parts from full_date
    dim_date["year"]       = pd.to_datetime(dim_date["full_date"]).dt.year
    dim_date["month"]      = pd.to_datetime(dim_date["full_date"]).dt.month
    dim_date["day"]        = pd.to_datetime(dim_date["full_date"]).dt.day
    dim_date["hour"]       = pd.to_datetime(dim_date["full_date"]).dt.hour

    # month_name converts month number to name
    # 1 → January, 2 → February etc
    dim_date["month_name"] = pd.to_datetime(
        dim_date["full_date"]
    ).dt.strftime("%B")

    # Sort by date so date_key follows chronological order
    dim_date = dim_date.sort_values("full_date").reset_index(drop=True)

    print(f"  Date range: {dim_date['full_date'].min()} "
          f"to {dim_date['full_date'].max()}")
    print(f"  Years covered: {sorted(dim_date['year'].unique().tolist())}")

    # Load into PostgreSQL
    dim_date.to_sql(
        name      = "dim_date",
        con       = engine,
        if_exists = "append",
        index     = False
    )

    print(f"  ✅ dim_date loaded — {len(dim_date):,} rows")
    print()
    return dim_date
# -----------------------------------------------
# STEP 5: Load dim_location
# -----------------------------------------------
def load_dim_location(customers, engine):
    print("=" * 50)
    print("  LOADING: dim_location")
    print("=" * 50)
    truncate_table("dim_location", engine)

    # Select location columns from customers
    dim_location = customers[[
        "customer_city",
        "customer_state",
        "customer_region"
    ]].copy()

    # Keep only unique city + state combinations
    # Many customers share the same city and state
    # We only want each location once
    before = len(dim_location)
    dim_location = dim_location.drop_duplicates(
        subset=["customer_city", "customer_state"]
    )
    after = len(dim_location)

    print(f"  Total customer rows      : {before:,}")
    print(f"  Unique city+state combos : {after:,}")
    print(f"  Duplicates removed       : {before - after:,}")

    # Sort by state then city for clean ordering
    dim_location = dim_location.sort_values(
        ["customer_state", "customer_city"]
    ).reset_index(drop=True)

    # Load into PostgreSQL
    dim_location.to_sql(
        name      = "dim_location",
        con       = engine,
        if_exists = "append",
        index     = False
    )

    print(f"\n  ✅ dim_location loaded — {len(dim_location):,} rows")
    print(f"  States covered: {dim_location['customer_state'].nunique()}")
    print(f"  Regions: {dim_location['customer_region'].value_counts().to_dict()}")
    print()
    return dim_location

# -----------------------------------------------
# STEP 6: Load fact_sales
# -----------------------------------------------
def load_fact_sales(order_items, orders, customers,engine):
    print("=" * 50)
    print("  LOADING: fact_sales")
    print("=" * 50)
    truncate_table("fact_sales", engine)

    # -----------------------------------------------
    # First read dimension tables back from PostgreSQL
    # We need their surrogate keys (customer_key etc)
    # These keys were auto-generated by SERIAL
    # when we loaded the dimensions
    # -----------------------------------------------
    print("  Reading dimension keys from PostgreSQL...")

    dim_customer = pd.read_sql(
        "SELECT customer_key, customer_id FROM dim_customer",
        engine
    )
    dim_product = pd.read_sql(
        "SELECT product_key, product_id FROM dim_product",
        engine
    )
    dim_date = pd.read_sql(
        "SELECT date_key, full_date FROM dim_date",
        engine
    )
    dim_location = pd.read_sql(
        "SELECT location_key, customer_city, customer_state FROM dim_location",
        engine
    )

    # -----------------------------------------------
    # Build the base fact table
    # Join order_items with orders to get all info
    # we need in one place
    # -----------------------------------------------
    print("  Building fact table base...")

    # Parse timestamp before merging
    orders["order_purchase_timestamp"] = pd.to_datetime(
        orders["order_purchase_timestamp"], errors="coerce"
    )

    # Merge order_items with orders
    # This gives us customer_id, status, timestamp
    # alongside price and product_id
    fact = order_items.merge(
        orders[[
            "order_id",
            "customer_id",
            "order_status",
            "order_purchase_timestamp",
            "delivery_time_days"
        ]],
        on="order_id",
        how="left"
    )
    print(f"  After joining orders: {len(fact):,} rows")

    # Merge with customers to get city and state
    # We need these to look up location_key
    fact = fact.merge(
        customers[[
            "customer_id",
            "customer_city",
            "customer_state"
        ]],
        on="customer_id",
        how="left"
    )
    print(f"  After joining customers: {len(fact):,} rows")

    # -----------------------------------------------
    # Look up surrogate keys from each dimension
    # Replace natural keys with integer dimension keys
    # -----------------------------------------------
    print("  Looking up dimension keys...")

    # Look up customer_key
    fact = fact.merge(
        dim_customer[["customer_key", "customer_id"]],
        on="customer_id",
        how="left"
    )

    # Look up product_key
    fact = fact.merge(
        dim_product[["product_key", "product_id"]],
        on="product_id",
        how="left"
    )

    # Look up date_key
    # Match on full_date = order_purchase_timestamp
    dim_date["full_date"] = pd.to_datetime(dim_date["full_date"])
    fact = fact.merge(
        dim_date[["date_key", "full_date"]],
        left_on="order_purchase_timestamp",
        right_on="full_date",
        how="left"
    )

    # Look up location_key
    fact = fact.merge(
        dim_location[[
            "location_key",
            "customer_city",
            "customer_state"
        ]],
        on=["customer_city", "customer_state"],
        how="left"
    )

    print(f"  Dimension keys looked up successfully")

    # -----------------------------------------------
    # Select only the columns fact_sales needs
    # Drop all the natural key columns we used
    # for lookups — we don't need them anymore
    # -----------------------------------------------
    fact_sales = fact[[
        "order_id",
        "order_item_id",
        "customer_key",
        "product_key",
        "date_key",
        "location_key",
        "price",
        "freight_value",
        "total_amount",
        "order_status",
        "delivery_time_days"
    ]].copy()

    # -----------------------------------------------
    # Check for any null keys before loading
    # Null keys mean a lookup failed somewhere
    # -----------------------------------------------
    key_cols = [
        "customer_key", "product_key",
        "date_key", "location_key"
    ]
    for col in key_cols:
        nulls = fact_sales[col].isnull().sum()
        if nulls > 0:
            print(f"  ⚠️  {col} has {nulls} null values")
        else:
            print(f"  ✅ {col} — all keys matched")

    # -----------------------------------------------
    # Load into PostgreSQL
    # -----------------------------------------------
    print(f"\n  ⏳ Loading {len(fact_sales):,} rows into fact_sales...")

    fact_sales.to_sql(
        name      = "fact_sales",
        con       = engine,
        if_exists = "append",
        index     = False,
        chunksize = 1000
    )

    print(f"  ✅ fact_sales loaded — {len(fact_sales):,} rows")
    print()
    return fact_sales

if __name__ == "__main__":
    engine = get_engine()

    # Load clean tables
    customers, products, orders, order_items = load_clean_tables(engine)

    # Load all dimensions first
    dim_customer = load_dim_customer(customers, engine)
    dim_product  = load_dim_product(products, engine)
    dim_date     = load_dim_date(orders, engine)
    dim_location = load_dim_location(customers, engine)

    # Load fact table last
    fact_sales = load_fact_sales(
        order_items, orders, customers, engine
    )

    # Final summary
    print("=" * 50)
    print("✅ STAR SCHEMA FULLY LOADED!")
    print(f"   dim_customer : {len(dim_customer):,} rows")
    print(f"   dim_product  : {len(dim_product):,} rows")
    print(f"   dim_date     : {len(dim_date):,} rows")
    print(f"   dim_location : {len(dim_location):,} rows")
    print(f"   fact_sales   : {len(fact_sales):,} rows")
    print("=" * 50)