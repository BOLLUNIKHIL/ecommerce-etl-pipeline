# staging.py
# Job: Load all 4 CSV files into PostgreSQL staging tables
# Tables created: stg_customers, stg_products, stg_orders, stg_order_items

import pandas as pd
from db_connection import get_engine

# -----------------------------------------------
# STEP 1: Define file paths
# -----------------------------------------------
CUSTOMERS_PATH   = "data/raw/customers.csv"
PRODUCTS_PATH    = "data/raw/products.csv"
ORDERS_PATH      = "data/raw/orders.csv"
ORDER_ITEMS_PATH = "data/raw/order_items.csv"


# -----------------------------------------------
# STEP 2: Read CSVs into DataFrames
# -----------------------------------------------
def read_csv_files():
    print("📂 Reading CSV files...\n")

    customers   = pd.read_csv(CUSTOMERS_PATH)
    products    = pd.read_csv(PRODUCTS_PATH)
    orders      = pd.read_csv(ORDERS_PATH)
    order_items = pd.read_csv(ORDER_ITEMS_PATH)

    print(f"   customers   : {len(customers):,} rows")
    print(f"   products    : {len(products):,} rows")
    print(f"   orders      : {len(orders):,} rows")
    print(f"   order_items : {len(order_items):,} rows")

    return customers, products, orders, order_items


# -----------------------------------------------
# STEP 3: Load each DataFrame into PostgreSQL
# -----------------------------------------------
# if_exists="replace" means:
#   - if the table already exists, drop it and recreate it
#   - if it doesn't exist, create it fresh
# index=False means:
#   - don't write the pandas row numbers as a column
def load_to_staging(df, table_name, engine):
    print(f"\n⏳ Loading {table_name}...")

    df.to_sql(
        name      = table_name,
        con       = engine,
        if_exists = "replace",
        index     = False
    )

    print(f"✅ {table_name} loaded — {len(df):,} rows")


# -----------------------------------------------
# STEP 4: Main function that runs everything
# -----------------------------------------------
def run_staging():
    print("=" * 50)
    print("  STAGING LAYER — Loading raw data")
    print("=" * 50)

    # Get database connection
    engine = get_engine()

    # Read all CSV files
    customers, products, orders, order_items = read_csv_files()

    # Load each one into PostgreSQL
    print("\n📤 Loading into PostgreSQL...\n")
    load_to_staging(customers,   "stg_customers",   engine)
    load_to_staging(products,    "stg_products",    engine)
    load_to_staging(orders,      "stg_orders",      engine)
    load_to_staging(order_items, "stg_order_items", engine)

    print("\n" + "=" * 50)
    print("✅ Staging complete — all 4 tables loaded!")
    print("=" * 50)


# -----------------------------------------------
# Run when this file is executed directly
# -----------------------------------------------
if __name__ == "__main__":
    run_staging()