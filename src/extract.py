# extract.py
# Job: Read the 4 CSV files into pandas DataFrames

import pandas as pd
from dotenv import load_dotenv

load_dotenv()

# -----------------------------------------------
# STEP 1: Define file paths
# -----------------------------------------------
CUSTOMERS_PATH   = "data/raw/customers.csv"
PRODUCTS_PATH    = "data/raw/products.csv"
ORDERS_PATH      = "data/raw/orders.csv"
ORDER_ITEMS_PATH = "data/raw/order_items.csv"


# -----------------------------------------------
# STEP 2: Read each CSV into a DataFrame
# -----------------------------------------------
def extract_data():

    print("📂 Reading CSV files...\n")

    customers   = pd.read_csv(CUSTOMERS_PATH)
    products    = pd.read_csv(PRODUCTS_PATH)
    orders      = pd.read_csv(ORDERS_PATH)
    order_items = pd.read_csv(ORDER_ITEMS_PATH)

    # -----------------------------------------------
    # STEP 3: Print shape (rows, columns) of each
    # -----------------------------------------------
    print(f"✅ customers    : {customers.shape[0]:,} rows, {customers.shape[1]} columns")
    print(f"✅ products     : {products.shape[0]:,} rows, {products.shape[1]} columns")
    print(f"✅ orders       : {orders.shape[0]:,} rows, {orders.shape[1]} columns")
    print(f"✅ order_items  : {order_items.shape[0]:,} rows, {order_items.shape[1]} columns")

    # -----------------------------------------------
    # STEP 4: Preview first 2 rows of each
    # -----------------------------------------------
    print("\n--- customers preview ---")
    print(customers.head(2).to_string())

    print("\n--- products preview ---")
    print(products.head(2).to_string())

    print("\n--- orders preview ---")
    print(orders.head(2).to_string())

    print("\n--- order_items preview ---")
    print(order_items.head(2).to_string())

    # -----------------------------------------------
    # STEP 5: Return all 4 DataFrames
    # -----------------------------------------------
    return customers, products, orders, order_items


# -----------------------------------------------
# Run the function when this file is executed
# -----------------------------------------------
if __name__ == "__main__":
    customers, products, orders, order_items = extract_data()
    print("\n✅ Extraction complete — all 4 files loaded successfully!")