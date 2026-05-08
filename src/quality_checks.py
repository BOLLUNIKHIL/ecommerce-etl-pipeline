# quality_checks.py
# Job: Check the staging tables for data quality issues
# and save a report to logs/quality_report.txt

import pandas as pd
from db_connection import get_engine
from datetime import datetime

# -----------------------------------------------
# STEP 1: Setup
# -----------------------------------------------

# This list will collect all issues we find
# At the end we save it to a file
issues_found = []

# This function adds a message to our issues list
# and prints it at the same time
def log_issue(message):
    print(message)
    issues_found.append(message)

# This function prints and logs a passing check
def log_pass(message):
    print(message)


# -----------------------------------------------
# STEP 2: Load staging tables from PostgreSQL
# -----------------------------------------------
# Remember — we already loaded these in staging.py
# Now we read them back from PostgreSQL into pandas
# so we can check them with Python

def load_staging_tables(engine):
    print("📂 Loading staging tables from PostgreSQL...\n")

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
# STEP 3: Check 1 — Null Checks
# -----------------------------------------------
# We check the most important columns in each table
# Not every column needs to be null-free
# Only the ones that matter for our analysis

def check_nulls(customers, products, orders, order_items):
    print("=" * 50)
    print("  CHECK 1 — Null Values")
    print("=" * 50)

    # --- Customers ---
    # customer_id is the unique identifier — must never be null
    # customer_city is needed for location analysis
    # customer_state is needed for location analysis
    cols_to_check = ["customer_id", "customer_city", "customer_state"]
    for col in cols_to_check:
        null_count = customers[col].isnull().sum()
        if null_count > 0:
            log_issue(f"  ⚠️  [customers] '{col}' has {null_count} null values")
        else:
            log_pass(f"  ✅ [customers] '{col}' — no nulls")

    # --- Products ---
    # product_id must never be null
    # product_category_name is needed for category analysis
    cols_to_check = ["product_id", "product_category_name"]
    for col in cols_to_check:
        null_count = products[col].isnull().sum()
        if null_count > 0:
            log_issue(f"  ⚠️  [products] '{col}' has {null_count} null values")
        else:
            log_pass(f"  ✅ [products] '{col}' — no nulls")

    # --- Orders ---
    # order_id must never be null
    # customer_id must never be null — we need to know who ordered
    # order_status tells us if order was completed or cancelled
    # order_purchase_timestamp is the order date — critical for time analysis
    cols_to_check = ["order_id", "customer_id", 
                     "order_status", "order_purchase_timestamp"]
    for col in cols_to_check:
        null_count = orders[col].isnull().sum()
        if null_count > 0:
            log_issue(f"  ⚠️  [orders] '{col}' has {null_count} null values")
        else:
            log_pass(f"  ✅ [orders] '{col}' — no nulls")

    # --- Order Items ---
    # order_id and product_id must never be null
    # price must never be null — needed for revenue calculation
    cols_to_check = ["order_id", "product_id", "price"]
    for col in cols_to_check:
        null_count = order_items[col].isnull().sum()
        if null_count > 0:
            log_issue(f"  ⚠️  [order_items] '{col}' has {null_count} null values")
        else:
            log_pass(f"  ✅ [order_items] '{col}' — no nulls")

    print()
    

# -----------------------------------------------
# STEP 4: Check 2 — Duplicate Checks
# -----------------------------------------------
# We check if the same primary key appears more than once
# A primary key is the unique identifier for each row
# For example order_id should appear only once in orders table

def check_duplicates(customers, products, orders, order_items):
    print("=" * 50)
    print("  CHECK 2 — Duplicate Records")
    print("=" * 50)

    # --- Customers ---
    # Each customer_id should appear only once
    dupes = customers.duplicated(subset=["customer_id"]).sum()
    if dupes > 0:
        log_issue(f"  ⚠️  [customers] {dupes} duplicate customer_id values found")
    else:
        log_pass(f"  ✅ [customers] no duplicate customer_id values")

    # --- Products ---
    # Each product_id should appear only once
    dupes = products.duplicated(subset=["product_id"]).sum()
    if dupes > 0:
        log_issue(f"  ⚠️  [products] {dupes} duplicate product_id values found")
    else:
        log_pass(f"  ✅ [products] no duplicate product_id values")

    # --- Orders ---
    # Each order_id should appear only once
    dupes = orders.duplicated(subset=["order_id"]).sum()
    if dupes > 0:
        log_issue(f"  ⚠️  [orders] {dupes} duplicate order_id values found")
    else:
        log_pass(f"  ✅ [orders] no duplicate order_id values")

    # --- Order Items ---
    # order_items can have multiple rows per order_id (one per product)
    # But the combination of order_id + order_item_id must be unique
    dupes = order_items.duplicated(subset=["order_id", "order_item_id"]).sum()
    if dupes > 0:
        log_issue(f"  ⚠️  [order_items] {dupes} duplicate order_id+order_item_id found")
    else:
        log_pass(f"  ✅ [order_items] no duplicate order_id+order_item_id values")

    print()
    

# -----------------------------------------------
# STEP 5: Check 3 — Invalid Values
# -----------------------------------------------
# We check for values that exist but make no logical sense
# For example negative prices, zero prices, invalid statuses

def check_invalid_values(products, orders, order_items):
    print("=" * 50)
    print("  CHECK 3 — Invalid Values")
    print("=" * 50)

    # --- Products ---
    # Price columns don't exist in products table
    # but we can check for any unreasonable product dimensions
    # product_weight_g should never be zero or negative
    if "product_weight_g" in products.columns:
        invalid = products[products["product_weight_g"] <= 0]["product_weight_g"].count()
        if invalid > 0:
            log_issue(f"  ⚠️  [products] {invalid} rows with zero or negative weight")
        else:
            log_pass(f"  ✅ [products] all product weights are positive")

    # --- Orders ---
    # order_status must be one of these known valid values only
    # anything else means bad data came in
    valid_statuses = [
        "delivered", "shipped", "canceled",
        "unavailable", "invoiced", "processing",
        "created", "approved"
    ]
    invalid_status = orders[~orders["order_status"].isin(valid_statuses)]
    if len(invalid_status) > 0:
        log_issue(f"  ⚠️  [orders] {len(invalid_status)} rows with invalid order_status")
        # Show what the bad values actually are
        bad_values = invalid_status["order_status"].unique()
        log_issue(f"      Bad values found: {bad_values}")
    else:
        log_pass(f"  ✅ [orders] all order_status values are valid")

    # --- Order Items ---
    # price must always be greater than zero
    # you cannot have a free or negative priced item
    invalid_price = order_items[order_items["price"] <= 0]
    if len(invalid_price) > 0:
        log_issue(f"  ⚠️  [order_items] {len(invalid_price)} rows with price <= 0")
    else:
        log_pass(f"  ✅ [order_items] all prices are greater than zero")

    # freight_value can be zero (free shipping promotions exist)
    # but it should never be negative
    invalid_freight = order_items[order_items["freight_value"] < 0]
    if len(invalid_freight) > 0:
        log_issue(f"  ⚠️  [order_items] {len(invalid_freight)} rows with negative freight_value")
    else:
        log_pass(f"  ✅ [order_items] all freight values are zero or positive")

    print()
    
# -----------------------------------------------
# STEP 6: Check 4 — Date Validation
# -----------------------------------------------
# We check that dates make logical sense
# Olist dataset covers 2016 to 2018
# So any date outside that range is suspicious

def check_dates(orders):
    print("=" * 50)
    print("  CHECK 4 — Date Validation")
    print("=" * 50)

    # First convert the date column from text to actual datetime
    # In the staging table dates are stored as plain text strings
    # pd.to_datetime() converts them to proper date objects
    # errors="coerce" means if a date string is invalid/unparseable
    # instead of crashing it just turns that value into NaT (Not a Time)
    orders["order_purchase_timestamp"] = pd.to_datetime(
        orders["order_purchase_timestamp"], errors="coerce"
    )

    # --- Check 4a: unparseable dates ---
    # If any date couldn't be parsed, to_datetime turned it into NaT
    # NaT is like NaN but for dates — it means "no date value"
    nat_count = orders["order_purchase_timestamp"].isnull().sum()
    if nat_count > 0:
        log_issue(f"  ⚠️  [orders] {nat_count} rows with unparseable order dates")
    else:
        log_pass(f"  ✅ [orders] all order dates are parseable")

    # --- Check 4b: future dates ---
    # No order can have a purchase date in the future
    # pd.Timestamp.now() gives us the current date and time
    future_orders = orders[orders["order_purchase_timestamp"] > pd.Timestamp.now()]
    if len(future_orders) > 0:
        log_issue(f"  ⚠️  [orders] {len(future_orders)} orders with future purchase dates")
    else:
        log_pass(f"  ✅ [orders] no future order dates found")

    # --- Check 4c: dates too old ---
    # Olist was founded in 2015 and dataset starts from 2016
    # Any order before 2016 is suspicious and likely bad data
    too_old = orders[
        orders["order_purchase_timestamp"] < pd.Timestamp("2016-01-01")
    ]
    if len(too_old) > 0:
        log_issue(f"  ⚠️  [orders] {len(too_old)} orders before 2016 (suspicious)")
    else:
        log_pass(f"  ✅ [orders] no suspiciously old dates found")

    # --- Check 4d: dates too far in future ---
    # Dataset should only go up to end of 2018
    # Anything after 2019 is definitely wrong
    too_new = orders[
        orders["order_purchase_timestamp"] > pd.Timestamp("2019-01-01")
    ]
    if len(too_new) > 0:
        log_issue(f"  ⚠️  [orders] {len(too_new)} orders after 2019 (suspicious)")
    else:
        log_pass(f"  ✅ [orders] no suspiciously future dates found")

    print()
    

# -----------------------------------------------
# STEP 7: Save quality report to a file
# -----------------------------------------------
def save_report():
    timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    report_path = f"logs/quality_report_{timestamp}.txt"

    with open(report_path, "w", encoding="utf-8") as f:
        f.write("=" * 50 + "\n")
        f.write("  DATA QUALITY REPORT\n")
        f.write(f"  Run at: {timestamp}\n")
        f.write("=" * 50 + "\n\n")

        if len(issues_found) == 0:
            f.write("All checks passed — no issues found!\n")
        else:
            f.write(f"Total issues found: {len(issues_found)}\n\n")
            f.write("Issues:\n")
            for issue in issues_found:
                f.write(f"{issue}\n")

        f.write("\n" + "=" * 50 + "\n")
        f.write("  END OF REPORT\n")
        f.write("=" * 50 + "\n")

    print(f"\n Report saved to: {report_path}")
    return report_path



if __name__ == "__main__":
    engine = get_engine()

    customers, products, orders, order_items = load_staging_tables(engine)

    check_nulls(customers, products, orders, order_items)
    check_duplicates(customers, products, orders, order_items)
    check_invalid_values(products, orders, order_items)
    check_dates(orders)

    print("=" * 50)
    print("  QUALITY CHECK SUMMARY")
    print("=" * 50)
    if len(issues_found) == 0:
        print("All checks passed — no issues found!")
    else:
        print(f"Total issues found: {len(issues_found)}")
        for issue in issues_found:
            print(issue)

    save_report()

    print("\nQuality checks complete!")