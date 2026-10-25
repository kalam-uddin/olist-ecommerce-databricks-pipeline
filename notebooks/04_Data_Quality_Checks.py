# Databricks notebook source
# DBTITLE 1,Checking Table availability and Row counts

from pyspark.sql import functions as F
from functools import reduce

CATALOG = "olist-project"
SCHEMA = "olist"
BASE = f"`{CATALOG}`.{SCHEMA}"

tables_to_check = [
    "silver_orders",
    "silver_order_items",
    "silver_order_payments",
    "silver_customers",
    "silver_products",
    "silver_sellers",
    "silver_category_translation",
    "silver_geolocation",
    "gold_order_summary",
    "gold_delivery_performance",
    "gold_product_performance",
    "gold_category_performance",
    "gold_seller_performance",
    "gold_customer_performance"
]

for table_name in tables_to_check:
    df = spark.table(f"{BASE}.{table_name}")
    print(f"{table_name}: {df.count():,} rows")


# COMMAND ----------

# DBTITLE 1,Check primary keys and duplicate records

key_checks = {
    "silver_orders": ["order_id"],
    "silver_order_items": ["order_id", "order_item_id"],
    "silver_order_payments": ["order_id", "payment_sequential"],
    "silver_customers": ["customer_id"],
    "silver_products": ["product_id"],
    "silver_sellers": ["seller_id"],
    "silver_category_translation": ["product_category_name"]
}

key_results = []

for table_name, key_cols in key_checks.items():
    df = spark.table(f"{BASE}.{table_name}")

    null_condition = reduce(
        lambda a, b: a | b,
        [F.col(c).isNull() for c in key_cols]
    )

    null_count = df.filter(null_condition).count()

    duplicate_count = (
        df.groupBy(*key_cols)
        .count()
        .filter(F.col("count") > 1)
        .count()
    )

    key_results.append((
        table_name,
        ",".join(key_cols),
        null_count,
        duplicate_count
    ))

key_results_df = spark.createDataFrame(
    key_results,
    ["table_name", "key_columns", "null_key_rows", "duplicate_key_groups"]
)

display(key_results_df)


# COMMAND ----------

# DBTITLE 1,Checking invalid business values

items = spark.table(f"{BASE}.silver_order_items")
delivery = spark.table(f"{BASE}.gold_delivery_performance")

print("Negative item prices:", items.filter(F.col("price") < 0).count())
print("Negative freight values:", items.filter(F.col("freight_value") < 0).count())

print(
    "Negative delivery durations:",
    delivery.filter(F.col("delivery_days") < 0).count()
)


# COMMAND ----------

# DBTITLE 1,Checking referential integrity

orders = spark.table(f"{BASE}.silver_orders").select("order_id")
products = spark.table(f"{BASE}.silver_products").select("product_id")
sellers = spark.table(f"{BASE}.silver_sellers").select("seller_id")

items = spark.table(f"{BASE}.silver_order_items")
payments = spark.table(f"{BASE}.silver_order_payments")

orphan_checks = {
    "items_missing_order": items.join(orders, "order_id", "left_anti").count(),
    "items_missing_product": items.join(products, "product_id", "left_anti").count(),
    "items_missing_seller": items.join(sellers, "seller_id", "left_anti").count(),
    "payments_missing_order": payments.join(orders, "order_id", "left_anti").count()
}

for check_name, count in orphan_checks.items():
    print(f"{check_name}: {count}")


# COMMAND ----------

# DBTITLE 1,Reconciling Gold revenue against Silver

silver_items_total = (
    spark.table(f"{BASE}.silver_order_items")
    .agg(F.sum("price").alias("total"))
    .first()["total"]
)

gold_products_total = (
    spark.table(f"{BASE}.gold_product_performance")
    .agg(F.sum("total_revenue").alias("total"))
    .first()["total"]
)

gold_sellers_total = (
    spark.table(f"{BASE}.gold_seller_performance")
    .agg(F.sum("total_revenue").alias("total"))
    .first()["total"]
)

gold_orders_total = (
    spark.table(f"{BASE}.gold_order_summary")
    .agg(F.sum("items_revenue").alias("total"))
    .first()["total"]
)

print("Silver item revenue:", silver_items_total)
print("Gold product revenue:", gold_products_total)
print("Gold seller revenue:", gold_sellers_total)
print("Gold order summary revenue:", gold_orders_total)


# COMMAND ----------

# DBTITLE 1,Creating a quality audit table

audit_rows = []

for row in key_results:
    table_name, key_columns, null_count, duplicate_count = row

    audit_rows.append((
        table_name,
        "null_key_rows",
        int(null_count),
        "PASS" if null_count == 0 else "FAIL"
    ))

    audit_rows.append((
        table_name,
        "duplicate_key_groups",
        int(duplicate_count),
        "PASS" if duplicate_count == 0 else "FAIL"
    ))

audit_df = (
    spark.createDataFrame(
        audit_rows,
        ["table_name", "check_name", "failed_count", "status"]
    )
    .withColumn("checked_at", F.current_timestamp())
)

(
    audit_df.write
    .format("delta")
    .mode("append")
    .saveAsTable(f"{BASE}.gold_quality_audit")
)

display(audit_df)


# COMMAND ----------

