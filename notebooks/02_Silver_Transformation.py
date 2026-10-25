# Databricks notebook source
# DBTITLE 1,Configuration and helper functions
from pyspark.sql import functions as F
from pyspark.sql import types as T

CATALOG = "olist-project"
SCHEMA = "olist"

BASE = f"`{CATALOG}`.{SCHEMA}"

def read_bronze(table_name):
    return spark.table(f"{BASE}.bronze_{table_name}")

def write_silver(df, table_name):
    target = f"{BASE}.silver_{table_name}"

    (
        df.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(target)
    )

    return target

def cast_if_exists(df, column_name, data_type):
    if column_name in df.columns:
        return df.withColumn(
            column_name,
            F.col(column_name).cast(data_type)
        )
    return df

def timestamp_if_exists(df, column_name):
    if column_name in df.columns:
        return df.withColumn(
            column_name,
            F.to_timestamp(F.col(column_name))
        )
    return df

print("Silver transformation setup complete.")


# COMMAND ----------

# DBTITLE 1,Cleaning Orders Table

orders = read_bronze("orders")

orders = orders.dropDuplicates()

order_timestamp_columns = [
    "order_purchase_timestamp",
    "order_approved_at",
    "order_delivered_carrier_date",
    "order_delivered_customer_date",
    "order_estimated_delivery_date"
]

for column_name in order_timestamp_columns:
    orders = timestamp_if_exists(orders, column_name)

orders = orders.withColumn(
    "order_status",
    F.lower(F.trim(F.col("order_status")))
)

orders = orders.withColumn(
    "_silver_processed_at",
    F.current_timestamp()
)

write_silver(orders, "orders")

display(orders.limit(10))


# COMMAND ----------

# DBTITLE 1,Cleaning Customers and Sellers Tables

customers = read_bronze("customers").dropDuplicates()

for column_name in customers.columns:
    if customers.schema[column_name].dataType == T.StringType():
        customers = customers.withColumn(
            column_name,
            F.trim(F.col(column_name))
        )

customers = customers.withColumn(
    "_silver_processed_at", F.current_timestamp()
)

write_silver(customers, "customers")


sellers = read_bronze("sellers").dropDuplicates()

for column_name in sellers.columns:
    if sellers.schema[column_name].dataType == T.StringType():
        sellers = sellers.withColumn(
            column_name,
            F.trim(F.col(column_name))
        )

sellers = sellers.withColumn(
    "_silver_processed_at", F.current_timestamp()
)

write_silver(sellers, "sellers")

display(customers.limit(5))
display(sellers.limit(5))


# COMMAND ----------

# DBTITLE 1,Clening Order Items and Payments tables

# Order items
items = read_bronze("order_items").dropDuplicates()

for column_name in ["price", "freight_value"]:
    items = cast_if_exists(items, column_name, T.DecimalType(12, 2))

items = timestamp_if_exists(items, "shipping_limit_date")

items = items.withColumn(
    "_silver_processed_at", F.current_timestamp()
)

write_silver(items, "order_items")


# Order payments
payments = read_bronze("order_payments").dropDuplicates()

payments = cast_if_exists(
    payments, "payment_sequential", T.IntegerType()
)
payments = cast_if_exists(
    payments, "payment_installments", T.IntegerType()
)
payments = cast_if_exists(
    payments, "payment_value", T.DecimalType(12, 2)
)

if "payment_type" in payments.columns:
    payments = payments.withColumn(
        "payment_type",
        F.lower(F.trim(F.col("payment_type")))
    )

payments = payments.withColumn(
    "_silver_processed_at", F.current_timestamp()
)

write_silver(payments, "order_payments")

display(items.limit(10))
display(payments.limit(10))


# COMMAND ----------

# DBTITLE 1,Cleaning Products and Translation tables

# Products
products = read_bronze("products").dropDuplicates()

product_integer_columns = [
    "product_name_lenght",
    "product_description_lenght",
    "product_photos_qty",
    "product_weight_g",
    "product_length_cm",
    "product_height_cm",
    "product_width_cm"
]

for column_name in product_integer_columns:
    products = cast_if_exists(
        products, column_name, T.IntegerType()
    )

if "product_category_name" in products.columns:
    products = products.withColumn(
        "product_category_name",
        F.lower(F.trim(F.col("product_category_name")))
    )

# Category translations
categories = read_bronze("product_category_name_translation").dropDuplicates()

for column_name in categories.columns:
    categories = categories.withColumn(
        column_name,
        F.trim(F.col(column_name))
    )

categories = categories.withColumn(
    "_silver_processed_at", F.current_timestamp()
)

write_silver(categories, "category_translation")

# Join products with category translation to add English category names
# (products without a match keep a null product_category_english)
# categories_english = categories.select(
#     "product_category_name",
#     F.col("product_category_name_english").alias("product_category_english")
# )
categories_english = categories.select(
    "product_category_name",
    F.initcap(
        F.regexp_replace(F.col("product_category_name_english"), "_", " ")
    ).alias("product_category_english")
)

products = products.join(
    categories_english,
    on="product_category_name",
    how="left"
)

products = products.withColumn(
    "_silver_processed_at", F.current_timestamp()
)

write_silver(products, "products")

display(products.limit(10))
display(categories.limit(10))


# COMMAND ----------

# DBTITLE 1,Cleaning Geolocation table

geolocation = read_bronze("geolocation").dropDuplicates()

geolocation = cast_if_exists(
    geolocation,
    "geolocation_zip_code_prefix",
    T.IntegerType()
)
geolocation = cast_if_exists(
    geolocation,
    "geolocation_lat",
    T.DoubleType()
)
geolocation = cast_if_exists(
    geolocation,
    "geolocation_lng",
    T.DoubleType()
)

for column_name in [
    "geolocation_city",
    "geolocation_state"
]:
    if column_name in geolocation.columns:
        geolocation = geolocation.withColumn(
            column_name,
            F.lower(F.trim(F.col(column_name)))
        )

geolocation = geolocation.withColumn(
    "_silver_processed_at", F.current_timestamp()
)

write_silver(geolocation, "geolocation")

display(geolocation.limit(10))


# COMMAND ----------

# DBTITLE 1,Data Quality Checks

KEYS = {
    "customers": ["customer_id"],
    "orders": ["order_id"],
    "order_items": ["order_id", "order_item_id"],
    "order_payments": ["order_id", "payment_sequential"],
    "products": ["product_id"],
    "sellers": ["seller_id"],
    "category_translation": ["product_category_name"],
}

quality_results = []

for table_name, key_columns in KEYS.items():
    table = f"{BASE}.silver_{table_name}"
    df = spark.table(table)

    missing_columns = [
        c for c in key_columns if c not in df.columns
    ]

    if missing_columns:
        quality_results.append({
            "table_name": table_name,
            "row_count": df.count(),
            "null_key_rows": None,
            "duplicate_key_groups": None,
            "status": "SCHEMA_ERROR",
            "details": f"Missing key columns: {missing_columns}"
        })
        continue

    null_condition = F.lit(False)

    for column_name in key_columns:
        null_condition = null_condition | F.col(
            column_name
        ).isNull()

    null_key_rows = df.filter(null_condition).count()

    duplicate_key_groups = (
        df.groupBy(*key_columns)
        .count()
        .filter(F.col("count") > 1)
        .count()
    )

    row_count = df.count()

    status = (
        "PASS"
        if null_key_rows == 0 and duplicate_key_groups == 0
        else "CHECK"
    )

    quality_results.append({
        "table_name": table_name,
        "row_count": row_count,
        "null_key_rows": null_key_rows,
        "duplicate_key_groups": duplicate_key_groups,
        "status": status,
        "details": None
    })

quality_schema = T.StructType([
    T.StructField("table_name", T.StringType(), False),
    T.StructField("row_count", T.LongType(), True),
    T.StructField("null_key_rows", T.LongType(), True),
    T.StructField("duplicate_key_groups", T.LongType(), True),
    T.StructField("status", T.StringType(), False),
    T.StructField("details", T.StringType(), True),
])

quality_df = spark.createDataFrame(
    quality_results,
    schema=quality_schema
)

display(quality_df.orderBy("table_name"))


# COMMAND ----------

# DBTITLE 1,Saving the Quality Report

(
    quality_df
    .withColumn(
        "checked_at",
        F.current_timestamp()
    )
    .write
    .format("delta")
    .mode("append")
    .saveAsTable(f"{BASE}.silver_quality_audit")
)

print("Silver transformation completed.")
