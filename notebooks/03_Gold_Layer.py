# Databricks notebook source
# DBTITLE 1,Reading Silver Tables

from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType

CATALOG = "olist-project"
SCHEMA = "olist"

BASE = f"`{CATALOG}`.{SCHEMA}"

def read_silver(table_name):
    return spark.table(f"{BASE}.silver_{table_name}")


orders = read_silver("orders")
items = read_silver("order_items")
payments = read_silver("order_payments")
customers = read_silver("customers")
products = read_silver("products")
sellers = read_silver("sellers")

# orders = spark.table(f"`{CATALOG_SCHEMA}`.silver_orders")
# items = spark.table(f"`{CATALOG_SCHEMA}`.silver_order_items")
# payments = spark.table(f"`{CATALOG_SCHEMA}`.silver_order_payments")
# customers = spark.table(f"`{CATALOG_SCHEMA}`.silver_customers")
# products = spark.table(f"`{CATALOG_SCHEMA}`.silver_products")
# sellers = spark.table(f"{CATALOG_SCHEMA}.silver_sellers")
# category_translation = spark.table(
#     f"{CATALOG_SCHEMA}.silver_category_translation"
# )

print("Silver tables loaded successfully.")

# COMMAND ----------

# DBTITLE 1,Building Reusable Aggregated DFs

# Aggregate order items: one row per order
order_items_agg = (
    items.groupBy("order_id")
    .agg(
        F.count("*").alias("item_count"),
        F.sum("price").alias("items_revenue"),
        F.sum("freight_value").alias("freight_total"),
        F.countDistinct("seller_id").alias("seller_count"),
        F.countDistinct("product_id").alias("distinct_product_count")
    )
)

# Aggregate payments: one row per order
order_payments_agg = (
    payments.groupBy("order_id")
    .agg(
        F.sum("payment_value").alias("payment_total"),
        F.count("*").alias("payment_record_count"),
        F.countDistinct("payment_type").alias("payment_type_count"),
        F.max("payment_installments").alias("max_installments")
    )
)


display(order_items_agg.limit(10))
display(order_payments_agg.limit(10))

# COMMAND ----------

# DBTITLE 1,Creating Gold Order Summary

gold_order_summary = (
    orders.alias("o")
    .join(order_items_agg.alias("i"), "order_id", "left")
    .join(order_payments_agg.alias("p"), "order_id", "left")
    .select(
        F.col("order_id"),
        F.col("customer_id"),
        F.col("order_status"),
        F.col("order_purchase_timestamp"),
        F.col("order_approved_at"),
        F.col("order_delivered_carrier_date"),
        F.col("order_delivered_customer_date"),
        F.col("order_estimated_delivery_date"),
        F.coalesce(F.col("item_count"), F.lit(0)).alias("item_count"),
        F.coalesce(F.col("items_revenue"), F.lit(0)).alias("items_revenue"),
        F.coalesce(F.col("freight_total"), F.lit(0)).alias("freight_total"),
        F.coalesce(F.col("seller_count"), F.lit(0)).alias("seller_count"),
        F.coalesce(
            F.col("distinct_product_count"), F.lit(0)
        ).alias("distinct_product_count"),
        F.col("payment_total"),
        F.col("payment_record_count"),
        F.col("payment_type_count"),
        F.col("max_installments")
    )
    .withColumn(
        "order_year",
        F.year("order_purchase_timestamp")
    )
    .withColumn(
        "order_month",
        F.date_trunc("month", "order_purchase_timestamp")
    )
    .withColumn(
        "items_plus_freight",
        F.coalesce(F.col("items_revenue"), F.lit(0))
        + F.coalesce(F.col("freight_total"), F.lit(0))
    )
    .withColumn("_gold_created_at", F.current_timestamp())
)

(
    gold_order_summary.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{BASE}.gold_order_summary")
)

display(gold_order_summary.limit(20))


# COMMAND ----------

# DBTITLE 1,Creating Gold Delivery Performance

gold_delivery_performance = (
    orders
    .filter(F.col("order_delivered_customer_date").isNotNull())
    .select(
        "order_id",
        "customer_id",
        "order_status",
        "order_purchase_timestamp",
        "order_delivered_customer_date",
        "order_estimated_delivery_date"
    )
    .withColumn(
        "delivery_days",
        F.datediff(
            F.to_date("order_delivered_customer_date"),
            F.to_date("order_purchase_timestamp")
        )
    )
    .withColumn(
        "days_vs_estimated",
        F.datediff(
            F.to_date("order_delivered_customer_date"),
            F.to_date("order_estimated_delivery_date")
        )
    )
    .withColumn(
        "delivery_outcome",
        F.when(
            F.col("order_estimated_delivery_date").isNull(),
            "Unknown"
        ).when(F.col("days_vs_estimated") > 0, "Late")
         .otherwise("On time or early")
    )
    .withColumn("_gold_created_at", F.current_timestamp())
)

(
    gold_delivery_performance.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{BASE}.gold_delivery_performance")
)

display(gold_delivery_performance.limit(20))


# COMMAND ----------

# DBTITLE 1,Creating Gold Products Performance

product_metrics = (
    items.groupBy("product_id")
    .agg(
        F.sum("price").alias("total_revenue"),
        F.sum("freight_value").alias("total_freight"),
        F.count("*").alias("units_sold"),
        F.countDistinct("order_id").alias("order_count"),
        F.countDistinct("seller_id").alias("seller_count")
    )
)

gold_product_performance = (
    products
    .join(product_metrics, "product_id", "left")
    # .join(category_translation, "product_category_name", "left")
    .select(
        "product_id",
        "product_category_name",
        "product_category_english",
        "product_name_lenght",
        "product_description_lenght",
        "product_photos_qty",
        "product_weight_g",
        "product_length_cm",
        "product_height_cm",
        "product_width_cm",
        "total_revenue",
        "total_freight",
        "units_sold",
        "order_count",
        "seller_count"
    )
    .withColumn("_gold_created_at", F.current_timestamp())
)

(
    gold_product_performance.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{BASE}.gold_product_performance")
)

display(gold_product_performance.orderBy(F.desc("total_revenue")).limit(20))


# COMMAND ----------

# DBTITLE 1,Creating Gold Category Performance

gold_category_performance = (
    gold_product_performance
    .groupBy(
        "product_category_name",
        "product_category_english"
    )
    .agg(
        F.sum("total_revenue").alias("total_revenue"),
        F.sum("total_freight").alias("total_freight"),
        F.sum("units_sold").alias("units_sold"),
        F.sum("order_count").alias("product_order_counts_sum"),
        F.countDistinct("product_id").alias("product_count"),
        F.avg("total_revenue").alias("avg_revenue_per_product")
    )
    .withColumn("_gold_created_at", F.current_timestamp())
)

(
    gold_category_performance.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{BASE}.gold_category_performance")
)

display(
    gold_category_performance
    .orderBy(F.desc("total_revenue"))
    .limit(20)
)


# COMMAND ----------

# DBTITLE 1,Creating Gold Seller Performance

seller_metrics = (
    items.groupBy("seller_id")
    .agg(
        F.sum("price").alias("total_revenue"),
        F.sum("freight_value").alias("total_freight"),
        F.count("*").alias("units_sold"),
        F.countDistinct("order_id").alias("order_count"),
        F.countDistinct("product_id").alias("distinct_products_sold")
    )
)

gold_seller_performance = (
    sellers
    .join(seller_metrics, "seller_id", "left")
    .withColumn("_gold_created_at", F.current_timestamp())
)

(
    gold_seller_performance.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{BASE}.gold_seller_performance")
)

display(gold_seller_performance.orderBy(F.desc("total_revenue")).limit(20))


# COMMAND ----------

# DBTITLE 1,Crating Gold Customer Performance

customer_orders = (
    gold_order_summary
    .select(
        "customer_id",
        "order_id",
        "order_status",
        "order_purchase_timestamp",
        "items_revenue",
        "payment_total"
    )
    .join(
        customers.select("customer_id", "customer_unique_id"),
        "customer_id",
        "left"
    )
)

gold_customer_performance = (
    customer_orders
    .groupBy("customer_unique_id")
    .agg(
        F.countDistinct("order_id").alias("order_count"),
        F.sum("items_revenue").alias("total_items_revenue"),
        F.sum("payment_total").alias("total_recorded_payments"),
        F.min("order_purchase_timestamp").alias("first_order_timestamp"),
        F.max("order_purchase_timestamp").alias("latest_order_timestamp"),
        F.countDistinct(
            F.when(F.col("order_status") == "delivered", F.col("order_id"))
        ).alias("delivered_order_count")
    )
    .withColumn("_gold_created_at", F.current_timestamp())
)

(
    gold_customer_performance.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{BASE}.gold_customer_performance")
)

display(
    gold_customer_performance
    .orderBy(F.desc("order_count"))
    .limit(20)
)


# COMMAND ----------

# DBTITLE 1,Validating the Gold Tables

gold_tables = [
    "gold_order_summary",
    "gold_delivery_performance",
    "gold_product_performance",
    "gold_category_performance",
    "gold_seller_performance",
    "gold_customer_performance"
]

for table_name in gold_tables:
    df = spark.table(f"{BASE}.{table_name}")

    print(f"\nTable: {table_name}")
    print(f"Row count: {df.count()}")
    print(f"Column count: {len(df.columns)}")

# Check whether the order summary has one row per order
order_summary = spark.table(f"{BASE}.gold_order_summary")

duplicate_orders = (
    order_summary.groupBy("order_id")
    .count()
    .filter(F.col("count") > 1)
    .count()
)

print(f"\nDuplicate order IDs in gold_order_summary: {duplicate_orders}")

# Review delivery outcomes
spark.table(f"{BASE}.gold_delivery_performance") \
    .groupBy("delivery_outcome") \
    .count() \
    .show()


# COMMAND ----------

