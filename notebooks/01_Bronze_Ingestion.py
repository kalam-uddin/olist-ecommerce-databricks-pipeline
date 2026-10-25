# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS `olist-project`.olist;
# MAGIC
# MAGIC CREATE VOLUME IF NOT EXISTS `olist-project`.olist.raw_files;

# COMMAND ----------

from pyspark.sql import functions as F
from datetime import datetime, timezone

SOURCE_PATH = "/Volumes/olist-project/olist/raw_files"
CATALOG = "olist-project"
SCHEMA = "olist"

BRONZE_SCHEMA = f"`{CATALOG}`.`{SCHEMA}`"

print("Spark version:", spark.version)
print("Source directory:", SOURCE_PATH)

display(dbutils.fs.ls(SOURCE_PATH))


# COMMAND ----------


raw_dataset = {
    "customers": "olist_customers_dataset.csv",
    "geolocation": "olist_geolocation_dataset.csv",
    "order_items": "olist_order_items_dataset.csv",
    "order_payments": "olist_order_payments_dataset.csv",
    # "order_reviews": "olist_order_reviews_dataset.csv",
    "orders": "olist_orders_dataset.csv",
    "products": "olist_products_dataset.csv",
    "sellers": "olist_sellers_dataset.csv",
    "product_category_name_translation": "product_category_name_translation.csv",
}

print(f"Datasets configured: {len(raw_dataset)}")


# COMMAND ----------


ingestion_results = []
failed_datasets = []

for table_name, file_name in raw_dataset.items():
    source_file = f"{SOURCE_PATH}/{table_name}"
    target_table = f"{BRONZE_SCHEMA}.bronze_{table_name}"

    print(f"Starting ingestion: {file_name}")

    try:
        # Verify that the source file exists
        file_info = dbutils.fs.ls(source_file)
        available_files = {item.name for item in file_info}

        if file_name not in available_files:
            raise FileNotFoundError(
                f"Source file not found: {source_file}"
            )

        # Read CSV
        df = (
            spark.read
            .format("csv")
            .option("header", "true")
            .option("inferSchema", "true")
            .option("mode", "FAILFAST")
            .load(source_file)
        )

        # Add ingestion metadata
        df = df.withColumns({
            "_ingested_at": F.current_timestamp(),
            "_source_file": F.lit(file_name),
        })

        # Write Bronze Delta table
        (
            df.write
            .format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "true")
            .saveAsTable(target_table)
        )

        # Count records written
        row_count = spark.table(target_table).count()

        ingestion_results.append({
            "dataset": table_name,
            "source_file": file_name,
            "target_table": target_table,
            "status": "SUCCESS",
            "row_count": row_count,
            "error": None
        })

        print(
            f"SUCCESS: {target_table} | "
            f"Rows: {row_count}"
        )

    except Exception as e:
        failed_datasets.append(table_name)

        ingestion_results.append({
            "dataset": table_name,
            "source_file": file_name,
            "target_table": target_table,
            "status": "FAILED",
            "row_count": None,
            "error": str(e)[:1000]
        })

        print(f"FAILED: {table_name}: {e}")


# COMMAND ----------


from pyspark.sql.types import StructType, StructField, StringType, LongType

schema = StructType([
    StructField("dataset", StringType(), True),
    StructField("source_file", StringType(), True),
    StructField("target_table", StringType(), True),
    StructField("status", StringType(), True),
    StructField("row_count", LongType(), True),
    StructField("error", StringType(), True),
])

results_df = spark.createDataFrame(ingestion_results, schema)

display(
    results_df.orderBy("dataset")
)


# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM `olist-project`.olist.bronze_products limit 10;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM `olist-project`.olist.bronze_product_category_name_translation limit 10;

# COMMAND ----------

