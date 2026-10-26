# Databricks notebook source
import os
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from delta.tables import DeltaTable

S = "workspace.orders_pipeline"
LANDING = "/Volumes/workspace/orders_pipeline/landing"
ORDERS = f"{S}.orders"
REJECTED = f"{S}.rejected_orders"
LOG = f"{S}.processed_files"

SCHEMA = ("order_id LONG, customer_id LONG, product STRING, amount DOUBLE, "
          "status STRING, order_date DATE, updated_at TIMESTAMP")

spark.sql(f"""CREATE TABLE IF NOT EXISTS {LOG} (
    file_name STRING, processed_at TIMESTAMP,
    rows_read BIGINT, rows_loaded BIGINT, rows_rejected BIGINT)""")

# COMMAND ----------

all_files = sorted(os.path.join(LANDING, f) for f in os.listdir(LANDING) if f.endswith(".csv"))
done = {r.file_name for r in spark.table(LOG).collect()}
new_files = [f for f in all_files if os.path.basename(f) not in done]
print(f"{len(all_files)} files in landing, {len(new_files)} new: {[os.path.basename(f) for f in new_files]}")

# COMMAND ----------

def is_valid():
    cond = (
        F.col("order_id").isNotNull()
        & F.col("customer_id").isNotNull()
        & (F.col("amount") > 0)
        & F.col("status").isin("placed", "shipped", "delivered", "cancelled")
        & F.col("order_date").isNotNull()
        & F.col("updated_at").isNotNull()
    )
    return F.coalesce(cond, F.lit(False))


def process_file(path):
    name = os.path.basename(path)
    raw = spark.read.option("header", True).schema(SCHEMA).csv(path)
    n_read = raw.count()

    valid = raw.filter(is_valid())
    rejected = raw.filter(~is_valid()).withColumn("source_file", F.lit(name))
    n_rejected = rejected.count()
    rejected.write.format("delta").mode("append").saveAsTable(REJECTED)

    w = Window.partitionBy("order_id").orderBy(F.col("updated_at").desc())
    deduped = (valid.withColumn("rn", F.row_number().over(w))
                    .filter("rn = 1").drop("rn"))
    n_loaded = deduped.count()

    if not spark.catalog.tableExists(ORDERS):
        deduped.write.format("delta").saveAsTable(ORDERS)
    else:
        (DeltaTable.forName(spark, ORDERS).alias("t")
            .merge(deduped.alias("s"), "t.order_id = s.order_id")
            .whenMatchedUpdateAll(condition="s.updated_at > t.updated_at")
            .whenNotMatchedInsertAll()
            .execute())

    spark.sql(f"INSERT INTO {LOG} VALUES ('{name}', current_timestamp(), "
              f"{n_read}, {n_loaded}, {n_rejected})")
    print(f"OK  {name}: read={n_read} loaded={n_loaded} rejected={n_rejected}")

# COMMAND ----------

for f in new_files:
    try:
        process_file(f)
    except Exception as e:
        print(f"FAILED {os.path.basename(f)}: {e}")
        raise

print("Pipeline run finished.")

# COMMAND ----------

