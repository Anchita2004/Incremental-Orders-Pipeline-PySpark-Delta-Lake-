# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # 01 - Simulate daily order files arriving
# MAGIC Each call to `generate_day(n)` writes ONE csv file, like a real system dropping a new file every day.
# MAGIC The files are deliberately messy: duplicates, bad rows, and updates to yesterday's orders.

# COMMAND ----------

# Create a schema (like a folder for tables) and a volume (a folder for raw files)
spark.sql("CREATE SCHEMA IF NOT EXISTS workspace.orders_pipeline")
spark.sql("CREATE VOLUME IF NOT EXISTS workspace.orders_pipeline.landing")

# COMMAND ----------

import random
from datetime import date, timedelta
import pandas as pd

BASE = "/Volumes/workspace/orders_pipeline/landing"
START = date(2026, 10, 1)
PRODUCTS = ["Laptop", "Phone", "Headphones", "Keyboard", "Mouse", "Monitor"]
COLS = ["order_id", "customer_id", "product", "amount", "status", "order_date", "updated_at"]


def base_order(day, i):
    """Same (day, i) always gives the same order, so updates match the original."""
    r = random.Random(day * 1000 + i)
    d = START + timedelta(days=day)
    return {
        "order_id": day * 1000 + i,
        "customer_id": r.randint(1, 500),
        "product": r.choice(PRODUCTS),
        "amount": round(r.uniform(5, 500), 2),
        "status": "placed",
        "order_date": d.isoformat(),
        "updated_at": f"{d.isoformat()} 09:00:00",
    }


def generate_day(day):
    r = random.Random(day)
    d = START + timedelta(days=day)

    # 200 brand-new orders for this day
    rows = [base_order(day, i) for i in range(200)]

    # 30 orders from the previous day change status (these must UPDATE, not duplicate)
    if day > 0:
        for i in r.sample(range(200), 30):
            o = base_order(day - 1, i)
            o["status"] = r.choice(["shipped", "delivered", "cancelled"])
            o["updated_at"] = f"{d.isoformat()} 15:00:00"
            rows.append(o)

    # 5 exact duplicate rows (source systems often send repeats)
    rows += [dict(x) for x in r.sample(rows, 5)]

    # 3 invalid rows the pipeline must reject
    bad = [base_order(day, 900 + k) for k in range(3)]
    bad[0]["customer_id"] = None      # missing customer
    bad[1]["amount"] = -50.0          # negative amount
    bad[2]["status"] = "unknown"      # invalid status
    rows += bad

    df = pd.DataFrame(rows, columns=COLS)
    df["customer_id"] = df["customer_id"].astype("Int64")  # keeps ids as whole numbers
    path = f"{BASE}/orders_{d.isoformat()}.csv"
    df.to_csv(path, index=False)
    return path

# COMMAND ----------

# Day 0 = first file. Run the pipeline notebook, then come back and run generate_day(1), and so on.
print(generate_day(0))

# COMMAND ----------

print(generate_day(1))

# COMMAND ----------

print(generate_day(2))

# COMMAND ----------

