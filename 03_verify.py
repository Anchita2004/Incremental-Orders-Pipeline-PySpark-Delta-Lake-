# Databricks notebook source
# MAGIC %sql
# MAGIC SELECT COUNT(*) AS total_orders FROM workspace.orders_pipeline.orders

# COMMAND ----------

# MAGIC %sql
# MAGIC -- must return ZERO rows
# MAGIC SELECT order_id, COUNT(*) AS n
# MAGIC FROM workspace.orders_pipeline.orders
# MAGIC GROUP BY order_id HAVING COUNT(*) > 1

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT status, COUNT(*) AS n
# MAGIC FROM workspace.orders_pipeline.orders GROUP BY status ORDER BY n DESC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM workspace.orders_pipeline.processed_files ORDER BY processed_at

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM workspace.orders_pipeline.rejected_orders ORDER BY source_file

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY workspace.orders_pipeline.orders

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT order_date, ROUND(SUM(amount), 2) AS revenue, COUNT(*) AS orders
# MAGIC FROM workspace.orders_pipeline.orders
# MAGIC WHERE status <> 'cancelled'
# MAGIC GROUP BY order_date ORDER BY order_date

# COMMAND ----------

