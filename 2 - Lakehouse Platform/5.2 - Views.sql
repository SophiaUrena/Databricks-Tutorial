-- Databricks notebook source
-- MAGIC %md
-- MAGIC ## New Notebook
-- MAGIC The notebook titled **5.2 - Views**, or creating a new one will start a new Spark session and let us observe how the views we've created behave.

-- COMMAND ----------

USE CATALOG test;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Show Tables
-- MAGIC The results should confirm that the table smartphones still exist as well as the stored view of Apple phones still exist in this new session. 
-- MAGIC
-- MAGIC However, the temporary view of the brand phones does not exist.

-- COMMAND ----------

SHOW TABLES;

-- COMMAND ----------

--The global_temp views should still exist
SHOW TABLES IN global_temp;

-- COMMAND ----------

SELECT * FROM global_temp.global_temp_view_latest_phones;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## View Persistence Summary
-- MAGIC
-- MAGIC | **View Type**         | **Lifetime**               | **Accessible Across Sessions** | **Scope**                 |
-- MAGIC |------------------------|----------------------------|----------------------------------|----------------------------|
-- MAGIC | **Stored View**        | Until manually dropped     | ✅ Yes                           | Database Schema            |
-- MAGIC | **Temporary View**     | Until session ends         | ❌ No                            | Current Notebook           |
-- MAGIC | **Global Temp View**   | Until cluster restarts     | ✅ Yes (same cluster only)       | `global_temp` schema       |
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ## Dropping Views

-- COMMAND ----------

DROP TABLE smartphones;

DROP VIEW view_apple_phones;
DROP VIEW global_temp.global_temp_view_latest_phones;
