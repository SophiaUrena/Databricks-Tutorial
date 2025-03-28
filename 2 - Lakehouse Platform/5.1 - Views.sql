-- Databricks notebook source
-- MAGIC %md
-- MAGIC ## Inserting Data into the Table

-- COMMAND ----------

USE CATALOG test;

CREATE TABLE IF NOT EXISTS smartphones
(id INT, name STRING, brand STRING, year INT);

INSERT INTO smartphones
VALUES (1, 'iPhone 14', 'Apple', 2022),
      (2, 'iPhone 13', 'Apple', 2021),
      (3, 'iPhone 6', 'Apple', 2014),
      (4, 'iPad Air', 'Apple', 2013),
      (5, 'Galaxy S22', 'Samsung', 2022),
      (6, 'Galaxy Z Fold', 'Samsung', 2022),
      (7, 'Galaxy S9', 'Samsung', 2016),
      (8, '12 Pro', 'Xiaomi', 2022),
      (9, 'Redmi 11T Pro', 'Xiaomi', 2022),
      (10, 'Redmi Note 11', 'Xiaomi', 2021)

-- COMMAND ----------

SHOW TABLES

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Creating a Stored View
-- MAGIC
-- MAGIC Definition:
-- MAGIC A stored view is a virtual table saved in the database. It runs the same SQL query every time it's accessed.

-- COMMAND ----------

CREATE VIEW view_apple_phones
AS  SELECT * 
    FROM smartphones 
    WHERE brand = 'Apple';

-- COMMAND ----------

SELECT * FROM view_apple_phones;

-- COMMAND ----------

SHOW TABLES;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ## Creating a Temporary View
-- MAGIC
-- MAGIC Definition:
-- MAGIC A temporary view exists only for the duration of the current Spark session.

-- COMMAND ----------

CREATE TEMP VIEW temp_view_phones_brands
AS  SELECT DISTINCT brand
    FROM smartphones;

SELECT * FROM temp_view_phones_brands;

-- COMMAND ----------

SHOW TABLES;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ## Creating a Global Temporary View
-- MAGIC
-- MAGIC Definition:
-- MAGIC A global temporary view lasts for the duration of the cluster and can be accessed across notebooks and sessions.

-- COMMAND ----------

--This view is not supported on serverless compute
CREATE GLOBAL TEMP VIEW global_temp_view_latest_phones
AS SELECT * FROM smartphones
    WHERE year > 2020
    ORDER BY year DESC;

-- COMMAND ----------

SELECT * FROM global_temp.global_temp_view_latest_phones;

-- COMMAND ----------

SHOW TABLES;

-- COMMAND ----------

SHOW TABLES IN global_temp;

-- COMMAND ----------

SHOW TABLES

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ## New Notebook
-- MAGIC Open the notebook titled **5.2 - Views**, or create a new one. This will start a new Spark session and let us observe how the views we've created behave.

-- COMMAND ----------

--This shows that the views are still active
SHOW TABLES;

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
