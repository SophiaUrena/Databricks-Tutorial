-- Databricks notebook source
-- MAGIC %md
-- MAGIC ## 📄 Managed Tables
-- MAGIC
-- MAGIC ### 🔹 What is a Managed Table?
-- MAGIC
-- MAGIC A **Managed Table** is a table where:
-- MAGIC
-- MAGIC - Both **metadata** and **data files** are fully managed by the **Hive Metastore**.
-- MAGIC - Dropping the table deletes **both the metadata and the underlying data**.
-- MAGIC
-- MAGIC ---
-- MAGIC
-- MAGIC ### 🔹 Creating a Managed Table:
-- MAGIC
-- MAGIC In this example, we create a table named `managed_default`:
-- MAGIC
-- MAGIC ```sql
-- MAGIC CREATE TABLE managed_default (id INT, name STRING);
-- MAGIC ```
-- MAGIC
-- MAGIC - Notice there is no `LOCATION` keyword, meaning:
-- MAGIC    - The data will be stored in the **default Hive warehouse directory**.
-- MAGIC    - Hive Metastore handles the storage path automatically.

-- COMMAND ----------

USE CATALOG test;

CREATE TABLE managed_default
  (width INT, length INT, height INT);

INSERT INTO managed_default
VALUES (3 INT, 2 INT, 1 INT)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Exploring Metadata
-- MAGIC
-- MAGIC Use the following command to inspect table details:
-- MAGIC
-- MAGIC ```sql
-- MAGIC DESCRIBE EXTENDED managed_default;
-- MAGIC ```
-- MAGIC
-- MAGIC - This shows:
-- MAGIC    - **Table name**: `managed_default`
-- MAGIC    - **Table Location**: Stored in the default Hive path (Location may show blank, storage is abstracted)
-- MAGIC    - **Table Type**: Managed

-- COMMAND ----------

DESCRIBE EXTENDED managed_default

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ## 📄 External Tables
-- MAGIC
-- MAGIC ### 🔹 What is an External Table?
-- MAGIC
-- MAGIC An **External Table** is a table where:
-- MAGIC
-- MAGIC - Only the **metadata** is managed by the **Hive Metastore**.
-- MAGIC - The **actual data files** are stored **outside** of Hive's default directory and control.
-- MAGIC - Dropping the table removes **only the metadata**, but **does NOT delete** the underlying data files.
-- MAGIC
-- MAGIC ---
-- MAGIC
-- MAGIC ### 🔹 Creating an External Table:
-- MAGIC
-- MAGIC To create an external table, you must explicitly define the storage path using the `LOCATION` keyword.
-- MAGIC
-- MAGIC Example:
-- MAGIC
-- MAGIC ```sql
-- MAGIC CREATE EXTERNAL TABLE external_table (id INT, name STRING)
-- MAGIC LOCATION 'your location';
-- MAGIC ```
-- MAGIC
-- MAGIC - The data filters for external_table will be stored in the '/mnt/demo' directory.
-- MAGIC - Hive Metastore records only the metadata and the tabl'es storage location.

-- COMMAND ----------

CREATE EXTERNAL LOCATION my_ext_loc
URL 's3://your-bucket/data/'
WITH STORAGE CREDENTIAL my_credential;

CREATE TABLE my_catalog.my_schema.external_default
  (width INT, length INT, height INT)
LOCATION '@my_ext_loc/external_default';
  
INSERT INTO external_default
VALUES (3 INT, 2 INT, 1 INT)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC #### Verifying Table Creation:
-- MAGIC
-- MAGIC - You can confirm successful creation in the Data Explorer.
-- MAGIC - Running the following command inspects table metadata:
-- MAGIC
-- MAGIC ```sql
-- MAGIC DESCRIBE EXTENDED external_table;
-- MAGIC ```
-- MAGIC
-- MAGIC - This shows:
-- MAGIC    - **Table Type**: External
-- MAGIC    - **Table Location**: Shows the path `/mnt/demo/` defined during creation

-- COMMAND ----------

DESCRIBE EXTENDED external_default

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ## 🗑️ Dropping Tables
-- MAGIC
-- MAGIC ### 🔹 Dropping Managed vs External Tables:
-- MAGIC
-- MAGIC ---
-- MAGIC
-- MAGIC ### 1️⃣ Dropping a Managed Table:
-- MAGIC
-- MAGIC ```sql
-- MAGIC DROP TABLE managed_default;
-- MAGIC ```
-- MAGIC
-- MAGIC - Outcome:
-- MAGIC   - Both **metadata** and the **underlying data files** are **permanently deleted**.
-- MAGIC   - Managed tables are fully controlled by the Hive Metastore, so deletion removes everything.
-- MAGIC ---
-- MAGIC
-- MAGIC ### 2️⃣ Dropping an External Table:
-- MAGIC
-- MAGIC ```sql
-- MAGIC DROP TABLE external_table;
-- MAGIC ```
-- MAGIC
-- MAGIC - Outcome:
-- MAGIC   - **Only the metadata is removed** from the Hive Metastore.
-- MAGIC   - **Data files (e.g., stored at `/mnt/demo`) remain intact**.
-- MAGIC   - You retain control over the physical data files.
-- MAGIC

-- COMMAND ----------

DROP TABLE managed_default

-- COMMAND ----------

DROP TABLE external_default

-- COMMAND ----------

-- MAGIC %fs ls 'dbfs:/mnt/demo/external_default'

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 📂 Creating Schemas
-- MAGIC
-- MAGIC ### 🔹 Schema Creation:
-- MAGIC
-- MAGIC You can create new schemas (also referred to as databases) in Databricks using either syntax:
-- MAGIC
-- MAGIC ```sql
-- MAGIC CREATE DATABASE sales_schema;
-- MAGIC CREATE SCHEMA marketing_schema;
-- MAGIC ```
-- MAGIC
-- MAGIC If the location is available the file will have .db for database/schemas

-- COMMAND ----------

CREATE SCHEMA new_default

-- COMMAND ----------

DESCRIBE DATABASE EXTENDED new_default

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Adding Tables to the New Schema:
-- MAGIC Once your schema is created you can add managed and external tables inside it
-- MAGIC

-- COMMAND ----------

USE new_default;

CREATE TABLE managed_new_default
  (width INT, length INT, height INT);
  
INSERT INTO managed_new_default
VALUES (3 INT, 2 INT, 1 INT);

-----------------------------------

CREATE TABLE external_new_default
  (width INT, length INT, height INT)
LOCATION 'dbfs:/mnt/demo/external_new_default';
  
INSERT INTO external_new_default
VALUES (3 INT, 2 INT, 1 INT);

-- COMMAND ----------

DESCRIBE EXTENDED managed_new_default

-- COMMAND ----------

DESCRIBE EXTENDED external_new_default

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Dropping Tables in Custom Schemas:
-- MAGIC Dropping tables in custom schemas will have the same outcomes as described previously.
-- MAGIC
-- MAGIC - Managed Table:
-- MAGIC   - **Outcome**: Both metadata and data files are deleted.
-- MAGIC - External Table:
-- MAGIC   - **Outcome**: Metadata is delted, but underyling data files remain.

-- COMMAND ----------

DROP TABLE managed_new_default;
DROP TABLE external_new_default;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 📂 Creating Schemas in Custom Location
-- MAGIC
-- MAGIC ### 🔹 Creating Schemas in Custom Locations:
-- MAGIC
-- MAGIC You can define a custom storage path when creating a new schema using the `LOCATION` keyword.
-- MAGIC
-- MAGIC **Example:**
-- MAGIC
-- MAGIC ```sql
-- MAGIC CREATE SCHEMA new_schema
-- MAGIC LOCATION '/mnt/custom/path';
-- MAGIC ```
-- MAGIC - The schema metadata is stored in the Hive Metastore.
-- MAGIC - The actual data files for managed tables will be stored at the specified custom location.
-- MAGIC ---
-- MAGIC ### 🔹 Behavior Consistency:
-- MAGIC - **Managed Tables**: Data files are stored inside the custom schema's defined location.
-- MAGIC - **External Tables**: You can still specify a separate `LOCATION` for each table as needed.
-- MAGIC
-- MAGIC The core behaviors remain the same:
-- MAGIC - Dropping a **managed table** deletes both metadata and data files.
-- MAGIC - Dropping an **external table** deletes only metadata, leaving the data files intact.

-- COMMAND ----------

CREATE SCHEMA custom
LOCATION 'dbfs:/Shared/schemas/custom.db'

-- COMMAND ----------

DESCRIBE DATABASE EXTENDED custom

-- COMMAND ----------

USE custom;

CREATE TABLE managed_custom
  (width INT, length INT, height INT);
  
INSERT INTO managed_custom
VALUES (3 INT, 2 INT, 1 INT);

-----------------------------------

CREATE TABLE external_custom
  (width INT, length INT, height INT)
LOCATION 'dbfs:/mnt/demo/external_custom';
  
INSERT INTO external_custom
VALUES (3 INT, 2 INT, 1 INT);

-- COMMAND ----------

DESCRIBE EXTENDED managed_custom

-- COMMAND ----------

DESCRIBE EXTENDED external_custom

-- COMMAND ----------

DROP TABLE managed_custom;
DROP TABLE external_custom;
