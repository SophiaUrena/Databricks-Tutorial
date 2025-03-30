-- Databricks notebook source
-- MAGIC %md-sandbox
-- MAGIC
-- MAGIC <div  style="text-align: center; line-height: 0; padding-top: 9px;">
-- MAGIC   <img src="https://raw.githubusercontent.com/derar-alhussein/Databricks-Certified-Data-Engineer-Associate/main/Includes/images/bookstore_schema.png" alt="Databricks Learning" style="width: 600">
-- MAGIC </div>

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Querying JSON 
-- MAGIC
-- MAGIC JSON is a self-describing format, meaning it includes schema information within the file itself.  
-- MAGIC This makes it ideal for querying directly with Spark SQL, as the schema can be automatically inferred.  
-- MAGIC
-- MAGIC - Use backticks (`) around file paths to avoid syntax errors.
-- MAGIC - JSON files are well-suited for structured, semi-structured, and nested data.
-- MAGIC - You can also query multiple JSON files using wildcard characters.
-- MAGIC - The `input_file_name()` function helps track which file each record came from.

-- COMMAND ----------

-- MAGIC %run ../Includes/Copy-Datasets

-- COMMAND ----------

--This will show all the files related here
%python
files = dbutils.fs.ls(f"{dataset_bookstore}/customers-json")
display(files)

-- COMMAND ----------

--This would query only the specified file
SELECT * FROM json.`${dataset.bookstore}/customers-json/export_001.json`

-- COMMAND ----------

--This would query all of the JSON files starting with the name export_ 
SELECT * FROM json.`${dataset.bookstore}/customers-json/export_*.json`

-- COMMAND ----------

--Query the complete directory of files, assuming all the files in the directory have the same format and schema
SELECT * FROM json.`${dataset.bookstore}/customers-json`

-- COMMAND ----------

SELECT count(*) FROM json.`${dataset.bookstore}/customers-json`

-- COMMAND ----------

--When reading multiple files, it's useful to add the input_file_name function, which is a built-in Spark SQL command that records the source data file for each record.
SELECT *,
    input_file_name() source_file
  FROM json.`${dataset.bookstore}/customers-json`;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Querying text Format
-- MAGIC
-- MAGIC The `text` format is used to read raw text-based files such as `.txt`, `.tsv`, `.csv`, or even malformed JSON.  
-- MAGIC Each row is loaded as a single column named `value`.
-- MAGIC
-- MAGIC - Useful for inspecting raw data or handling corrupted input files.
-- MAGIC - It’s ideal when you don’t know the structure of the data in advance.
-- MAGIC - You can apply parsing and transformations manually after loading.

-- COMMAND ----------

SELECT * FROM text.`${dataset.bookstore}/customers-json`

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC ## Querying binaryFile Format
-- MAGIC
-- MAGIC The `binaryFile` format is used for reading binary content like images, PDFs, or other non-text files.
-- MAGIC
-- MAGIC - Each row includes metadata like file path, size, and binary content.
-- MAGIC - Often used in machine learning or document/image processing workflows.
-- MAGIC - Common file types include `.png`, `.jpg`, `.pdf`, etc.
-- MAGIC - Unlike structured formats, binary files do not support schema inference.

-- COMMAND ----------

SELECT * FROM binaryFile.`${dataset.bookstore}/customers-json`

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ## Querying CSV 
-- MAGIC
-- MAGIC CSV is a simple, row-based format that **does not include schema information**.
-- MAGIC
-- MAGIC - Databricks must infer the schema or have it explicitly defined.
-- MAGIC - Header rows may be misread as data if not handled properly.
-- MAGIC - Options like `header=true`, `inferSchema=true`, and `delimiter=','` are often required.
-- MAGIC - Use caution: inconsistent formatting or missing headers can lead to incorrect parsing.

-- COMMAND ----------

SELECT * FROM csv.`${dataset.bookstore}/books-csv`

-- COMMAND ----------

--The USING keyword allows us to create a table against an external source like CSV format
--Here we need to specify the table schema (column names, types, file format, header, delimiter, etc)
CREATE TABLE books_csv
  (book_id STRING, title STRING, author STRING, category STRING, price DOUBLE)
USING CSV
OPTIONS (
  header = "true",
  delimiter = ";"
)
LOCATION "${dataset.bookstore}/books-csv"

-- COMMAND ----------

SELECT * FROM books_csv

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ## Limitations of Non-Delta Tables
-- MAGIC
-- MAGIC External tables created from formats like CSV, JSON, or Parquet without Delta:
-- MAGIC
-- MAGIC - Lack ACID compliance.
-- MAGIC - Don’t support features like schema enforcement, time travel, or optimized file compaction.
-- MAGIC - Are not automatically updated when new files are added unless the table is refreshed.
-- MAGIC - Have limited support for concurrent writes and reads.
-- MAGIC
-- MAGIC To overcome these issues, convert external tables into **Delta tables** using CTAS or data loading techniques.

-- COMMAND ----------

--Shows detail on the table
DESCRIBE EXTENDED books_csv

-- COMMAND ----------

--Check how many CSV files are in the directory
%python
files = dbutils.fs.ls(f"{dataset_bookstore}/books-csv")
display(files)

-- COMMAND ----------

--Use a Spark DataFrame API that allows us to write data in a specific format like CSV
%python
(spark.read
        .table("books_csv")
      .write
        .mode("append")
        .format("csv")
        .option('header', 'true')
        .option('delimiter', ';')
        .save(f"{dataset_bookstore}/books-csv"))

-- COMMAND ----------

--View how many CSV files are in the directory
%python
files = dbutils.fs.ls(f"{dataset_bookstore}/books-csv")
display(files)

-- COMMAND ----------

--Even with new data being successfully written to the table directory, we are unable to see this new data
--This is because Spark automatically cached the underlying data in local storage to ensure that on subsequent queries, Spark will provide the optimal performance by querying this local cache
SELECT COUNT(*) FROM books_csv

-- COMMAND ----------

--Manually refresh the cache of the data by running the REFRESH TABLE command
REFRESH TABLE books_csv

-- COMMAND ----------

SELECT COUNT(*) FROM books_csv

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## CTAS Statements (Create Table As Select)
-- MAGIC
-- MAGIC CTAS (Create Table As Select) allows you to:
-- MAGIC
-- MAGIC - Create a new table and populate it with data in a single step.
-- MAGIC - Automatically infer the schema from the result of the SELECT statement.
-- MAGIC - Transform and clean data while loading into a Delta Lake table.
-- MAGIC - Improve performance and data reliability by leveraging Delta features.
-- MAGIC
-- MAGIC It’s commonly used to convert external tables or temporary views into optimized Delta tables.

-- COMMAND ----------

--CTAS files do not support specifying additional file options
CREATE TABLE customers AS
SELECT * FROM json.`${dataset.bookstore}/customers-json`;

DESCRIBE EXTENDED customers;

-- COMMAND ----------

CREATE TABLE books_unparsed AS
SELECT * FROM csv.`${dataset.bookstore}/books-csv`;

SELECT * FROM books_unparsed;

-- COMMAND ----------

CREATE TEMP VIEW books_tmp_vw
   (book_id STRING, title STRING, author STRING, category STRING, price DOUBLE)
USING CSV
OPTIONS (
  path = "${dataset.bookstore}/books-csv/export_*.csv",
  header = "true",
  delimiter = ";"
);

CREATE TABLE books AS
  SELECT * FROM books_tmp_vw;
  
SELECT * FROM books

-- COMMAND ----------

DESCRIBE EXTENDED books
