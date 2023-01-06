-- Databricks notebook source
-- MAGIC %python 
-- MAGIC 
-- MAGIC # Creating mount point
-- MAGIC dbutils.fs.mount(
-- MAGIC   source = "wasbs://raw@blobazuretest1234567.blob.core.windows.net/",
-- MAGIC   mount_point = "/mnt/raw",
-- MAGIC   extra_configs = {"fs.azure.account.key.blobazuretest1234567.blob.core.windows.net":
-- MAGIC "v/AgOJ+GGNoV8ipq7h+dD8ueOCsABdAYrSf5OSc5E3VbqLdZqTYyUYKSj3Y7TwKDkZBJagMfreGs+AStImrQnA=="})

-- COMMAND ----------

-- MAGIC %python 
-- MAGIC 
-- MAGIC from pyspark.sql.functions import *
-- MAGIC import urllib
-- MAGIC 
-- MAGIC df = spark.read.format("csv")\
-- MAGIC .load("/mnt/raw/Sample - Superstore Sales (Excel).xls.txt")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC display(df)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC 
-- MAGIC file_location = "/mnt/raw/Sample - Superstore Sales (Excel).xls.txt"
-- MAGIC file_type = "csv"
-- MAGIC infer_schema = "true"
-- MAGIC first_row_is_header = "true"
-- MAGIC delimiter = ","
-- MAGIC df = spark.read.format(file_type) \
-- MAGIC  .option("inferSchema", infer_schema) \
-- MAGIC  .option("header", first_row_is_header) \
-- MAGIC  .option("sep", delimiter) \
-- MAGIC  .load(file_location)
-- MAGIC display(df)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC #df.write.mode("overwrite").saveAsTable("Store_data")
-- MAGIC # Get a list of the current column names
-- MAGIC column_names = df.columns
-- MAGIC 
-- MAGIC # Use a list comprehension to create a list of new column names with the spaces replaced by underscores
-- MAGIC new_column_names = [name.replace(" ", "_") for name in column_names]
-- MAGIC 
-- MAGIC # Use the `toDF` method to create a new DataFrame with the new column names
-- MAGIC df = df.toDF(*new_column_names)
-- MAGIC 
-- MAGIC  

-- COMMAND ----------

-- MAGIC %python
-- MAGIC 
-- MAGIC df.columns 

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df.write.mode("overwrite").saveAsTable("Store_data")

-- COMMAND ----------

select * from Store_data

-- COMMAND ----------

select sum(Profit),Province from Store_data
group by Province

-- COMMAND ----------

select sum(Sales),Province,Product_Category from Store_data
where Province = "Newfoundland"
group by Province,Product_Category
