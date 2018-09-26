// Databricks notebook source
import org.apache.spark.sql._
import org.apache.spark.sql.types._

val url = "wasbs://data@iomegastorage.blob.core.windows.net/customer-orders.csv"
var orderSchema = StructType(
  Array(
    StructField("userId", IntegerType, true),
    StructField("lineItemId", IntegerType, true),
    StructField("orderAmount", DoubleType, true)))


val data = spark.read.
  option("header","false").
  option("sep",",").
  option("inferSchema", "false").
  schema(orderSchema).
  csv(url)

data.createOrReplaceTempView("orders")

// COMMAND ----------

// MAGIC %sql
// MAGIC 
// MAGIC CREATE TABLE ProcessedOrders
// MAGIC   USING PARQUET
// MAGIC   PARTITIONED BY (userId)
// MAGIC   OPTIONS ('compression' = 'snappy')
// MAGIC   AS
// MAGIC   SELECT userId, SUM(orderAmount) AS totalAmount
// MAGIC     FROM orders
// MAGIC     GROUP BY userId
// MAGIC     ORDER BY totalAmount DESC
// MAGIC     LIMIT 10

// COMMAND ----------

val processedOrdersDF = spark.sql("SELECT userId, SUM(orderAmount) AS totalAmount FROM orders GROUP BY userId ORDER BY totalAmount DESC LIMIT 10")

// COMMAND ----------

import com.microsoft.azure.sqldb.spark.bulkcopy.BulkCopyMetadata
import com.microsoft.azure.sqldb.spark.config.Config
import com.microsoft.azure.sqldb.spark.connect._

var bulkCopyMetadata = new BulkCopyMetadata

bulkCopyMetadata.addColumnMetadata(1, "userId", java.sql.Types.INTEGER, 0, 0)
bulkCopyMetadata.addColumnMetadata(2, "totalAmount", java.sql.Types.DOUBLE, 50, 0)


// COMMAND ----------


val bulkCopyConfig = Config(Map(
  "url"               -> "sumithdatabase.database.windows.net",
  "databaseName"      -> "sumithdatabase",
  "user"              -> "bansal301",
  "password"          -> "kidslove@123",
  "dbTable"           -> "dbo.ProcessedOrders",
  "driver" -> "com.microsoft.sqlserver.jdbc.SQLServerDriver",
  "bulkCopyBatchSize" -> "2500",
  "bulkCopyTableLock" -> "true",
  "bulkCopyTimeout"   -> "600"
))

processedOrdersDF.bulkCopyToSqlDB(bulkCopyConfig, bulkCopyMetadata)

// COMMAND ----------

