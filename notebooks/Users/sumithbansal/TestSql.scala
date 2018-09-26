// Databricks notebook source
// MAGIC %md
// MAGIC #Customer Details

// COMMAND ----------

// Databricks notebook source
// MAGIC %md
// MAGIC # Learning ADB Professionally

// COMMAND ----------

val defaultMoviesUrl = "wasbs://blob3day3@sumiday3storageaccount.blob.core.windows.net"
//val defaultMoviesUrl = "https://sumiday3storageaccount.blob.core.windows.net/blob3day3/movies.csv"
//val defaultRatingsUrl = "adl://day3lakestore.azuredatalakestore.net/data/ratings.csv"

val moviesUrl = dbutils.widgets.text("moviesUrl","")
//val ratingsUrl = dbutils.widgets.text("ratingsUrl", "")

var inputMoviesUrl = dbutils.widgets.get("moviesUrl")

if(inputMoviesUrl == null) {
  inputMoviesUrl = defaultMoviesUrl
}

//var inputRatingsUrl = dbutils.widgets.get("ratingsUrl")

//if(inputRatingsUrl == null) {
//  inputRatingsUrl = defaultRatingsUrl
//}

// COMMAND ----------

val fileName = "wasbs://blob3day3@sumiday3storageaccount.blob.core.windows.net/customer-orders.csv"
import org.apache.spark.sql._
import org.apache.spark.sql.types._
val schema = StructType(
Array(
StructField("customerId",IntegerType,true),
StructField("lineItemId",IntegerType,true),
StructField("Value",DoubleType,true)
)
)
val data = spark.read.option("inferSchema",false).option("header","false").option("sep",",").schema(schema).csv(fileName)
data.schema
data.printSchema
data.createOrReplaceTempView("customer")
val result = spark.sql("SELECT customerId, SUM(Value) as Value FROM customer GROUP BY customerId ORDER BY SUM(Value) DESC limit 10")
display(result)


// COMMAND ----------

