package org.opensearch.sql

import org.apache.spark.sql.{DataFrame, SparkSession, SQLContext, Row}
import org.apache.spark.SparkConf
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

object SQLJob {
  def main(args: Array[String]) {
    // Get the SQL query and Opensearch Config from the command line arguments
    val query = args(0)
    val index = args(1)
    val aos_host = args(2)
    val aos_region = args(3)

     val aos = Map(
      "host" -> aos_host, 
      "port" -> "-1", 
      "scheme" -> "https", 
      "auth" -> "sigv4", 
      "region" -> aos_region)

    // Create a SparkSession
    val spark = SparkSession.builder().appName("SQLJob").getOrCreate()

    try {
      // Create a tableschema for the DataFrame
      val tableschema = StructType(Seq(
        StructField("name", StringType, nullable = false),
        StructField("age", IntegerType, nullable = false),
        StructField("city", StringType, nullable = false)
      ))

      // Create a sequence of Row objects representing the data
      val tabledata = Seq(
        Row("Tina", 29, "Bellevue"),
        Row("Jane", 25, "London"),
        Row("Mike", 35, "Paris")
      )

      // Create a DataFrame from the tableschema and data
      val df = spark.createDataFrame(spark.sparkContext.parallelize(tabledata), tableschema)

      // Register the DataFrame as a temporary table/view
      df.createOrReplaceTempView("my_table")  // Replace "my_table" with your desired name

      // Execute SQL query
      val result: DataFrame = spark.sql(query)
      val resultJsonBlob = result.select(to_json(collect_list(struct(result.columns.map(col): _*)))).first().getString(0)

      // Convert the schema to a DataFrame
      // Get the schema of the DataFrame
      val resultschema = result.schema
      val resultschemaRows = resultschema.fields.map { field =>
        Row(field.name, field.dataType.typeName)
      }
      val resultschemaDF = spark.createDataFrame(spark.sparkContext.parallelize(resultschemaRows), StructType(Seq(
        StructField("column_name", StringType, nullable = false),
        StructField("data_type", StringType, nullable = false)
      )))
      val schemaJsonBlob = resultschemaDF.select(to_json(collect_list(struct(resultschemaDF.columns.map(col): _*)))).first().getString(0)

      // Create a step DataFrame from the tableschema and data
      val envVarValue = sys.env.getOrElse("EMR_STEP_ID", "")

      val stepRows = Seq(
        Row(envVarValue)
      )

      val stepDF = spark.createDataFrame(spark.sparkContext.parallelize(stepRows), StructType(Seq(
        StructField("step_id", StringType, nullable = false)
      )))

      // Add JSON blob column to df1
      val stepWithSchema = stepDF.withColumn("schema", lit(schemaJsonBlob)).withColumn("result",lit(resultJsonBlob))
      stepWithSchema.show(false)

     stepWithSchema.write
        .format("flint")
        .options(aos)
        .mode("append")
       .save(index)
     println("Result saved to OpenSearch index successfully.")

    } finally {
      // Stop SparkSession
      spark.stop()
    }
  }
}
