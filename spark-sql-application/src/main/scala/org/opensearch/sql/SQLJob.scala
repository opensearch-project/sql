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
      // Execute SQL query
      val result: DataFrame = spark.sql(query)
      val resultJson = result.select(to_json(collect_list(struct(result.columns.map(col): _*)))).first().getString(0)

      // Convert the schema to a DataFrame
      val schema = result.schema
      val schemaRow = schema.fields.map { field =>
        Row(field.name, field.dataType.typeName)
      }
      val schemaDF = spark.createDataFrame(spark.sparkContext.parallelize(schemaRow), StructType(Seq(
        StructField("column_name", StringType, nullable = false),
        StructField("data_type", StringType, nullable = false)
      )))
      val schemaJson = schemaDF.select(to_json(collect_list(struct(schemaDF.columns.map(col): _*)))).first().getString(0)

      // Create a DataFrame with stepId, schema and result
      val dataRow = Seq(
        Row(sys.env.getOrElse("EMR_STEP_ID", ""))
      )
      val dataDF = spark.createDataFrame(spark.sparkContext.parallelize(dataRow), StructType(Seq(
        StructField("stepId", StringType, nullable = false)
      )))

      val data = dataDF.withColumn("schema", lit(schemaJson)).withColumn("result",lit(resultJson))

      // Write data to OpenSearch index
      data.write
        .format("flint")
        .options(aos)
        .mode("append")
        .save(index)

    } finally {
      // Stop SparkSession
      spark.stop()
    }
  }
}
