/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql

import org.apache.spark.sql.{DataFrame, SparkSession, Row}
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

object SQLJob {
  def main(args: Array[String]) {
    // Get the SQL query and Opensearch Config from the command line arguments
    val query = args(0)
    val index = args(1)
    val host = args(2)
    val port = args(3)
    val scheme = args(4)
    val auth = args(5)
    val region = args(6)

    // Create a SparkSession
    val spark = SparkSession.builder().appName("SQLJob").getOrCreate()

    try {
      // Execute SQL query
      val result: DataFrame = spark.sql(query)
      val resultJson = getJson(result)

      // Convert the schema to a DataFrame
      val schema = schemaDF(spark, result)
      val schemaJson = getJson(schema)

      // Create a DataFrame with stepId, schema and result
      val data = stepIdDF(spark).withColumn("schema", lit(schemaJson)).withColumn("result",lit(resultJson))

      // Write data to OpenSearch index
      val aos = Map(
        "host" -> host,
        "port" -> port,
        "scheme" -> scheme,
        "auth" -> auth,
        "region" -> region)

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

  def getJson(df: DataFrame): String = {
    df.select(to_json(collect_list(struct(df.columns.map(col): _*)))).first().getString(0)
  }

  def schemaDF(spark: SparkSession, result: DataFrame): DataFrame  = {
    val schema = result.schema
    val schemaRow = schema.fields.map { field =>
      Row(field.name, field.dataType.typeName)
    }
    spark.createDataFrame(spark.sparkContext.parallelize(schemaRow), StructType(Seq(
      StructField("column_name", StringType, nullable = false),
      StructField("data_type", StringType, nullable = false)
    )))
  }

  def stepIdDF(spark: SparkSession): DataFrame  = {
    val dataRow = Seq(
      Row(sys.env.getOrElse("EMR_STEP_ID", ""))
    )
    spark.createDataFrame(spark.sparkContext.parallelize(dataRow), StructType(Seq(
      StructField("stepId", StringType, nullable = false)
    )))
  }
}
