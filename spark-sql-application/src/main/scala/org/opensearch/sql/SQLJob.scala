/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql

import org.apache.spark.sql.{DataFrame, SparkSession, Row}
import org.apache.spark.sql.types._

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

      // Get Data
      val data = getData(result, spark)

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

  def getData(result: DataFrame, spark: SparkSession): DataFrame = {
    // Create the schema dataframe
    val schemaRows = result.schema.fields.map { field =>
      Row(field.name, field.dataType.typeName)
    }
    val resultSchema = spark.createDataFrame(spark.sparkContext.parallelize(schemaRows), StructType(Seq(
      StructField("column_name", StringType, nullable = false),
      StructField("data_type", StringType, nullable = false))))

    // Define the data schema
    val schema = StructType(Seq(
      StructField("result", ArrayType(StringType, containsNull = true), nullable = true),
      StructField("schema", ArrayType(StringType, containsNull = true), nullable = true),
      StructField("stepId", StringType, nullable = true)))

    // Create the data rows
    val rows = Seq((
      result.toJSON.collect.toList.map(_.replaceAll("\"", "'")),
      resultSchema.toJSON.collect.toList.map(_.replaceAll("\"", "'")),
      sys.env.getOrElse("EMR_STEP_ID", "")))

    // Create the DataFrame for data
    spark.createDataFrame(rows).toDF(schema.fields.map(_.name): _*)
  }
}
