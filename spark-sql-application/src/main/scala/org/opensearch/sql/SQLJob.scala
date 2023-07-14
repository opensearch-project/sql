/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession, Row}
import org.apache.spark.sql.types._

/**
 * Spark SQL Application entrypoint
 *
 * @param args(0)
 *   sql query
 * @param args(1)
 *   opensearch index name
 * @param args(2-6)
 *   opensearch connection values required for flint-integration jar. host, port, scheme, auth, region respectively.
 * @return
 *   write sql query result to given opensearch index
 */
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

    val conf: SparkConf = new SparkConf()
      .setAppName("SQLJob")
      .set("spark.sql.extensions", "org.opensearch.flint.spark.FlintSparkExtensions")
      .set("spark.datasource.flint.host", host)
      .set("spark.datasource.flint.port", port)
      .set("spark.datasource.flint.scheme", scheme)
      .set("spark.datasource.flint.auth", auth)
      .set("spark.datasource.flint.region", region)

    // Create a SparkSession
    val spark = SparkSession.builder().config(conf).enableHiveSupport().getOrCreate()

    try {
      // Execute SQL query
      val result: DataFrame = spark.sql(query)

      // Get Data
      val data = getFormattedData(result, spark)

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

  /**
   * Create a new formatted dataframe with json result, json schema and EMR_STEP_ID.
   *
   * @param result
   *    sql query result dataframe
   * @param spark
   *    spark session
   * @return
   *    dataframe with result, schema and emr step id
   */
  def getFormattedData(result: DataFrame, spark: SparkSession): DataFrame = {
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
      StructField("stepId", StringType, nullable = true),
      StructField("applicationId", StringType, nullable = true)))

    // Create the data rows
    val rows = Seq((
      result.toJSON.collect.toList.map(_.replaceAll("'", "\\\\'").replaceAll("\"", "'")),
      resultSchema.toJSON.collect.toList.map(_.replaceAll("\"", "'")),
      sys.env.getOrElse("EMR_STEP_ID", "unknown"),
      spark.sparkContext.applicationId))

    // Create the DataFrame for data
    spark.createDataFrame(rows).toDF(schema.fields.map(_.name): _*)
  }
}
