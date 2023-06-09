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
      val schemaJson = getJson(getSchema(spark, result))
      val data = getList(spark, resultJson, "result").join(getList(spark, schemaJson, "schema")).withColumn("stepId", lit(sys.env.getOrElse("EMR_STEP_ID", "")))

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

  def getJson(df: DataFrame): DataFrame = {
    df.select(to_json(struct(df.columns.map(col): _*))).toDF("json")
  }

  def getSchema(spark: SparkSession, result: DataFrame): DataFrame  = {
    val resultschema = result.schema
    val resultschemaRows = resultschema.fields.map { field =>
      Row(field.name, field.dataType.typeName)
    }
    spark.createDataFrame(spark.sparkContext.parallelize(resultschemaRows), StructType(Seq(
      StructField("column_name", StringType, nullable = false),
      StructField("data_type", StringType, nullable = false)
    )))
  }

  def getList(spark: SparkSession, resultJson: DataFrame, name:String): DataFrame = {
    val list = resultJson.agg(collect_list("json").as(name)).head().getSeq[String](0)
    val schema = StructType(Seq(StructField(name, ArrayType(StringType))))
    val rdd = spark.sparkContext.parallelize(Seq(Row(list)))
    spark.createDataFrame(rdd, schema)
  }
}
