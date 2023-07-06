/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql

import org.scalatest.funsuite.AnyFunSuite
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.types.{ArrayType, IntegerType, StringType, StructField, StructType}


class SQLJobTest extends AnyFunSuite{

  val spark = SparkSession.builder().appName("Test").master("local").getOrCreate()

  // Define input dataframe
  val inputSchema = StructType(Seq(
    StructField("Letter", StringType, nullable = false),
    StructField("Number", IntegerType, nullable = false)
  ))
  val inputRows = Seq(
    Row("A", 1),
    Row("B", 2),
    Row("C", 3)
  )
  val input: DataFrame = spark.createDataFrame(spark.sparkContext.parallelize(inputRows), inputSchema)

  test("Test getFormattedData method") {
    // Define expected dataframe
    val expectedSchema = StructType(Seq(
      StructField("result", ArrayType(StringType, containsNull = true), nullable = true),
      StructField("schema", ArrayType(StringType, containsNull = true), nullable = true),
      StructField("stepId", StringType, nullable = true),
      StructField("applicationId", StringType, nullable = true)
    ))
    val expectedRows = Seq(
      Row(
        Array("{'Letter':'A','Number':1}","{'Letter':'B','Number':2}", "{'Letter':'C','Number':3}"),
        Array("{'column_name':'Letter','data_type':'string'}", "{'column_name':'Number','data_type':'integer'}"),
        "unknown",
        spark.sparkContext.applicationId
      )
    )
    val expected: DataFrame = spark.createDataFrame(spark.sparkContext.parallelize(expectedRows), expectedSchema)

    // Compare the result
    val result = SQLJob.getFormattedData(input, spark)
    assertEqualDataframe(expected, result)
  }

  def assertEqualDataframe(expected: DataFrame, result: DataFrame): Unit ={
    assert(expected.schema === result.schema)
    assert(expected.collect() === result.collect())
  }
}
