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
  val inputDF: DataFrame = spark.createDataFrame(spark.sparkContext.parallelize(inputRows), inputSchema)

  test("Test getJson method") {
    // Define expected dataframe
    val expectedSchema = StructType(Seq(
      StructField("json", StringType, nullable = true)
    ))
    val expectedRows = Seq(
      Row("{\"Letter\":\"A\",\"Number\":1}"),
      Row("{\"Letter\":\"B\",\"Number\":2}"),
      Row("{\"Letter\":\"C\",\"Number\":3}")
    )
    val expected: DataFrame = spark.createDataFrame(spark.sparkContext.parallelize(expectedRows), expectedSchema)

    // Compare the result
    val result = SQLJob.getJson(inputDF)
    assertEqualDataframe(expected, result)
  }

  test("Test getSchema method") {
    // Define expected dataframe
    val expectedSchema = StructType(Seq(
      StructField("column_name", StringType, nullable = false),
      StructField("data_type", StringType, nullable = false)
    ))
    val expectedRows = Seq(
      Row("Letter","string"),
      Row("Number", "integer")
    )
    val expected: DataFrame = spark.createDataFrame(spark.sparkContext.parallelize(expectedRows), expectedSchema)

    // Compare the result
    val result = SQLJob.getSchema(spark, inputDF)
    assertEqualDataframe(expected, result)
  }

  test("Test getList method") {
    val name = "list"
    // Define expected dataframe
    val expectedSchema = StructType(Seq(
      StructField(name, ArrayType(StringType, containsNull = true), nullable = true)
    ))
    val expectedRows = Seq(
      Row(Seq("{\"Letter\":\"A\",\"Number\":1}",
        "{\"Letter\":\"B\",\"Number\":2}",
        "{\"Letter\":\"C\",\"Number\":3}"))
    )
    val expected: DataFrame = spark.createDataFrame(spark.sparkContext.parallelize(expectedRows), expectedSchema)

    // Compare the result
    val result = SQLJob.getList(spark, SQLJob.getJson(inputDF), name)
    assertEqualDataframe(expected, result)
  }

  def assertEqualDataframe(expected: DataFrame, result: DataFrame): Unit ={
    assert(expected.schema === result.schema)
    assert(expected.collect() === result.collect())
  }
}
