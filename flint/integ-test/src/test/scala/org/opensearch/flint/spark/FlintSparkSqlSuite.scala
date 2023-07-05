/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark

import scala.Option.empty

import org.opensearch.flint.OpenSearchSuite
import org.opensearch.flint.spark.skipping.FlintSparkSkippingIndex.getSkippingIndexName
import org.scalatest.matchers.should.Matchers.convertToAnyShouldWrapper

import org.apache.spark.FlintSuite
import org.apache.spark.sql.{QueryTest, Row}
import org.apache.spark.sql.flint.config.FlintSparkConf.{HOST_ENDPOINT, HOST_PORT}

class FlintSparkSqlSuite extends QueryTest with FlintSuite with OpenSearchSuite {

  /** Flint Spark high level API for assertion */
  private lazy val flint: FlintSpark = {
    setFlintSparkConf(HOST_ENDPOINT, openSearchHost)
    setFlintSparkConf(HOST_PORT, openSearchPort)
    new FlintSpark(spark)
  }

  /** Test table and index name */
  private val testTable = "flint_sql_test"
  private val testIndex = getSkippingIndexName(testTable)

  override def beforeAll(): Unit = {
    super.beforeAll()
    sql(s"""
           | CREATE TABLE $testTable
           | (
           |   name STRING,
           |   age INT
           | )
           | USING CSV
           | OPTIONS (
           |  header 'false',
           |  delimiter '\t'
           | )
           | PARTITIONED BY (
           |    year INT,
           |    month INT
           | )
           |""".stripMargin)
  }

  protected override def beforeEach(): Unit = {
    super.beforeEach()
    flint
      .skippingIndex()
      .onTable(testTable)
      .addPartitions("year")
      .addValueSet("name")
      .addMinMax("age")
      .create()
  }

  protected override def afterEach(): Unit = {
    super.afterEach()
    flint.deleteIndex(testIndex)
  }

  test("describe skipping index") {
    val result = sql(s"DESC SKIPPING INDEX ON $testTable")

    checkAnswer(
      result,
      Seq(
        Row("year", "int", "Partition"),
        Row("name", "string", "ValuesSet"),
        Row("age", "int", "MinMax")))
  }

  test("should return empty if no skipping index to describe") {
    flint.deleteIndex(testIndex)

    val result = sql(s"DESC SKIPPING INDEX ON $testTable")
    checkAnswer(result, Seq.empty)
  }

  test("drop skipping index") {
    sql(s"DROP SKIPPING INDEX ON $testTable")

    flint.describeIndex(testIndex) shouldBe empty
  }
}
