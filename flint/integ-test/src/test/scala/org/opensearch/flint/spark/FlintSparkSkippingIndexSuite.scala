/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark

import scala.Option._

import com.stephenn.scalatest.jsonassert.JsonMatchers.matchJson
import org.opensearch.flint.OpenSearchSuite
import org.opensearch.flint.spark.skipping.FlintSparkSkippingIndex
import org.scalatest.matchers.must.Matchers.defined
import org.scalatest.matchers.should.Matchers.convertToAnyShouldWrapper

import org.apache.spark.FlintSuite
import org.apache.spark.sql.flint.config.FlintSparkConf

class FlintSparkSkippingIndexSuite extends FlintSuite with OpenSearchSuite {

  /** Flint Spark high level API being tested */
  lazy val flint: FlintSpark = {
    spark.conf.set(FlintSparkConf.sparkConf(FlintSparkConf.HOST_ENDPOINT.key), openSearchHost)
    spark.conf.set(FlintSparkConf.sparkConf(FlintSparkConf.HOST_PORT.key), openSearchPort)
    new FlintSpark(spark)
  }

  /** Test table name. */
  private val testTable = "test"

  override def beforeAll(): Unit = {
    super.beforeAll()

    val tempLocation = spark.conf.get("spark.sql.warehouse.dir")
    sql(s"""
        | CREATE TABLE $testTable
        | (
        |   name STRING
        | )
        | USING CSV
        | OPTIONS (
        |  path '$tempLocation/$testTable',
        |  header 'false',
        |  delimiter '\t'
        | )
        | PARTITIONED BY (
        |    year INT,
        |    month INT
        | )
        |""".stripMargin)
  }

  override def afterEach(): Unit = {
    super.afterEach()

    val indexName = FlintSparkSkippingIndex.getIndexName(testTable)
    flint.deleteIndex(indexName)
  }

  test("create skipping index with metadata successfully") {
    flint
      .skippingIndex()
      .onTable(testTable)
      .addPartitionIndex("year", "month")
      .create()

    val indexName = s"flint_${testTable}_skipping_index"
    val metadata = flint.describeIndex(indexName)
    metadata shouldBe defined
    metadata.get.getContent should matchJson(""" {
        |   "_meta": {
        |     "kind": "SkippingIndex",
        |     "indexedColumns": [
        |     {
        |       "year": "int"
        |     },
        |     {
        |       "month": "int"
        |     }]
        |   },
        |   "properties": {
        |     "year": {
        |       "type": "integer"
        |     },
        |     "month": {
        |       "type": "integer"
        |     },
        |     "file_path": {
        |       "type": "keyword"
        |     }
        |   }
        | }
        |""".stripMargin)
  }

  test("can have only 1 skipping index on a table") {
    flint
      .skippingIndex()
      .onTable(testTable)
      .create()

    assertThrows[IllegalStateException] {
      flint
        .skippingIndex()
        .onTable(testTable)
        .create()
    }
  }

  test("should return empty if describe index not exist") {
    flint.describeIndex("non-exist") shouldBe empty
  }
}
