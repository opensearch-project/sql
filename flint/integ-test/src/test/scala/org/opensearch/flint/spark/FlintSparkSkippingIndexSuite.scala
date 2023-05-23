/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark

import scala.Option._

import com.stephenn.scalatest.jsonassert.JsonMatchers.matchJson
import org.opensearch.flint.OpenSearchSuite
import org.opensearch.flint.spark.FlintSpark.FLINT_INDEX_STORE_LOCATION
import org.opensearch.flint.spark.skipping.{ApplyFlintSparkSkippingIndex, FlintSparkSkippingIndex}
import org.scalatest.matchers.must.Matchers.defined
import org.scalatest.matchers.should.Matchers.convertToAnyShouldWrapper

import org.apache.spark.FlintSuite

class FlintSparkSkippingIndexSuite extends FlintSuite with OpenSearchSuite {

  /** Flint Spark high level API being tested */
  lazy val flint: FlintSpark = {
    spark.conf.set(FLINT_INDEX_STORE_LOCATION, openSearchHost)
    spark.conf.set(FlintSpark.FLINT_INDEX_STORE_PORT, openSearchPort)
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
        |       "kind": "partition",
        |       "columnName": "year",
        |       "columnType": "int"
        |     },
        |     {
        |       "kind": "partition",
        |       "columnName": "month",
        |       "columnType": "int"
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

  test("applicable query can be rewritten with skipping index") {
    flint
      .skippingIndex()
      .onTable(testTable)
      .addPartitionIndex("year", "month")
      .create()

    val query = sql(s"""
         | SELECT name
         | FROM $testTable
         | WHERE year = 2023 AND month = 04
         |""".stripMargin)

    val rewriter = new ApplyFlintSparkSkippingIndex(flint)
    rewriter.apply(query.queryExecution.optimizedPlan)
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
