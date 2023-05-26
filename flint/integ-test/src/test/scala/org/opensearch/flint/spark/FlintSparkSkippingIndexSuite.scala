/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark

import scala.Option._

import com.stephenn.scalatest.jsonassert.JsonMatchers.matchJson
import org.opensearch.flint.OpenSearchSuite
import org.opensearch.flint.spark.FlintSpark.RefreshMode.{FULL, INCREMENTAL}
import org.opensearch.flint.spark.skipping.FlintSparkSkippingIndex.getSkippingIndexName
import org.scalatest.matchers.must.Matchers.{defined, have}
import org.scalatest.matchers.should.Matchers.convertToAnyShouldWrapper

import org.apache.spark.FlintSuite
import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.flint.FlintDataSourceV2.FLINT_DATASOURCE
import org.apache.spark.sql.flint.config.FlintSparkConf._
import org.apache.spark.sql.streaming.StreamTest

class FlintSparkSkippingIndexSuite
    extends QueryTest
    with FlintSuite
    with OpenSearchSuite
    with StreamTest {

  /** Flint Spark high level API being tested */
  lazy val flint: FlintSpark = {
    setFlintSparkConf(HOST_ENDPOINT, openSearchHost)
    setFlintSparkConf(HOST_PORT, openSearchPort)
    setFlintSparkConf(REFRESH_POLICY, "true")
    new FlintSpark(spark)
  }

  /** Test table and index name */
  private val testTable = "test"
  private val testIndex = getSkippingIndexName(testTable)

  override def beforeAll(): Unit = {
    super.beforeAll()

    sql(s"""
        | CREATE TABLE $testTable
        | (
        |   name STRING
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

    sql(s"""
        | INSERT INTO $testTable
        | PARTITION (year=2023, month=04)
        | VALUES ('Hello')
        | """.stripMargin)

    sql(s"""
        | INSERT INTO $testTable
        | PARTITION (year=2023, month=05)
        | VALUES ('World')
        | """.stripMargin)
  }

  override def afterEach(): Unit = {
    super.afterEach()

    // Delete all test indices
    flint.deleteIndex(testIndex)

    // Stop all streaming jobs if any
    spark.streams.active.foreach { job =>
      job.stop()
      job.awaitTermination()
    }
  }

  test("create skipping index with metadata successfully") {
    flint
      .skippingIndex()
      .onTable(testTable)
      .addPartitionIndex("year", "month")
      .create()

    val indexName = s"flint_${testTable}_skipping_index"
    val index = flint.describeIndex(indexName)
    index shouldBe defined
    index.get.metadata().getContent should matchJson(s"""{
        |   "_meta": {
        |     "kind": "skipping",
        |     "indexedColumns": [
        |     {
        |        "kind": "partition",
        |        "columnName": "year",
        |        "columnType": "int"
        |     },
        |     {
        |        "kind": "partition",
        |        "columnName": "month",
        |        "columnType": "int"
        |     }],
        |     "source": "$testTable"
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

  test("full refresh skipping index successfully") {
    // Create Flint index and wait for complete
    flint
      .skippingIndex()
      .onTable(testTable)
      .addPartitionIndex("year", "month")
      .create()

    val jobId = flint.refreshIndex(testIndex, FULL)
    jobId shouldBe empty

    // TODO: add query index API to avoid this duplicate
    val indexData =
      spark.read
        .format(FLINT_DATASOURCE)
        .schema("year INT, month INT, file_path STRING")
        .options(openSearchOptions)
        .load(testIndex)
        .collect()
        .toSet
    indexData should have size 2
  }

  test("incremental refresh skipping index successfully") {
    // Create Flint index and wait for complete
    flint
      .skippingIndex()
      .onTable(testTable)
      .addPartitionIndex("year", "month")
      .create()

    val jobId = flint.refreshIndex(testIndex, INCREMENTAL)
    jobId shouldBe defined

    val job = spark.streams.get(jobId.get)
    failAfter(streamingTimeout) {
      job.processAllAvailable()
    }

    val indexData =
      spark.read
        .format(FLINT_DATASOURCE)
        .schema("year INT, month INT, file_path STRING")
        .options(openSearchOptions)
        .load(testIndex)
        .collect()
        .toSet
    indexData should have size 2
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
