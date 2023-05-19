/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark

import com.stephenn.scalatest.jsonassert.JsonMatchers.matchJson
import org.apache.spark.FlintSuite
import org.opensearch.flint.OpenSearchSuite
import org.opensearch.flint.spark.skipping.FlintSparkSkippingIndex
import org.opensearch.flint.spark.skipping.partition.PartitionSkippingStrategy
import org.scalatest.matchers.must.Matchers.defined
import org.scalatest.matchers.should.Matchers.convertToAnyShouldWrapper

import scala.Option._

class FlintSparkSkippingIndexSuite extends FlintSuite with OpenSearchSuite {

  lazy val flint: FlintSpark = {
    spark.conf.set(FlintSpark.FLINT_INDEX_STORE_LOCATION, openSearchHost)
    spark.conf.set(FlintSpark.FLINT_INDEX_STORE_PORT, openSearchPort)
    new FlintSpark(spark)
  }

  /** Test table name. */
  val testTable: String = "test"

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

  test("create value list skipping index") {
    val index = new FlintSparkSkippingIndex(
      testTable,
      Seq(new PartitionSkippingStrategy))
    flint.createIndex(index)

    val metadata = flint.describeIndex(index.name())
    metadata shouldBe defined
    metadata.get.getContent should matchJson(
      """ {
        |   "_meta": {
        |     "kind": "SkippingIndex",
        |     "indexedColumns": [{}]
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
        |""".stripMargin
    )
  }

  test("describe non-exist index should return empty") {
    flint.describeIndex("non-exist") shouldBe empty
  }
}
