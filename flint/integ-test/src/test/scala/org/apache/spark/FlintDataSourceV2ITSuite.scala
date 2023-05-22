/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.apache.spark

import org.opensearch.flint.OpenSearchSuite

import org.apache.spark.sql.{DataFrame, ExplainSuiteHelper, QueryTest, Row}
import org.apache.spark.sql.catalyst.plans.logical.Filter
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2ScanRelation
import org.apache.spark.sql.functions.asc
import org.apache.spark.sql.types._

/**
 * OpenSearch related integration test.
 */
class FlintDataSourceV2ITSuite
    extends QueryTest
    with FlintSuite
    with OpenSearchSuite
    with ExplainSuiteHelper {

  import testImplicits._

  test("create dataframe successfully from flint datasource") {
    val indexName = "t0001"
    withIndexName(indexName) {
      simpleIndex(indexName)
      val schema = StructType(
        Seq(
          StructField("accountId", StringType, true),
          StructField("eventName", StringType, true),
          StructField("eventSource", StringType, true)))
      val df = spark.sqlContext.read
        .format("flint")
        .options(openSearchOptions)
        .schema(schema)
        .load(indexName)

      assert(df.count() == 1)
      checkAnswer(df, Row("123", "event", "source"))
    }
  }

  test("scroll api test, force scroll_size = 1") {
    val indexName = "t0002"
    withIndexName(indexName) {
      multipleDocIndex(indexName, 5)
      val schema = StructType(Seq(StructField("id", IntegerType, true)))

      val df = spark.sqlContext.read
        .format("flint")
        .options(openSearchOptions + ("scroll_size" -> "1"))
        .schema(schema)
        .load(indexName)
        .sort(asc("id"))

      assert(df.count() == 5)
      checkAnswer(df, (1 to 5).map(i => Row(i)))
    }
  }

  test("scan with filter push-down") {
    val indexName = "t0003"
    withIndexName(indexName) {
      val mappings = """{
                       |  "properties": {
                       |    "aInt": {
                       |      "type": "integer"
                       |    },
                       |    "aString": {
                       |      "type": "keyword"
                       |    },
                       |    "aText": {
                       |      "type": "text"
                       |    }
                       |  }
                       |}""".stripMargin
      val docs = Seq(
        """{
          |  "aInt": 1,
          |  "aString": "a",
          |  "aText": "i am first"
          |}""".stripMargin,
        """{
          |  "aInt": 2,
          |  "aString": "b",
          |  "aText": "i am second"
          |}""".stripMargin)
      index(indexName, oneNodeSetting, mappings, docs)

      val schema = StructType(
        Seq(
          StructField("aInt", IntegerType, nullable = true),
          StructField("aString", StringType, nullable = true),
          StructField(
            "aText",
            StringType,
            nullable = true,
            new MetadataBuilder().putString("osType", "text").build())))

      val df = spark.sqlContext.read
        .format("flint")
        .options(openSearchOptions)
        .schema(schema)
        .load(indexName)

      val df1 = df.filter($"aInt" > 1)
      checkFiltersRemoved(df1)
      checkPushedInfo(df1, "PushedPredicates: [aInt IS NOT NULL, aInt > 1]")
      checkAnswer(df1, Row(2, "b", "i am second"))

      val df2 = df.filter($"aText".contains("second"))
      checkFiltersRemoved(df2)
      checkPushedInfo(df2, "PushedPredicates: [aText IS NOT NULL, aText LIKE '%second%']")
      checkAnswer(df2, Row(2, "b", "i am second"))

      val df3 =
        df.filter($"aInt" === 1 || $"aString" === "b").sort(asc("aInt"))
      checkFiltersRemoved(df3)
      checkPushedInfo(df3, "PushedPredicates: [(aInt = 1) OR (aString = 'b')]")
      checkAnswer(df3, Seq(Row(1, "a", "i am first"), Row(2, "b", "i am second")))

      val df4 = df.filter($"aInt" > 1 && $"aText".contains("second"))
      checkFiltersRemoved(df4)
      checkPushedInfo(
        df4,
        "PushedPredicates: [aInt IS NOT NULL, aText IS NOT NULL, aInt > 1, aText LIKE '%second%']")
      checkAnswer(df4, Row(2, "b", "i am second"))
    }
  }

  /**
   * Copy from SPARK JDBCV2Suite.
   */
  private def checkPushedInfo(df: DataFrame, expectedPlanFragment: String*): Unit = {
    df.queryExecution.optimizedPlan.collect { case _: DataSourceV2ScanRelation =>
      checkKeywordsExistsInExplain(df, expectedPlanFragment: _*)
    }
  }

  /**
   * Copy from SPARK JDBCV2Suite.
   */
  private def checkFiltersRemoved(df: DataFrame, removed: Boolean = true): Unit = {
    val filters = df.queryExecution.optimizedPlan.collect { case f: Filter =>
      f
    }
    if (removed) {
      assert(filters.isEmpty)
    } else {
      assert(filters.nonEmpty)
    }
  }
}
