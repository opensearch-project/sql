/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.apache.spark

import org.opensearch.flint.OpenSearchSuite

import org.apache.spark.sql.{DataFrame, ExplainSuiteHelper, QueryTest, Row}
import org.apache.spark.sql.catalyst.plans.logical.Filter
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2ScanRelation
import org.apache.spark.sql.execution.streaming.MemoryStream
import org.apache.spark.sql.flint.config.FlintSparkConf
import org.apache.spark.sql.functions.asc
import org.apache.spark.sql.streaming.{StreamingQuery, StreamTest}
import org.apache.spark.sql.types._
import org.apache.spark.util.Utils

/**
 * OpenSearch related integration test.
 */
class FlintDataSourceV2ITSuite
    extends QueryTest
    with StreamTest
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
        .options(openSearchOptions + (s"${FlintSparkConf.SCROLL_SIZE.key}" -> "1"))
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

  test("write dataframe to flint") {
    val indexName = "t0004"
    val mappings =
      """{
        |  "properties": {
        |    "aInt": {
        |      "type": "integer"
        |    }
        |  }
        |}""".stripMargin
    val options =
      openSearchOptions + (s"${FlintSparkConf.REFRESH_POLICY.key}" -> "wait_for",
      s"${FlintSparkConf.DOC_ID_COLUMN_NAME.key}" -> "aInt")
    Seq(Seq.empty, 1 to 14).foreach(data => {
      withIndexName(indexName) {
        index(indexName, oneNodeSetting, mappings, Seq.empty)
        if (data.nonEmpty) {
          data
            .toDF("aInt")
            .coalesce(1)
            .write
            .format("flint")
            .options(options)
            .mode("overwrite")
            .save(indexName)
        }

        val df = spark.range(15).toDF("aInt")
        df.coalesce(1)
          .write
          .format("flint")
          .options(options)
          .mode("overwrite")
          .save(indexName)

        val schema = StructType(Seq(StructField("aInt", IntegerType)))
        val dfResult1 = spark.sqlContext.read
          .format("flint")
          .options(options)
          .schema(schema)
          .load(indexName)
        checkAnswer(dfResult1, df)
      }
    })
  }

  test("write dataframe to flint with batch size configuration") {
    val indexName = "t0004"
    val options =
      openSearchOptions + (s"${FlintSparkConf.REFRESH_POLICY.key}" -> "wait_for",
      s"${FlintSparkConf.DOC_ID_COLUMN_NAME.key}" -> "aInt")
    Seq(0, 1).foreach(batchSize => {
      withIndexName(indexName) {
        val mappings =
          """{
            |  "properties": {
            |    "aInt": {
            |      "type": "integer"
            |    }
            |  }
            |}""".stripMargin
        index(indexName, oneNodeSetting, mappings, Seq.empty)

        val df = spark.range(15).toDF("aInt")
        df.coalesce(1)
          .write
          .format("flint")
          .options(options + ("spark.flint.write.batch.size" -> s"$batchSize"))
          .mode("overwrite")
          .save(indexName)

        val schema = StructType(Seq(StructField("aInt", IntegerType)))
        checkAnswer(
          spark.sqlContext.read
            .format("flint")
            .options(openSearchOptions)
            .schema(schema)
            .load(indexName),
          df)
      }
    })
  }

  test("streaming write to flint") {
    val indexName = "t0001"
    val inputData = MemoryStream[Int]
    val df = inputData.toDF().toDF("aInt")

    val checkpointDir = Utils.createTempDir(namePrefix = "stream.checkpoint").getCanonicalPath
    var query: StreamingQuery = null

    withIndexName(indexName) {
      val mappings =
        """{
          |  "properties": {
          |    "aInt": {
          |      "type": "integer"
          |    }
          |  }
          |}""".stripMargin
      index(indexName, oneNodeSetting, mappings, Seq.empty)

      try {
        query = df.writeStream
          .option("checkpointLocation", checkpointDir)
          .format("flint")
          .options(openSearchOptions)
          .option(s"${FlintSparkConf.REFRESH_POLICY.key}", "wait_for")
          .option(s"${FlintSparkConf.DOC_ID_COLUMN_NAME.key}", "aInt")
          .start(indexName)

        inputData.addData(1, 2, 3)

        failAfter(streamingTimeout) {
          query.processAllAvailable()
        }

        val outputDf = spark.sqlContext.read
          .format("flint")
          .options(openSearchOptions)
          .schema(StructType(Seq(StructField("aInt", IntegerType))))
          .load(indexName)
          .as[Int]
        checkDatasetUnorderly(outputDf, 1, 2, 3)

      } finally {
        if (query != null) {
          query.stop()
        }
      }
    }
  }

  test("read index with spark conf") {
    val indexName = "t0001"
    withIndexName(indexName) {
      simpleIndex(indexName)
      spark.conf.set(FlintSparkConf.sparkConf(FlintSparkConf.HOST_ENDPOINT.key), openSearchHost)
      spark.conf.set(FlintSparkConf.sparkConf(FlintSparkConf.HOST_PORT.key), openSearchPort)
      val schema = StructType(
        Seq(
          StructField("accountId", StringType, true),
          StructField("eventName", StringType, true),
          StructField("eventSource", StringType, true)))
      val df = spark.sqlContext.read
        .format("flint")
        .schema(schema)
        .load(indexName)

      assert(df.count() == 1)
      checkAnswer(df, Row("123", "event", "source"))
    }
  }

  test("datasource option should overwrite spark conf") {
    val indexName = "t0001"
    withIndexName(indexName) {
      simpleIndex(indexName)
      // set invalid host name and port which should be overwrite by datasource option.
      spark.conf.set(FlintSparkConf.sparkConf(FlintSparkConf.HOST_ENDPOINT.key), "invalid host")
      spark.conf.set(FlintSparkConf.sparkConf(FlintSparkConf.HOST_PORT.key), "0")

      val schema = StructType(
        Seq(
          StructField("accountId", StringType, true),
          StructField("eventName", StringType, true),
          StructField("eventSource", StringType, true)))
      val df = spark.sqlContext.read
        .format("flint")
        // override spark conf
        .options(openSearchOptions)
        .schema(schema)
        .load(indexName)

      assert(df.count() == 1)
      checkAnswer(df, Row("123", "event", "source"))
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
