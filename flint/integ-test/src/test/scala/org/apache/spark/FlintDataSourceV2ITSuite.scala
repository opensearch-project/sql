/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.apache.spark

import java.sql.{Date, Timestamp}

import org.opensearch.flint.OpenSearchSuite

import org.apache.spark.sql.{DataFrame, ExplainSuiteHelper, QueryTest, Row}
import org.apache.spark.sql.catalyst.plans.logical.Filter
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2ScanRelation
import org.apache.spark.sql.execution.streaming.MemoryStream
import org.apache.spark.sql.flint.config.FlintSparkConf
import org.apache.spark.sql.flint.config.FlintSparkConf.{DOC_ID_COLUMN_NAME, IGNORE_DOC_ID_COLUMN}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.{StreamingQuery, StreamTest}
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

  test("create dataframe from flint datasource with provided schema ") {
    val indexName = "t0001"
    withIndexName(indexName) {
      simpleIndex(indexName)
      val df = spark.sqlContext.read
        .format("flint")
        .options(openSearchOptions)
        .schema("accountId STRING, eventName STRING, eventSource STRING")
        .load(indexName)

      assert(df.count() == 1)
      checkAnswer(df, Row("123", "event", "source"))
    }
  }

  test("scroll api test, force scroll_size = 1") {
    val indexName = "t0002"
    withIndexName(indexName) {
      multipleDocIndex(indexName, 5)

      val df = spark.sqlContext.read
        .format("flint")
        .options(openSearchOptions + (s"${FlintSparkConf.SCROLL_SIZE.optionKey}" -> "1"))
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

      val df = spark.sqlContext.read
        .format("flint")
        .options(openSearchOptions)
        .load(indexName)
        .select("aInt", "aString", "aText")

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
    val options = openSearchOptions + (s"${DOC_ID_COLUMN_NAME.optionKey}" -> "aInt")
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

        val dfResult1 = spark.sqlContext.read
          .format("flint")
          .options(options)
          .load(indexName)
        checkAnswer(dfResult1, df)
      }
    })
  }

  test("write dataframe to flint ignore id column") {
    val indexName = "t0002"
    val mappings =
      """{
        |  "properties": {
        |    "aString": {
        |      "type": "keyword"
        |    }
        |  }
        |}""".stripMargin
    val options =
      openSearchOptions + (s"${DOC_ID_COLUMN_NAME.optionKey}" -> "aInt",
      s"${IGNORE_DOC_ID_COLUMN.optionKey}" -> "true")
    withIndexName(indexName) {
      index(indexName, oneNodeSetting, mappings, Seq.empty)

      val df = spark.createDataFrame(Seq((1, "string1"), (2, "string2"))).toDF("aInt", "aString")

      df.coalesce(1)
        .write
        .format("flint")
        .options(options)
        .mode("overwrite")
        .save(indexName)

      val dfResult1 = spark.sqlContext.read
        .format("flint")
        .options(options)
        .load(indexName)
      checkAnswer(dfResult1, df.drop("aInt"))
    }
  }

  test("write dataframe to flint with batch size configuration") {
    val indexName = "t0004"
    val options = openSearchOptions + (s"${DOC_ID_COLUMN_NAME.optionKey}" -> "aInt")
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

        checkAnswer(
          spark.sqlContext.read
            .format("flint")
            .options(openSearchOptions)
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
          .option(s"${DOC_ID_COLUMN_NAME.optionKey}", "aInt")
          .start(indexName)

        inputData.addData(1, 2, 3)

        failAfter(streamingTimeout) {
          query.processAllAvailable()
        }

        val outputDf = spark.sqlContext.read
          .format("flint")
          .options(openSearchOptions)
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
      spark.conf.set(FlintSparkConf.HOST_ENDPOINT.key, openSearchHost)
      spark.conf.set(FlintSparkConf.HOST_PORT.key, openSearchPort)

      val df = spark.sqlContext.read
        .format("flint")
        .load(indexName)
        .select("accountId", "eventName", "eventSource")

      assert(df.count() == 1)
      checkAnswer(df, Row("123", "event", "source"))
    }
  }

  test("datasource option should overwrite spark conf") {
    val indexName = "t0001"
    withIndexName(indexName) {
      simpleIndex(indexName)
      // set invalid host name and port which should be overwrite by datasource option.
      spark.conf.set(FlintSparkConf.HOST_ENDPOINT.key, "invalid host")
      spark.conf.set(FlintSparkConf.HOST_PORT.key, "0")

      val df = spark.sqlContext.read
        .format("flint")
        // override spark conf
        .options(openSearchOptions)
        .load(indexName)
        .select("accountId", "eventName", "eventSource")

      assert(df.count() == 1)
      checkAnswer(df, Row("123", "event", "source"))
    }
  }

  test("load and save date and timestamp type field") {
    val indexName = "t0001"
    Seq(
      """{
          |  "properties": {
          |    "aDate": {
          |      "type": "date",
          |      "format": "strict_date"
          |    },
          |    "aTimestamp": {
          |      "type": "date"
          |    }
          |  }
          |}""".stripMargin,
      """{
          |  "properties": {
          |    "aDate": {
          |      "type": "date",
          |      "format": "strict_date"
          |    },
          |    "aTimestamp": {
          |      "type": "date",
          |      "format": "strict_date_optional_time_nanos"
          |    }
          |  }
          |}""".stripMargin).foreach(mapping => {
      withIndexName(indexName) {
        index(indexName, oneNodeSetting, mapping, Seq.empty)

        val df = spark
          .range(1)
          .select(current_date().as("aDate"), current_timestamp().as("aTimestamp"))
          .cache()

        df.coalesce(1)
          .write
          .format("flint")
          .options(openSearchOptions)
          .mode("overwrite")
          .save(indexName)

        val dfResult1 = spark.sqlContext.read
          .format("flint")
          .options(openSearchOptions)
          .load(indexName)
        checkAnswer(dfResult1, df)
      }
    })
  }

  test("load timestamp field in epoch format") {
    val indexName = "t0001"
    Seq(
      """{
        |  "properties": {
        |    "aTimestamp": {
        |      "type": "date"
        |    }
        |  }
        |}""".stripMargin,
      """{
        |  "properties": {
        |    "aTimestamp": {
        |      "type": "date",
        |      "format": "epoch_millis"
        |    }
        |  }
        |}""".stripMargin).foreach(mapping => {
      withIndexName(indexName) {
        val docs = Seq("""{
            |  "aTimestamp": 1420070400000
            |}""".stripMargin)
        index(indexName, oneNodeSetting, mapping, docs)

        val df = spark.sqlContext.read
          .format("flint")
          .options(openSearchOptions)
          .load(indexName)
        checkAnswer(df, Row(Timestamp.valueOf("2014-12-31 16:00:00")))
      }
    })
  }

  // scalastyle:off
  /**
   * More reading at.
   * https://www.databricks.com/blog/2020/07/22/a-comprehensive-look-at-dates-and-timestamps-in-apache-spark-3-0.html
   */
  // scalastyle:on
  test("load timestamp using session timeZone conf") {
    val indexName = "t0001"
    Seq(("UTC", "2023-06-01 08:30:00 UTC"), ("PST", "2023-06-01 01:30:00 PST")).foreach {
      case (timezone, timestampStr) =>
        withIndexName(indexName) {
          val mapping =
            """{
              |  "properties": {
              |    "aTimestamp": {
              |      "type": "date",
              |      "format": "strict_date_optional_time_nanos"
              |    }
              |  }
              |}""".stripMargin

          /**
           * store 2 same timestamp represent with different timezone in OpenSearch
           */
          val docs = Seq(
            """{"aTimestamp": "2023-06-01T01:30:00.000000-0700"}""",
            """{"aTimestamp": "2023-06-01T08:30:00.000000+0000"}""")
          index(indexName, oneNodeSetting, mapping, docs)

          spark.conf.set("spark.sql.session.timeZone", timezone)
          val dfResult1 = spark.sqlContext.read
            .format("flint")
            .options(openSearchOptions)
            .load(indexName)
          checkAnswer(
            dfResult1,
            Seq(timestampStr, timestampStr)
              .toDF("aTimestamp")
              .select(to_timestamp($"aTimestamp", "yyyy-MM-dd HH:mm:ss z").as("aTimestamp")))
        }
    }
  }

  test("scan with date filter push-down") {
    val indexName = "t0001"
    withIndexName(indexName) {
      val mappings = """{
                       |  "properties": {
                       |    "aDate": {
                       |      "type": "date",
                       |      "format": "strict_date"
                       |    },
                       |    "aTimestamp": {
                       |      "type": "date",
                       |      "format": "strict_date_optional_time_nanos"
                       |    }
                       |  }
                       |}""".stripMargin
      index(indexName, oneNodeSetting, mappings, Seq.empty)

      spark.conf.set("spark.sql.session.timeZone", "UTC")

      val df =
        Seq(("2023-05-01", "2023-05-01 12:30:00 UTC"), ("2023-06-01", "2023-06-01 12:30:00 UTC"))
          .toDF("aDate", "aTimestamp")
          .select(
            to_date($"aDate").as("aDate"),
            to_timestamp($"aTimestamp", "yyyy-MM-dd HH:mm:ss z").as("aTimestamp"))
          .cache()
      df.coalesce(1)
        .write
        .format("flint")
        .options(openSearchOptions)
        .mode("overwrite")
        .save(indexName)

      val dfRead = spark.sqlContext.read
        .format("flint")
        .options(openSearchOptions)
        .load(indexName)

      val dfResult1 = dfRead.filter("aDate < date'2023-06-01'").select("aDate")
      checkFiltersRemoved(dfResult1)
      checkPushedInfo(dfResult1, "PushedPredicates: [aDate IS NOT NULL, aDate < 19509]")
      checkAnswer(dfResult1, Row(Date.valueOf("2023-05-01")))

      val dfResult2 = dfRead
        .filter("aTimestamp < timestamp'2023-06-01 12:29:59 UTC'")
        .select("aTimestamp")
      checkFiltersRemoved(dfResult2)
      checkPushedInfo(
        dfResult2,
        "PushedPredicates: [aTimestamp IS NOT NULL, aTimestamp < 1685622599000000]")
      checkAnswer(
        dfResult2,
        Seq("2023-05-01 12:30:00 UTC")
          .toDF("aTimestamp")
          .select(to_timestamp($"aTimestamp", "yyyy-MM-dd HH:mm:ss z").as("aTimestamp")))
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
