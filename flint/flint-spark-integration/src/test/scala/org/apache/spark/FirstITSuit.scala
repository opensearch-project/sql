/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.apache.spark

import org.apache.spark.sql.{QueryTest, Row}
import org.apache.spark.sql.functions.asc
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

class FirstITSuit extends QueryTest with FlintSuite with OpenSearchSuite with OpenSearchIndex {
  test("basic flint read test") {
    val indexName = "t0001"
    withIndexName(indexName) {
      simpleIndex(indexName)
      val schema = StructType(
        Array(
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
}
