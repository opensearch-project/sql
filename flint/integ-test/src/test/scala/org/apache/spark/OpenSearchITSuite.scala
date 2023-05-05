/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.apache.spark

import org.apache.spark.sql.{QueryTest, Row}
import org.apache.spark.sql.types.{StringType, StructField, StructType}

/**
 * OpenSearch related integration test.
 */
class OpenSearchITSuite
    extends QueryTest
    with FlintSuite
    with OpenSearchSuite
    with OpenSearchIndex {

  /**
   * Todo. could be deleted in future. for demo purpose now.
   */
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
}
