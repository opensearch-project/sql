/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark.skipping

import org.mockito.Mockito.when
import org.opensearch.flint.spark.FlintSparkIndex.ID_COLUMN
import org.opensearch.flint.spark.skipping.FlintSparkSkippingIndex.FILE_PATH_COLUMN
import org.scalatest.matchers.must.Matchers.contain
import org.scalatest.matchers.should.Matchers.convertToAnyShouldWrapper
import org.scalatestplus.mockito.MockitoSugar.mock

import org.apache.spark.FlintSuite
import org.apache.spark.sql.catalyst.expressions.aggregate.CollectSet
import org.apache.spark.sql.functions.col

class FlintSparkSkippingIndexSuite extends FlintSuite {

  test("get skipping index name") {
    val index = new FlintSparkSkippingIndex("test", Seq(mock[FlintSparkSkippingStrategy]))
    index.name() shouldBe "flint_test_skipping_index"
  }

  test("can build index building job with unique ID column") {
    val indexCol = mock[FlintSparkSkippingStrategy]
    when(indexCol.outputSchema()).thenReturn(Map("name" -> "string"))
    when(indexCol.getAggregators).thenReturn(Seq(CollectSet(col("name").expr)))
    val index = new FlintSparkSkippingIndex("test", Seq(indexCol))

    val df = spark.createDataFrame(Seq(("hello", 20))).toDF("name", "age")
    val indexDf = index.build(df)
    indexDf.schema.fieldNames should contain only ("name", FILE_PATH_COLUMN, ID_COLUMN)
  }

  test("should fail if no indexed column given") {
    assertThrows[IllegalArgumentException] {
      new FlintSparkSkippingIndex("test", Seq.empty)
    }
  }
}
