/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark

import com.stephenn.scalatest.jsonassert.JsonMatchers.matchJson
import org.apache.spark.FlintSuite
import org.opensearch.flint.OpenSearchSuite
import org.opensearch.flint.spark.skipping.FlintSparkSkippingIndex
import org.scalatest.matchers.must.Matchers.defined
import org.scalatest.matchers.should.Matchers.convertToAnyShouldWrapper

import scala.Option._

class FlintSparkSuite extends FlintSuite with OpenSearchSuite {

  lazy val flint: FlintSpark = {
    spark.conf.set(FlintSpark.FLINT_INDEX_STORE_LOCATION, openSearchHost)
    spark.conf.set(FlintSpark.FLINT_INDEX_STORE_PORT, openSearchPort)
    new FlintSpark(spark)
  }

  test("create index and describe it") {
    val index = new FlintSparkSkippingIndex("test")
    flint.createIndex(index)

    val metadata = flint.describeIndex(index.name())
    metadata shouldBe defined
    metadata.get.getContent should matchJson(index.metadata().getContent)
  }

  test("describe non-exist index should return empty") {
    flint.describeIndex("non-exist") shouldBe empty
  }
}
