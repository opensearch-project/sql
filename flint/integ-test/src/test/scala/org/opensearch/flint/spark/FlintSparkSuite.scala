/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark

import org.apache.spark.FlintSuite
import org.opensearch.flint.OpenSearchSuite
import org.scalatest.matchers.should.Matchers.convertToAnyShouldWrapper

class FlintSparkSuite extends FlintSuite with OpenSearchSuite {

  lazy val flint: FlintSpark = {
    spark.conf.set(FlintSpark.FLINT_INDEX_STORE_LOCATION, openSearchHost)
    spark.conf.set(FlintSpark.FLINT_INDEX_STORE_PORT, openSearchPort)
    new FlintSpark(spark)
  }

  test("describe index") {
    flint.describeIndex("test") shouldBe Option.empty
  }
}
