/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.apache.spark.sql.flint.config

import scala.collection.JavaConverters._

import org.apache.spark.FlintSuite

class FlintSparkConfSuite extends FlintSuite {
  test("test spark conf") {
    spark.conf.set("spark.datasource.flint.host", "127.0.0.1")
    spark.conf.set("spark.datasource.flint.read.scroll_size", "10")

    val flintOptions = FlintSparkConf(spark.conf)
    assert(flintOptions.getHost == "127.0.0.1")
    assert(flintOptions.getScrollSize == 10)

    // default value
    assert(flintOptions.getPort == 9200)
    assert(flintOptions.getRefreshPolicy == "false")
  }

  test("test spark options") {
    val options = FlintSparkConf(Map("write.batch_size" -> "10", "write.id_name" -> "id").asJava)
    assert(options.batchSize() == 10)
    assert(options.docIdColumnName().isDefined)
    assert(options.docIdColumnName().get == "id")

    // default value
    assert(options.flintOptions().getHost == "localhost")
    assert(options.flintOptions().getPort == 9200)
  }
}
