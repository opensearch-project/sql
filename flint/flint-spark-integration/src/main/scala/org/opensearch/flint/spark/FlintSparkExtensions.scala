/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark

import org.opensearch.flint.spark.skipping.ApplyFlintSparkSkippingIndex

import org.apache.spark.sql.SparkSessionExtensions

/**
 * Flint Spark extension entrypoint.
 */
class FlintSparkExtensions extends (SparkSessionExtensions => Unit) {

  override def apply(extensions: SparkSessionExtensions): Unit = {
    // Add skipping index query rewrite rule
    extensions.injectOptimizerRule { spark =>
      new ApplyFlintSparkSkippingIndex(new FlintSpark(spark))
    }
  }
}
