/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark

import org.opensearch.flint.spark.sql.FlintSparkSqlParser

import org.apache.spark.sql.SparkSessionExtensions

/**
 * Flint Spark extension entrypoint.
 */
class FlintSparkExtensions extends (SparkSessionExtensions => Unit) {

  override def apply(extensions: SparkSessionExtensions): Unit = {
    extensions.injectParser { (spark, parser) =>
      new FlintSparkSqlParser(parser)
    }
    extensions.injectOptimizerRule { spark =>
      new FlintSparkOptimizer(spark)
    }
  }
}
