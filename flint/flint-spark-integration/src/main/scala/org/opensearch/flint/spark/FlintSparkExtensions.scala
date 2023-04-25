/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark

import org.apache.spark.sql.SparkSessionExtensions

class FlintSparkExtensions extends (SparkSessionExtensions => Unit) {

  override def apply(v1: SparkSessionExtensions): Unit = {
  }
}
