/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark.skipping

/**
 * Skipping index strategy that defines skipping data structure building and reading logic.
 */
trait FlintSparkSkippingStrategy {

  /**
   * Skipping strategy kind.
   */
  val kind: String

  /**
   * Indexed column name and its Spark SQL type.
   */
  val columnName: String
  val columnType: String

  /**
   * @return
   *   output schema mapping from Flint field name to Flint field type
   */
  def outputSchema(): Map[String, String]
}
