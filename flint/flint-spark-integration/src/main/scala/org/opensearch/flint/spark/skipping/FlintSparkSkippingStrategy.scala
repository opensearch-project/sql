/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark.skipping

import org.apache.spark.sql.catalyst.expressions.Predicate

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

  /**
   * Rewrite a predicate (filtering condition) on source table into another predicate on index
   * data based on current skipping strategy.
   *
   * @param predicate
   *   filtering condition on source table
   * @return
   *   rewritten filtering condition on index data
   */
  def rewritePredicate(predicate: Predicate): Option[Predicate]
}
