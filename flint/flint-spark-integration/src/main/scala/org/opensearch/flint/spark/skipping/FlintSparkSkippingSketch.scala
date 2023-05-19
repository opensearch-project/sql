/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark.skipping

import org.apache.spark.sql.catalog.Column
import org.apache.spark.sql.catalyst.expressions.aggregate.AggregateFunction

/**
 * Skipping index strategy that defines skipping data structure building and reading logic.
 */
trait FlintSparkSkippingSketch {

  /**
   * @return
   *   output schema mapping from Flint field name to Flint field type
   */
  def outputSchema(columns: Map[String, Column]): Map[String, String]

  /**
   * @return
   *   aggregators that generate skipping data structure
   */
  def getAggregators: Seq[AggregateFunction]
}
