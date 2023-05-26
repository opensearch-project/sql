/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark.skipping.partition

import org.opensearch.flint.spark.skipping.FlintSparkSkippingStrategy

import org.apache.spark.sql.Column
import org.apache.spark.sql.catalyst.expressions.aggregate.{AggregateFunction, First}

/**
 * Skipping strategy for partitioned columns of source table.
 */
class PartitionSkippingStrategy(
    override val kind: String = "partition",
    override val columnName: String,
    override val columnType: String)
    extends FlintSparkSkippingStrategy {

  override def outputSchema(): Map[String, String] = {
    Map(columnName -> convertToFlintType(columnType))
  }

  override def getAggregators: Seq[AggregateFunction] = {
    Seq(First(new Column(columnName).expr, ignoreNulls = true))
  }

  // TODO: move this mapping info to single place
  private def convertToFlintType(colType: String): String = {
    colType match {
      case "string" => "keyword"
      case "int" => "integer"
    }
  }
}
