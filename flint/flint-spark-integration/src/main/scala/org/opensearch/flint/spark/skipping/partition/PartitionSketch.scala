/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark.skipping.partition

import org.opensearch.flint.spark.skipping.FlintSparkSkippingSketch

import org.apache.spark.sql.catalog.Column
import org.apache.spark.sql.catalyst.expressions.aggregate.AggregateFunction

/**
 * Skipping strategy for partitioned columns of source table.
 */
class PartitionSketch extends FlintSparkSkippingSketch {

  override def outputSchema(columns: Map[String, Column]): Map[String, String] = {
    columns.values
      .filter(col => col.isPartition)
      .map(col => col.name -> convertToFlintType(col.dataType))
      .toMap
  }

  override def getAggregators: Seq[AggregateFunction] =
    Seq() // TODO: will do separate with pending FlintSpark.createIndex

  // TODO: move this mapping info to single place
  private def convertToFlintType(colType: String): String = {
    colType match {
      case "string" => "keyword"
      case "int" => "integer"
    }
  }
}
