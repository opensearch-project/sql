/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark.skipping.partition

import org.opensearch.flint.spark.skipping.FlintSparkSkippingStrategy

/**
 * Skipping strategy for partitioned columns of source table.
 */
class PartitionSkippingStrategy(override val indexedColumn: (String, String))
    extends FlintSparkSkippingStrategy {

  override def outputSchema(): Map[String, String] = {
    Map(indexedColumn._1 -> convertToFlintType(indexedColumn._2))
  }

  // TODO: move this mapping info to single place
  private def convertToFlintType(colType: String): String = {
    colType match {
      case "string" => "keyword"
      case "int" => "integer"
    }
  }
}
