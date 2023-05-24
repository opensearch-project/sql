/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark.skipping.partition

import org.opensearch.flint.spark.skipping.FlintSparkSkippingStrategy

import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference, EqualTo, Expression, Literal, Predicate}

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

  // TODO: move this mapping info to single place
  private def convertToFlintType(colType: String): String = {
    colType match {
      case "string" => "keyword"
      case "int" => "integer"
    }
  }

  override def rewritePredicate(predicate: Predicate): Option[Predicate] = {
    val newPred = predicate.collect {
      case EqualTo(AttributeReference(columnName, _, _, _), value: Literal) =>
        EqualTo(UnresolvedAttribute(columnName), value)
    }
    newPred.headOption
  }
}
