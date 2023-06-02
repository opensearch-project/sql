/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark.skipping.partition

import org.opensearch.flint.spark.skipping.FlintSparkSkippingStrategy

import org.apache.spark.sql.Column
import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, EqualTo, Literal, Predicate}
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
    Map(columnName -> columnType)
  }

  override def getAggregators: Seq[AggregateFunction] = {
    Seq(First(new Column(columnName).expr, ignoreNulls = true))
  }

  override def rewritePredicate(predicate: Predicate): Option[Predicate] = {
    // Column has same name in index data, so just rewrite to the same equation
    predicate.collect {
      case EqualTo(AttributeReference(`columnName`, _, _, _), value: Literal) =>
        EqualTo(UnresolvedAttribute(columnName), value)
    }.headOption
  }
}
