/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark.skipping.partition

import org.opensearch.flint.spark.skipping.FlintSparkSkippingStrategy
import org.opensearch.flint.spark.skipping.FlintSparkSkippingStrategy.SkippingKind.{Partition, SkippingKind}

import org.apache.spark.sql.catalyst.expressions.{AttributeReference, EqualTo, Literal, Predicate}
import org.apache.spark.sql.catalyst.expressions.aggregate.{AggregateFunction, First}
import org.apache.spark.sql.functions.col

/**
 * Skipping strategy for partitioned columns of source table.
 */
case class PartitionSkippingStrategy(
    override val kind: SkippingKind = Partition,
    override val columnName: String,
    override val columnType: String)
    extends FlintSparkSkippingStrategy {

  override def outputSchema(): Map[String, String] = {
    Map(columnName -> columnType)
  }

  override def getAggregators: Seq[AggregateFunction] = {
    Seq(First(col(columnName).expr, ignoreNulls = true))
  }

  override def rewritePredicate(predicate: Predicate): Option[Predicate] = {
    // Column has same name in index data, so just rewrite to the same equation
    predicate.collect { case EqualTo(AttributeReference(`columnName`, _, _, _), value: Literal) =>
      convertToPredicate(col(columnName) === value)
    }.headOption
  }
}
