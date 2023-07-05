/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark.skipping.minmax

import org.opensearch.flint.spark.skipping.FlintSparkSkippingStrategy
import org.opensearch.flint.spark.skipping.FlintSparkSkippingStrategy.SkippingKind.{MIN_MAX, SkippingKind}

import org.apache.spark.sql.Column
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, EqualTo, Literal, Predicate}
import org.apache.spark.sql.catalyst.expressions.aggregate.{AggregateFunction, Max, Min}
import org.apache.spark.sql.functions.col

/**
 * Skipping strategy based on min-max boundary of column values.
 */
case class MinMaxSkippingStrategy(
    override val kind: SkippingKind = MIN_MAX,
    override val columnName: String,
    override val columnType: String)
    extends FlintSparkSkippingStrategy {

  /** Column name in Flint index data. */
  private def minColName = s"MinMax_${columnName}_0"
  private def maxColName = s"MinMax_${columnName}_1"

  override def outputSchema(): Map[String, String] =
    Map(minColName -> columnType, maxColName -> columnType)

  override def getAggregators: Seq[AggregateFunction] =
    Seq(Min(col(columnName).expr), Max(col(columnName).expr))

  override def rewritePredicate(predicate: Predicate): Option[Predicate] =
    predicate.collect { case EqualTo(AttributeReference(`columnName`, _, _, _), value: Literal) =>
      rewriteTo(col(minColName) <= value && col(maxColName) >= value)
    }.headOption

  // Convert a column to predicate
  private def rewriteTo(col: Column): Predicate = col.expr.asInstanceOf[Predicate]
}
