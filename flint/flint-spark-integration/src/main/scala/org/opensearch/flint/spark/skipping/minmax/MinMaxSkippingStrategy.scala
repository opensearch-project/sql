/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark.skipping.minmax

import org.opensearch.flint.spark.skipping.FlintSparkSkippingStrategy
import org.opensearch.flint.spark.skipping.FlintSparkSkippingStrategy.SkippingKind.{MinMax, SkippingKind}

import org.apache.spark.sql.catalyst.expressions.{AttributeReference, EqualTo, GreaterThan, GreaterThanOrEqual, In, LessThan, LessThanOrEqual, Literal, Or, Predicate}
import org.apache.spark.sql.catalyst.expressions.aggregate.{AggregateFunction, Max, Min}
import org.apache.spark.sql.functions.col

/**
 * Skipping strategy based on min-max boundary of column values.
 */
case class MinMaxSkippingStrategy(
    override val kind: SkippingKind = MinMax,
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
    predicate.collect {
      case EqualTo(AttributeReference(`columnName`, _, _, _), value: Literal) =>
        convertToPredicate(col(minColName) <= value && col(maxColName) >= value)
      case LessThan(AttributeReference(`columnName`, _, _, _), value: Literal) =>
        convertToPredicate(col(minColName) < value)
      case LessThanOrEqual(AttributeReference(`columnName`, _, _, _), value: Literal) =>
        convertToPredicate(col(minColName) <= value)
      case GreaterThan(AttributeReference(`columnName`, _, _, _), value: Literal) =>
        convertToPredicate(col(maxColName) > value)
      case GreaterThanOrEqual(AttributeReference(`columnName`, _, _, _), value: Literal) =>
        convertToPredicate(col(maxColName) >= value)
      case In(AttributeReference(`columnName`, _, _, _), values: Seq[Literal]) =>
        values
          .map(value => convertToPredicate(col(minColName) <= value && col(maxColName) >= value))
          .reduceLeft(Or)
    }.headOption
}
