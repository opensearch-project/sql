/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark.skipping.valueset

import org.opensearch.flint.spark.skipping.FlintSparkSkippingStrategy
import org.opensearch.flint.spark.skipping.FlintSparkSkippingStrategy.SkippingKind.{SkippingKind, VALUE_SET}

import org.apache.spark.sql.catalyst.expressions.{AttributeReference, EqualTo, Literal, Predicate}
import org.apache.spark.sql.catalyst.expressions.aggregate.{AggregateFunction, CollectSet}
import org.apache.spark.sql.functions.col

/**
 * Skipping strategy based on unique column value set.
 */
case class ValueSetSkippingStrategy(
    override val kind: SkippingKind = VALUE_SET,
    override val columnName: String,
    override val columnType: String)
    extends FlintSparkSkippingStrategy {

  override def outputSchema(): Map[String, String] =
    Map(columnName -> columnType)

  override def getAggregators: Seq[AggregateFunction] =
    Seq(CollectSet(col(columnName).expr))

  override def rewritePredicate(predicate: Predicate): Option[Predicate] = {
    /*
     * This is supposed to be rewritten to ARRAY_CONTAINS(columName, value).
     * However, due to push down limitation in Spark, we keep the equation.
     */
    predicate.collect { case EqualTo(AttributeReference(`columnName`, _, _, _), value: Literal) =>
      EqualTo(col(columnName).expr, value)
    }.headOption
  }
}
