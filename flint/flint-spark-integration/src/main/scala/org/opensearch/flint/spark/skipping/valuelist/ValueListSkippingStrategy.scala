/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark.skipping.valuelist

import org.opensearch.flint.spark.skipping.FlintSparkSkippingStrategy

import org.apache.spark.sql.catalyst.expressions.{AttributeReference, EqualTo, Literal, Predicate}
import org.apache.spark.sql.catalyst.expressions.aggregate.{AggregateFunction, CollectSet}
import org.apache.spark.sql.functions.col

/**
 * Skipping strategy based on complete column value list.
 */
class ValueListSkippingStrategy(
    override val kind: String = "value_list", // TODO: enum
    override val columnName: String,
    override val columnType: String)
    extends FlintSparkSkippingStrategy {

  override def outputSchema(): Map[String, String] =
    Map(columnName -> columnType)

  override def getAggregators: Seq[AggregateFunction] =
    Seq(CollectSet(col(columnName).expr)) // TODO: change others to use col instead of new Column

  override def rewritePredicate(predicate: Predicate): Option[Predicate] =
    predicate.collect { case EqualTo(AttributeReference(`columnName`, _, _, _), value: Literal) =>
      EqualTo(
        col(columnName).expr,
        value
      ) // TODO: what's the difference between Column and UnresolvedAttribute
    }.headOption
}
