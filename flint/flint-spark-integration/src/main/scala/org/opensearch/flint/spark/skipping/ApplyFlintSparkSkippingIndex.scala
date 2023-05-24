/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark.skipping

import org.opensearch.flint.spark.FlintSpark
import org.opensearch.flint.spark.skipping.FlintSparkSkippingIndex.{getSkippingIndexName, FILE_PATH_COLUMN, SKIPPING_INDEX_TYPE}

import org.apache.spark.sql.Column
import org.apache.spark.sql.catalyst.expressions.{And, Predicate}
import org.apache.spark.sql.catalyst.plans.logical.{Filter, LogicalPlan}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.datasources.{HadoopFsRelation, LogicalRelation}

/**
 * Flint Spark skipping index apply rule that rewrites applicable query's filtering condition and
 * table scan operator to leverage additional skipping data structure and accelerate query by
 * reducing data scanned significantly.
 *
 * @param flint
 *   Flint Spark API
 */
class ApplyFlintSparkSkippingIndex(val flint: FlintSpark) extends Rule[LogicalPlan] {

  override def apply(plan: LogicalPlan): LogicalPlan = plan transform {
    case filter @ Filter(
          condition: Predicate,
          relation @ LogicalRelation(
            baseRelation @ HadoopFsRelation(location, _, _, _, _, _),
            _,
            Some(table),
            false)) =>
      // Spark optimize recursively
      // if (location.isInstanceOf[FlintSparkSkippingFileIndex]) {
      //  return filter
      // }

      val indexName = getSkippingIndexName(table.identifier.table) // TODO: ignore schema name
      val index = flint.describeIndex(indexName)

      if (index.exists(_.kind == SKIPPING_INDEX_TYPE)) {
        val skippingIndex = index.get.asInstanceOf[FlintSparkSkippingIndex]
        val rewrittenPredicate = rewriteToPredicateOnSkippingIndex(skippingIndex, condition)
        val selectedFiles = getSelectedFilesToScanAfterSkip(skippingIndex, rewrittenPredicate)

        filter
      } else {
        filter
      }
  }

  private def rewriteToPredicateOnSkippingIndex(
      index: FlintSparkSkippingIndex,
      condition: Predicate): Predicate = {

    index.indexedColumns
      .map(index => index.rewritePredicate(condition))
      .filter(pred => pred.isDefined)
      .map(pred => pred.get)
      .reduce(And(_, _))
  }

  private def getSelectedFilesToScanAfterSkip(
      index: FlintSparkSkippingIndex,
      rewrittenPredicate: Predicate): Set[String] = {

    index
      .query()
      .filter(new Column(rewrittenPredicate))
      .select(FILE_PATH_COLUMN)
      .collect
      .map(_.getString(0))
      .toSet
  }
}
