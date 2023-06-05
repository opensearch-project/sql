/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark.skipping

import org.opensearch.flint.spark.FlintSpark
import org.opensearch.flint.spark.skipping.FlintSparkSkippingIndex.{getSkippingIndexName, FILE_PATH_COLUMN, SKIPPING_INDEX_TYPE}

import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.catalyst.expressions.{And, Predicate}
import org.apache.spark.sql.catalyst.plans.logical.{Filter, LogicalPlan}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.datasources.{HadoopFsRelation, LogicalRelation}
import org.apache.spark.sql.flint.FlintDataSourceV2.FLINT_DATASOURCE

/**
 * Flint Spark skipping index apply rule that rewrites applicable query's filtering condition and
 * table scan operator to leverage additional skipping data structure and accelerate query by
 * reducing data scanned significantly.
 *
 * @param flint
 *   Flint Spark API
 */
class ApplyFlintSparkSkippingIndex(flint: FlintSpark) extends Rule[LogicalPlan] {

  override def apply(plan: LogicalPlan): LogicalPlan = plan transform {
    case filter @ Filter( // TODO: abstract pattern match logic for different table support
          condition: Predicate,
          relation @ LogicalRelation(
            baseRelation @ HadoopFsRelation(location, _, _, _, _, _),
            _,
            Some(table),
            false)) if !location.isInstanceOf[FlintSparkSkippingFileIndex] =>

      val indexName = getSkippingIndexName(table.identifier.table) // TODO: database name
      val index = flint.describeIndex(indexName)
      if (index.exists(_.kind == SKIPPING_INDEX_TYPE)) {
        val skippingIndex = index.get.asInstanceOf[FlintSparkSkippingIndex]
        val indexPred = rewriteToIndexPredicate(skippingIndex, condition)

        /*
         * Replace original file index with Flint skipping file index:
         *  Filter(a=b)
         *  |- LogicalRelation(A)
         *     |- HadoopFsRelation
         *        |- FileIndex <== replaced with FlintSkippingFileIndex
         */
        if (indexPred.isDefined) {
          val filterByIndex = buildFilterIndexQuery(skippingIndex, indexPred.get)
          val fileIndex = new FlintSparkSkippingFileIndex(location, filterByIndex)
          val indexRelation = baseRelation.copy(location = fileIndex)(baseRelation.sparkSession)
          filter.copy(child = relation.copy(relation = indexRelation))
        } else {
          filter
        }
      } else {
        filter
      }
  }

  private def rewriteToIndexPredicate(
      index: FlintSparkSkippingIndex,
      condition: Predicate): Option[Predicate] = {

    // TODO: currently only handle conjunction, namely the given condition is consist of
    //  one or more expression concatenated by AND only.
    index.indexedColumns
      .flatMap(index => index.rewritePredicate(condition))
      .reduceOption(And(_, _))
  }

  private def buildFilterIndexQuery(
      index: FlintSparkSkippingIndex,
      rewrittenPredicate: Predicate): DataFrame = {

    // Get file list based on the rewritten predicates on index data
    flint.spark.read
      .format(FLINT_DATASOURCE)
      .load(index.name())
      .filter(new Column(rewrittenPredicate))
      .select(FILE_PATH_COLUMN)
  }
}
