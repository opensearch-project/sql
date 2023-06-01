/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark.skipping

import java.util.Locale

import org.opensearch.flint.spark.FlintSpark
import org.opensearch.flint.spark.skipping.FlintSparkSkippingIndex.{getSkippingIndexName, FILE_PATH_COLUMN, SKIPPING_INDEX_TYPE}

import org.apache.spark.internal.Logging
import org.apache.spark.sql.Column
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
class ApplyFlintSparkSkippingIndex(flint: FlintSpark) extends Rule[LogicalPlan] with Logging {

  override def apply(plan: LogicalPlan): LogicalPlan = plan transform {
    case filter @ Filter(
          condition: Predicate,
          relation @ LogicalRelation(
            baseRelation @ HadoopFsRelation(location, _, _, _, _, _),
            _,
            Some(table),
            false)) =>
      // Exit if plan is already rewritten with skipping index
      if (location.isInstanceOf[FlintSparkSkippingFileIndex]) {
        return plan
      }

      val indexName = getSkippingIndexName(table.identifier.table) // TODO: ignore database name
      val index = flint.describeIndex(indexName)
      logDebug(s"Checking skipping index $indexName result: $index")

      if (index.exists(_.kind == SKIPPING_INDEX_TYPE)) {
        val skippingIndex = index.get.asInstanceOf[FlintSparkSkippingIndex]
        val indexPred = rewriteToIndexPredicate(skippingIndex, condition)
        logDebug(s"Predicate after rewrite: $indexPred")

        /*
         * Replace original file index with Flint skipping file index:
         *  Filter(a=v)
         *   |- LogicalRelation(A)
         *      |- HadoopFsRelation
         *         |- XFileIndex... <-- replaced with FlintSkippingFileIndex(
         *                                SELECT file_path
         *                                FROM flint_A_skipping_index
         *                                WHERE rewrite(a=v))
         */
        if (indexPred.isDefined) {
          val selectedFiles = selectFilesByIndex(skippingIndex, indexPred.get)
          logDebug(s"Selected files from skipping index: $selectedFiles")

          val fileIndex = new FlintSparkSkippingFileIndex(location, selectedFiles)
          val indexRelation = baseRelation.copy(location = fileIndex)(baseRelation.sparkSession)
          filter.copy(child = relation.copy(relation = indexRelation))
        } else {
          filter // No applicable index for the filtering condition
        }
      } else {
        filter // No index found for the source table
      }
  }

  private def rewriteToIndexPredicate(
      index: FlintSparkSkippingIndex,
      condition: Predicate): Option[Predicate] = {

    // Assume the given condition is conjunction (one or more concatenated by AND only)
    // Let each skipping strategy rewrite the predicate on source table
    // to a new predicate on index data, if applicable
    index.indexedColumns
      .flatMap(index => index.rewritePredicate(condition))
      .reduceOption(And(_, _))
  }

  private def selectFilesByIndex(
      index: FlintSparkSkippingIndex,
      rewrittenPredicate: Predicate): Set[String] = {

    // Get file list based on the rewritten predicates on index data
    flint.spark.read
      .format(FLINT_DATASOURCE)
      .schema(getSchema(index.indexedColumns))
      .load(index.name())
      .filter(new Column(rewrittenPredicate))
      .select(FILE_PATH_COLUMN)
      .collect
      .map(_.getString(0))
      .toSet
  }

  private def getSchema(indexCols: Seq[FlintSparkSkippingStrategy]): String = {
    val colTypes =
      indexCols
        .flatMap(_.outputSchema())
        .map { case (name, colType) => s"$name ${colType.toUpperCase(Locale.ROOT)}" }

    val allColTypes = colTypes :+ s"$FILE_PATH_COLUMN STRING"
    allColTypes.mkString(", ")
  }
}
