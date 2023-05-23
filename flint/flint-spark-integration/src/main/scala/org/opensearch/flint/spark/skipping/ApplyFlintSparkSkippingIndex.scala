/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark.skipping

import org.json4s._
import org.json4s.native.JsonMethods._
import org.json4s.native.Serialization
import org.opensearch.flint.core.metadata.FlintMetadata
import org.opensearch.flint.spark.FlintSpark
import org.opensearch.flint.spark.skipping.partition.PartitionSkippingStrategy

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
          condition,
          relation @ LogicalRelation(
            baseRelation @ HadoopFsRelation(location, _, _, _, _, _),
            _,
            Some(table),
            false)) =>
      val indexName =
        FlintSparkSkippingIndex.getIndexName(table.identifier.table) // TODO: ignore schema name
      val index = flint.describeIndex(indexName)
      val indexedCols = parseMetadata(index)

      if (indexedCols.isEmpty) {
        filter
      } else {
        filter
      }
  }

  private def parseMetadata(index: Option[FlintMetadata]): Seq[FlintSparkSkippingStrategy] = {
    implicit val formats: Formats = Serialization.formats(NoTypeHints)

    if (index.isDefined) {

      // TODO: move all these JSON parsing to FlintMetadata once Flint spec finalized
      val json = parse(index.get.getContent)
      val kind = (json \ "_meta" \ "kind").extract[String]

      if (kind == "SkippingIndex") {
        val indexedColumns = (json \ "_meta" \ "indexedColumns").asInstanceOf[JArray]

        indexedColumns.arr.map { colInfo =>
          val kind = (colInfo \ "kind").extract[String]
          val columnName = (colInfo \ "columnName").extract[String]
          val columnType = (colInfo \ "columnType").extract[String]

          kind match {
            case "partition" =>
              new PartitionSkippingStrategy(columnName = columnName, columnType = columnType)
            case other =>
              throw new IllegalStateException(s"Unknown skipping strategy: $other")
          }
        }
      } else {
        Seq()
      }
    } else {
      Seq()
    }
  }
}
