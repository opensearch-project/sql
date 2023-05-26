/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark.skipping

import org.json4s._
import org.json4s.native.Serialization
import org.opensearch.flint.core.metadata.FlintMetadata
import org.opensearch.flint.spark.FlintSparkIndex
import org.opensearch.flint.spark.skipping.FlintSparkSkippingIndex.{getSkippingIndexName, FILE_PATH_COLUMN, SKIPPING_INDEX_TYPE}

import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.catalyst.dsl.expressions.DslExpression
import org.apache.spark.sql.functions.input_file_name

/**
 * Flint skipping index in Spark.
 *
 * @param tableName
 *   source table name
 */
class FlintSparkSkippingIndex(tableName: String, indexedColumns: Seq[FlintSparkSkippingStrategy])
    extends FlintSparkIndex {

  /** Required by json4s write function */
  implicit val formats: Formats = Serialization.formats(NoTypeHints)

  /** Skipping index type */
  override val kind: String = SKIPPING_INDEX_TYPE

  /** Output schema of the skipping index */
  private val outputSchema: Map[String, String] = {
    val schema = indexedColumns
      .flatMap(_.outputSchema().toList)
      .toMap

    schema + (FILE_PATH_COLUMN -> "keyword")
  }

  override def name(): String = {
    getSkippingIndexName(tableName)
  }

  override def metadata(): FlintMetadata = {
    new FlintMetadata(s"""{
        |   "_meta": {
        |     "kind": "$SKIPPING_INDEX_TYPE",
        |     "indexedColumns": $getMetaInfo,
        |     "source": "$tableName"
        |   },
        |   "properties": $getSchema
        | }
        |""".stripMargin)
  }

  override def build(df: DataFrame): DataFrame = {
    val outputNames = indexedColumns.flatMap(_.outputSchema().keys)
    val aggFuncs = indexedColumns.flatMap(_.getAggregators)

    // Wrap aggregate function with output column name
    val namedAggFuncs =
      (outputNames, aggFuncs).zipped.map { case (name, aggFunc) =>
        new Column(aggFunc.toAggregateExpression().as(name))
      }

    df.groupBy(input_file_name().as(FILE_PATH_COLUMN))
      .agg(namedAggFuncs.head, namedAggFuncs.tail: _*)
  }

  private def getMetaInfo: String = {
    Serialization.write(indexedColumns)
  }

  private def getSchema: String = {
    Serialization.write(outputSchema.map { case (colName, colType) =>
      colName -> ("type" -> colType)
    })
  }
}

object FlintSparkSkippingIndex {

  /** Index type name */
  val SKIPPING_INDEX_TYPE = "skipping"

  /** File path column name */
  val FILE_PATH_COLUMN = "file_path"

  /**
   * Get skipping index name which follows the convention: "flint_" prefix + source table name +
   * "_skipping_index" suffix.
   *
   * This helps identify the Flint index because Flint index is not registered to Spark Catalog
   * for now.
   *
   * @param tableName
   *   source table name
   * @return
   *   Flint skipping index name
   */
  def getSkippingIndexName(tableName: String): String = s"flint_${tableName}_skipping_index"
}
