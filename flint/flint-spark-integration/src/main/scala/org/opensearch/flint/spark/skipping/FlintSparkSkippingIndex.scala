/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark.skipping

import org.json4s._
import org.json4s.native.Serialization
import org.opensearch.flint.core.metadata.FlintMetadata
import org.opensearch.flint.spark.FlintSparkIndex
import org.opensearch.flint.spark.skipping.FlintSparkSkippingIndex.{getName, FILE_PATH_COLUMN}

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

  /** Output schema of the skipping index */
  val outputSchema: Map[String, String] = {
    val schema = indexedColumns
      .flatMap(_.outputSchema().toList)
      .toMap

    schema + (FILE_PATH_COLUMN -> "keyword")
  }

  override def name(): String = {
    getName(tableName)
  }

  override def metadata(): FlintMetadata = {
    new FlintMetadata(s"""{
        |   "_meta": {
        |     "kind": "SkippingIndex",
        |     "indexedColumns": $getMetaInfo
        |   },
        |   "properties": $getSchema
        | }
        |""".stripMargin)
  }

  private def getMetaInfo: String = {
    Serialization.write(indexedColumns.map(_.indexedColumn))
  }

  private def getSchema: String = {
    Serialization.write(outputSchema.map { case (colName, colType) =>
      colName -> ("type" -> colType)
    })
  }
}

object FlintSparkSkippingIndex {

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
  def getName(tableName: String): String = s"flint_${tableName}_skipping_index"
}
