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

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

/**
 * Flint skipping index in Spark.
 *
 * @param tableName
 *   source table name
 */
case class FlintSparkSkippingIndex(
    spark: SparkSession,
    tableName: String,
    indexedColumns: Seq[FlintSparkSkippingStrategy])
    extends FlintSparkIndex {

  /** Skipping index type */
  override val kind: String = SKIPPING_INDEX_TYPE

  /** Required by json4s write function */
  implicit val formats: Formats = Serialization.formats(NoTypeHints)

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
        |     "kind": "$kind",
        |     "indexedColumns": $getMetaInfo,
        |     "source": "$tableName"
        |   },
        |   "properties": $getSchema
        | }
        |""".stripMargin)
  }

  override def query(): DataFrame = {
    spark.read
      .format("flint")
      .schema(getDfSchema)
      .load(name())
  }

  private def getMetaInfo: String = {
    Serialization.write(indexedColumns)
  }

  private def getSchema: String = {
    Serialization.write(outputSchema.map { case (colName, colType) =>
      colName -> ("type" -> colType)
    })
  }

  private def getDfSchema: StructType = {
    StructType(outputSchema.map {
      case (colName, "integer") =>
        StructField(colName, IntegerType, nullable = false)
      case (colName, "keyword") =>
        StructField(colName, StringType, nullable = false)
    }.toSeq)
  }
}

object FlintSparkSkippingIndex {

  /** Index type name */
  val SKIPPING_INDEX_TYPE = "SkippingIndex"

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
