/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark.skipping

import org.json4s._
import org.json4s.native.Serialization
import org.opensearch.flint.core.metadata.FlintMetadata
import org.opensearch.flint.spark.FlintSparkIndex
import org.opensearch.flint.spark.skipping.FlintSparkSkippingIndex.FILE_PATH_COLUMN

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.catalog.Column

/**
 * Flint skipping index in Spark.
 *
 * @param tableName
 *   source table name
 */
class FlintSparkSkippingIndex(tableName: String, indexedColumns: Seq[FlintSparkSkippingSketch])
    extends FlintSparkIndex {

  /** Required by json4s write function */
  implicit val formats: Formats = Serialization.formats(NoTypeHints)

  /** Output schema of the skipping index */
  lazy val outputSchema: SparkSession => Map[String, String] = spark => {
    val columns: Map[String, Column] =
      spark.catalog
        .listColumns(tableName)
        .collect()
        .map(col => col.name -> col)
        .toMap

    val schema = indexedColumns
      .flatMap(_.outputSchema(columns).toList)
      .toMap

    schema + (FILE_PATH_COLUMN -> "keyword")
  }

  /**
   * @return
   *   Flint index name
   */
  override def name(): String = {
    tableName + "_skipping_index"
  }

  /**
   * @return
   *   Flint index metadata
   */
  override def metadata(spark: SparkSession): FlintMetadata = {
    new FlintMetadata(s"""{
        |   "_meta": {
        |     "kind": "SkippingIndex",
        |     "indexedColumns": $getMetaInfo
        |   },
        |   "properties": ${getSchema(spark)}
        | }
        |""".stripMargin)
  }

  /**
   * Represent index building by Spark DataFrame.
   *
   * @param spark
   *   Spark session
   * @return
   *   index building data frame
   */
  override def build(spark: SparkSession): DataFrame = {
    // TODO: pending on specific skipping sketch
    spark.readStream
      .table(tableName)
  }

  private def getMetaInfo: String = {
    Serialization.write(indexedColumns)
  }

  private def getSchema(spark: SparkSession): String = {
    Serialization.write(outputSchema(spark).map { case (colName, colType) =>
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
