/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark.skipping

import org.json4s._
import org.json4s.native.JsonMethods._
import org.json4s.native.Serialization
import org.opensearch.flint.core.metadata.FlintMetadata
import org.opensearch.flint.spark.FlintSparkIndex
import org.opensearch.flint.spark.FlintSparkIndex.ID_COLUMN
import org.opensearch.flint.spark.skipping.FlintSparkSkippingIndex.{getSkippingIndexName, FILE_PATH_COLUMN, SKIPPING_INDEX_TYPE}
import org.opensearch.flint.spark.skipping.FlintSparkSkippingStrategy.SkippingKindSerializer

import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.catalyst.dsl.expressions.DslExpression
import org.apache.spark.sql.flint.datatype.FlintDataType
import org.apache.spark.sql.functions.{col, input_file_name, sha1}
import org.apache.spark.sql.types.{DataType, StringType, StructField, StructType}

/**
 * Flint skipping index in Spark.
 *
 * @param tableName
 *   source table name
 */
class FlintSparkSkippingIndex(
    tableName: String,
    val indexedColumns: Seq[FlintSparkSkippingStrategy])
    extends FlintSparkIndex {

  require(indexedColumns.nonEmpty, "indexed columns must not be empty")

  /** Required by json4s write function */
  implicit val formats: Formats = Serialization.formats(NoTypeHints) + SkippingKindSerializer

  /** Skipping index type */
  override val kind: String = SKIPPING_INDEX_TYPE

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
      .withColumn(ID_COLUMN, sha1(col(FILE_PATH_COLUMN)))
  }

  private def getMetaInfo: String = {
    Serialization.write(indexedColumns)
  }

  private def getSchema: String = {
    val indexFieldTypes =
      indexedColumns.flatMap(_.outputSchema()).map { case (colName, colType) =>
        // Data type INT from catalog is not recognized by Spark DataType.fromJson()
        val columnType = if (colType == "int") "integer" else colType
        val sparkType = DataType.fromJson("\"" + columnType + "\"")
        StructField(colName, sparkType, nullable = false)
      }

    val allFieldTypes =
      indexFieldTypes :+ StructField(FILE_PATH_COLUMN, StringType, nullable = false)

    // Convert StructType to {"properties": ...} and only need the properties value
    val properties = FlintDataType.serialize(StructType(allFieldTypes))
    compact(render(parse(properties) \ "properties"))
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
