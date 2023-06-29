/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark

import scala.collection.JavaConverters._

import org.json4s.{Formats, JArray, NoTypeHints}
import org.json4s.native.JsonMethods.parse
import org.json4s.native.Serialization
import org.opensearch.flint.core.{FlintClient, FlintClientBuilder}
import org.opensearch.flint.core.metadata.FlintMetadata
import org.opensearch.flint.spark.FlintSpark._
import org.opensearch.flint.spark.FlintSpark.RefreshMode.{FULL, INCREMENTAL, RefreshMode}
import org.opensearch.flint.spark.FlintSparkIndex.ID_COLUMN_NAME
import org.opensearch.flint.spark.skipping.{FlintSparkSkippingIndex, FlintSparkSkippingStrategy}
import org.opensearch.flint.spark.skipping.FlintSparkSkippingIndex.SKIPPING_INDEX_TYPE
import org.opensearch.flint.spark.skipping.FlintSparkSkippingStrategy.{SkippingKind, SkippingKindSerializer}
import org.opensearch.flint.spark.skipping.FlintSparkSkippingStrategy.SkippingKind.{MinMax, Partition, ValuesSet}
import org.opensearch.flint.spark.skipping.minmax.MinMaxSkippingStrategy
import org.opensearch.flint.spark.skipping.partition.PartitionSkippingStrategy
import org.opensearch.flint.spark.skipping.valueset.ValueSetSkippingStrategy

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.SaveMode._
import org.apache.spark.sql.catalog.Column
import org.apache.spark.sql.flint.FlintDataSourceV2.FLINT_DATASOURCE
import org.apache.spark.sql.flint.config.FlintSparkConf
import org.apache.spark.sql.flint.config.FlintSparkConf.{DOC_ID_COLUMN_NAME, IGNORE_DOC_ID_COLUMN}
import org.apache.spark.sql.streaming.OutputMode.Append

/**
 * Flint Spark integration API entrypoint.
 */
class FlintSpark(val spark: SparkSession) {

  /** Flint client for low-level index operation */
  private val flintClient: FlintClient =
    FlintClientBuilder.build(
      FlintSparkConf(
        Map(DOC_ID_COLUMN_NAME.key -> ID_COLUMN_NAME, IGNORE_DOC_ID_COLUMN.key -> "true").asJava)
        .flintOptions())

  /** Required by json4s parse function */
  implicit val formats: Formats = Serialization.formats(NoTypeHints) + SkippingKindSerializer

  /**
   * Create index builder for creating index with fluent API.
   *
   * @return
   *   index builder
   */
  def skippingIndex(): IndexBuilder = {
    new IndexBuilder(this)
  }

  /**
   * Create the given index with metadata.
   *
   * @param index
   *   Flint index to create
   */
  def createIndex(index: FlintSparkIndex): Unit = {
    val indexName = index.name()
    if (flintClient.exists(indexName)) {
      throw new IllegalStateException(
        s"A table can only have one Flint skipping index: Flint index $indexName is found")
    }
    flintClient.createIndex(indexName, index.metadata())
  }

  /**
   * Start refreshing index data according to the given mode.
   *
   * @param indexName
   *   index name
   * @param mode
   *   refresh mode
   * @return
   *   refreshing job ID (empty if batch job for now)
   */
  def refreshIndex(indexName: String, mode: RefreshMode): Option[String] = {
    val index = describeIndex(indexName)
      .getOrElse(throw new IllegalStateException(s"Index $indexName doesn't exist"))
    val tableName = getSourceTableName(index)

    // Write Flint index data to Flint data source (shared by both refresh modes for now)
    def writeFlintIndex(df: DataFrame): Unit = {
      index
        .build(df)
        .write
        .format(FLINT_DATASOURCE)
        .mode(Overwrite)
        .save(indexName)
    }

    mode match {
      case FULL =>
        writeFlintIndex(
          spark.read
            .table(tableName))
        None

      case INCREMENTAL =>
        // TODO: Use Foreach sink for now. Need to move this to FlintSparkSkippingIndex
        //  once finalized. Otherwise, covering index/MV may have different logic.
        val job = spark.readStream
          .table(tableName)
          .writeStream
          .outputMode(Append())
          .foreachBatch { (batchDF: DataFrame, _: Long) =>
            writeFlintIndex(batchDF)
          }
          .start()
        Some(job.id.toString)
    }
  }

  /**
   * Describe a Flint index.
   *
   * @param indexName
   *   index name
   * @return
   *   Flint index
   */
  def describeIndex(indexName: String): Option[FlintSparkIndex] = {
    if (flintClient.exists(indexName)) {
      val metadata = flintClient.getIndexMetadata(indexName)
      Some(deserialize(metadata))
    } else {
      Option.empty
    }
  }

  /**
   * Delete index.
   *
   * @param indexName
   *   index name
   * @return
   *   true if exist and deleted, otherwise false
   */
  def deleteIndex(indexName: String): Boolean = {
    if (flintClient.exists(indexName)) {
      flintClient.deleteIndex(indexName)
      true
    } else {
      false
    }
  }

  // TODO: Remove all parsing logic below once Flint spec finalized and FlintMetadata strong typed
  private def getSourceTableName(index: FlintSparkIndex): String = {
    val json = parse(index.metadata().getContent)
    (json \ "_meta" \ "source").extract[String]
  }

  /*
   * For now, deserialize skipping strategies out of Flint metadata json
   * ex. extract Seq(Partition("year", "int"), ValueList("name")) from
   *  { "_meta": { "indexedColumns": [ {...partition...}, {...value list...} ] } }
   *
   */
  private def deserialize(metadata: FlintMetadata): FlintSparkIndex = {
    val meta = parse(metadata.getContent) \ "_meta"
    val tableName = (meta \ "source").extract[String]
    val indexType = (meta \ "kind").extract[String]
    val indexedColumns = (meta \ "indexedColumns").asInstanceOf[JArray]

    indexType match {
      case SKIPPING_INDEX_TYPE =>
        val strategies = indexedColumns.arr.map { colInfo =>
          val skippingKind = SkippingKind.withName((colInfo \ "kind").extract[String])
          val columnName = (colInfo \ "columnName").extract[String]
          val columnType = (colInfo \ "columnType").extract[String]

          skippingKind match {
            case Partition =>
              PartitionSkippingStrategy(columnName = columnName, columnType = columnType)
            case ValuesSet =>
              ValueSetSkippingStrategy(columnName = columnName, columnType = columnType)
            case MinMax =>
              MinMaxSkippingStrategy(columnName = columnName, columnType = columnType)
            case other =>
              throw new IllegalStateException(s"Unknown skipping strategy: $other")
          }
        }
        new FlintSparkSkippingIndex(tableName, strategies)
    }
  }
}

object FlintSpark {

  /**
   * Index refresh mode: FULL: refresh on current source data in batch style at one shot
   * INCREMENTAL: auto refresh on new data in continuous streaming style
   */
  object RefreshMode extends Enumeration {
    type RefreshMode = Value
    val FULL, INCREMENTAL = Value
  }

  /**
   * Helper class for index class construct. For now only skipping index supported.
   */
  class IndexBuilder(flint: FlintSpark) {
    var tableName: String = ""
    var indexedColumns: Seq[FlintSparkSkippingStrategy] = Seq()

    lazy val allColumns: Map[String, Column] = {
      flint.spark.catalog
        .listColumns(tableName)
        .collect()
        .map(col => (col.name, col))
        .toMap
    }

    /**
     * Configure which source table the index is based on.
     *
     * @param tableName
     *   source table name
     * @return
     *   index builder
     */
    def onTable(tableName: String): IndexBuilder = {
      this.tableName = tableName
      this
    }

    /**
     * Add partition skipping indexed columns.
     *
     * @param colNames
     *   indexed column names
     * @return
     *   index builder
     */
    def addPartitions(colNames: String*): IndexBuilder = {
      require(tableName.nonEmpty, "table name cannot be empty")

      colNames
        .map(findColumn)
        .map(col => PartitionSkippingStrategy(columnName = col.name, columnType = col.dataType))
        .foreach(addIndexedColumn)
      this
    }

    /**
     * Add value set skipping indexed column.
     *
     * @param colName
     *   indexed column name
     * @return
     *   index builder
     */
    def addValueSet(colName: String): IndexBuilder = {
      require(tableName.nonEmpty, "table name cannot be empty")

      val col = findColumn(colName)
      addIndexedColumn(ValueSetSkippingStrategy(columnName = col.name, columnType = col.dataType))
      this
    }

    /**
     * Add min max skipping indexed column.
     *
     * @param colName
     *   indexed column name
     * @return
     *   index builder
     */
    def addMinMax(colName: String): IndexBuilder = {
      val col = findColumn(colName)
      indexedColumns =
        indexedColumns :+ MinMaxSkippingStrategy(columnName = col.name, columnType = col.dataType)
      this
    }

    /**
     * Create index.
     */
    def create(): Unit = {
      flint.createIndex(new FlintSparkSkippingIndex(tableName, indexedColumns))
    }

    private def findColumn(colName: String): Column =
      allColumns.getOrElse(
        colName,
        throw new IllegalArgumentException(s"Column $colName does not exist"))

    private def addIndexedColumn(indexedCol: FlintSparkSkippingStrategy): Unit = {
      require(
        indexedColumns.forall(_.columnName != indexedCol.columnName),
        s"${indexedCol.columnName} is already indexed")

      indexedColumns = indexedColumns :+ indexedCol
    }
  }
}
