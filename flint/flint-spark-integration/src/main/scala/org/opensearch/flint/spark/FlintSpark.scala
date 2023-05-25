/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark

import scala.collection.JavaConverters._

import org.json4s.{Formats, JArray, NoTypeHints}
import org.json4s.native.JsonMethods.parse
import org.json4s.native.Serialization
import org.opensearch.flint.core.{FlintClient, FlintClientBuilder, FlintOptions}
import org.opensearch.flint.core.FlintOptions._
import org.opensearch.flint.core.metadata.FlintMetadata
import org.opensearch.flint.spark.FlintSpark._
import org.opensearch.flint.spark.skipping.{FlintSparkSkippingIndex, FlintSparkSkippingStrategy}
import org.opensearch.flint.spark.skipping.FlintSparkSkippingIndex.SKIPPING_INDEX_TYPE
import org.opensearch.flint.spark.skipping.partition.PartitionSkippingStrategy

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.catalog.Column

/**
 * Flint Spark integration API entrypoint.
 */
class FlintSpark(val spark: SparkSession) {

  /** Flint client configuration options */
  private val flintClientOptions =
    Map(
      HOST -> spark.conf.get(FLINT_INDEX_STORE_LOCATION, FLINT_INDEX_STORE_LOCATION_DEFAULT),
      PORT -> spark.conf.get(FLINT_INDEX_STORE_PORT, FLINT_INDEX_STORE_PORT_DEFAULT)).asJava

  /** Flint client for low-level index operation */
  private val flintClient: FlintClient = {
    FlintClientBuilder.build(new FlintOptions(flintClientOptions))
  }

  /** Required by json4s parse function */
  implicit val formats: Formats = Serialization.formats(NoTypeHints)

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
   * Start refreshing index data
   *
   * @param indexName
   *   index name
   * @return
   *   refreshing job ID
   */
  def refreshIndex(indexName: String): String = {
    val index = describeIndex(indexName)
      .getOrElse(throw new IllegalStateException(s"Index $indexName doesn't exist"))

    // TODO: Use Foreach sink for now. Need to move this to FlintSparkSkippingIndex
    //  if this is permanent solution. Otherwise, not applicable to covering index.
    spark.readStream
      .table(getSourceTableName(index))
      .writeStream
      .outputMode("append")
      .foreachBatch { (batchDF: DataFrame, _: Long) =>
        index
          .build(batchDF)
          .write
          .format("flint")
          .options(flintClientOptions)
          .mode("overwrite")
          .save(indexName)
      }
      .start()
      .id
      .toString
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
    implicit val formats: Formats = Serialization.formats(NoTypeHints)

    val meta = parse(metadata.getContent) \ "_meta"
    val tableName = (meta \ "source").extract[String]
    val indexType = (meta \ "kind").extract[String]
    val indexedColumns = (meta \ "indexedColumns").asInstanceOf[JArray]

    indexType match {
      case SKIPPING_INDEX_TYPE =>
        val strategies = indexedColumns.arr.map { colInfo =>
          val skippingType = (colInfo \ "kind").extract[String]
          val columnName = (colInfo \ "columnName").extract[String]
          val columnType = (colInfo \ "columnType").extract[String]

          skippingType match {
            case "partition" =>
              new PartitionSkippingStrategy(columnName = columnName, columnType = columnType)
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
   * Flint configurations in Spark. TODO: shared with Flint data source config?
   */
  val FLINT_INDEX_STORE_LOCATION = "spark.flint.indexstore.location"
  val FLINT_INDEX_STORE_LOCATION_DEFAULT = "localhost"
  val FLINT_INDEX_STORE_PORT = "spark.flint.indexstore.port"
  val FLINT_INDEX_STORE_PORT_DEFAULT = "9200"

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
    def addPartitionIndex(colNames: String*): IndexBuilder = {
      colNames
        .map(colName =>
          allColumns.getOrElse(
            colName,
            throw new IllegalArgumentException(s"Column $colName does not exist")))
        .map(col =>
          new PartitionSkippingStrategy(columnName = col.name, columnType = col.dataType))
        .foreach(indexedCol => indexedColumns = indexedColumns :+ indexedCol)
      this
    }

    /**
     * Create index.
     */
    def create(): Unit = {
      require(tableName.nonEmpty, "table name cannot be empty")

      flint.createIndex(new FlintSparkSkippingIndex(tableName, indexedColumns))
    }
  }
}
