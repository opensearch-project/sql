/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark

import scala.collection.JavaConverters._

import org.opensearch.flint.core.{FlintClient, FlintOptions}
import org.opensearch.flint.core.FlintOptions._
import org.opensearch.flint.core.metadata.FlintMetadata
import org.opensearch.flint.core.storage.FlintOpenSearchClient
import org.opensearch.flint.spark.FlintSpark._

import org.apache.spark.sql.SparkSession

/**
 * Flint Spark integration API entrypoint.
 */
class FlintSpark(spark: SparkSession) {

  val flintClient: FlintClient = {
    val options = new FlintOptions(
      Map(
        HOST -> spark.conf.get(FLINT_INDEX_STORE_LOCATION, FLINT_INDEX_STORE_LOCATION_DEFAULT),
        PORT -> spark.conf.get(FLINT_INDEX_STORE_PORT, FLINT_INDEX_STORE_PORT_DEFAULT)).asJava)
    new FlintOpenSearchClient(options)
  }

  /**
   * Create index with the given config.
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
    flintClient.createIndex(indexName, index.metadata(spark))

    // TODO: pending on Flint data source write capability
    /*
    index.build(spark)
      .writeStream
      .format("flint")
      .start()
     */
  }

  /**
   * Describe a Flint index.
   *
   * @param indexName
   *   index name
   * @return
   *   Flint index metadata
   */
  def describeIndex(indexName: String): Option[FlintMetadata] = {
    if (flintClient.exists(indexName)) {
      Some(flintClient.getIndexMetadata(indexName))
    } else {
      Option.empty
    }
  }

  /**
   * Delete index.
   *
   * @param indexName
   *   index name
   */
  def deleteIndex(indexName: String): Unit = {
    flintClient.deleteIndex(indexName)
  }
}

object FlintSpark {

  /** Flint configurations in Spark. */
  val FLINT_INDEX_STORE_LOCATION = "spark.flint.indexstore.location"
  val FLINT_INDEX_STORE_LOCATION_DEFAULT = "localhost"
  val FLINT_INDEX_STORE_PORT = "spark.flint.indexstore.port"
  val FLINT_INDEX_STORE_PORT_DEFAULT = "9200"
}
