/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark

import org.opensearch.flint.core.FlintClient
import org.opensearch.flint.core.metadata.FlintMetadata
import org.opensearch.flint.core.storage.FlintOpenSearchClient
import org.opensearch.flint.spark.FlintSpark._

import org.apache.spark.sql.SparkSession

/**
 * Flint Spark integration API entrypoint.
 */
class FlintSpark(spark: SparkSession) {

  val flintClient: FlintClient = {
    val host = spark.conf.get(FLINT_INDEX_STORE_LOCATION, FLINT_INDEX_STORE_LOCATION_DEFAULT)
    val port = spark.conf.get(FLINT_INDEX_STORE_PORT, FLINT_INDEX_STORE_PORT_DEFAULT).toInt
    new FlintOpenSearchClient(host, port)
  }

  def createIndex(index: FlintSparkIndex): Unit = {
    flintClient.createIndex(index.name(), index.metadata())

    // TODO: pending on Flint data source
    // index.build(spark).writeStream.format("flint")
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
}

object FlintSpark {

  val FLINT_INDEX_STORE_LOCATION = "spark.flint.indexstore.location"
  val FLINT_INDEX_STORE_LOCATION_DEFAULT = "localhost"
  val FLINT_INDEX_STORE_PORT = "spark.flint.indexstore.port"
  val FLINT_INDEX_STORE_PORT_DEFAULT = "9200"
}
