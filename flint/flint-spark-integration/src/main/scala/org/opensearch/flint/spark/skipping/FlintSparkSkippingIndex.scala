/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark.skipping

import org.opensearch.flint.core.metadata.FlintMetadata
import org.opensearch.flint.spark.FlintSparkIndex

import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * Flint skipping index in Spark.
 *
 * @param tableName
 *   source table name
 */
class FlintSparkSkippingIndex(tableName: String) extends FlintSparkIndex {

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
  override def metadata(): FlintMetadata = {
    new FlintMetadata("""
        | {
        |   "_meta": {
        |     "kind": "SkippingIndex"
        |   },
        |   "properties": {
        |     "file_path": {
        |       "type": "keyword"
        |     }
        |   }
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
    spark.readStream.table(tableName)
  }
}
