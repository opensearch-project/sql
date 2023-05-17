/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark

import org.opensearch.flint.core.metadata.FlintMetadata

import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * Flint index interface in Spark.
 */
trait FlintSparkIndex {

  /**
   * @return
   *   Flint index name
   */
  def name(): String

  /**
   * @return
   *   Flint index metadata
   */
  def metadata(): FlintMetadata

  /**
   * Represent index building by Spark DataFrame.
   *
   * @param spark
   *   Spark session
   * @return
   *   index building data frame
   */
  def build(spark: SparkSession): DataFrame

}
