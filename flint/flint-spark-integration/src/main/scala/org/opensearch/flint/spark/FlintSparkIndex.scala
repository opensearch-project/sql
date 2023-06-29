/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark

import org.opensearch.flint.core.metadata.FlintMetadata

import org.apache.spark.sql.DataFrame

/**
 * Flint index interface in Spark.
 */
trait FlintSparkIndex {

  /**
   * Index type
   */
  val kind: String

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
   * Build a data frame to represent index data computation logic. Upper level code decides how to
   * use this, ex. batch or streaming, fully or incremental refresh.
   *
   * @param df
   *   data frame to append building logic
   * @return
   *   index building data frame
   */
  def build(df: DataFrame): DataFrame
}

object FlintSparkIndex {

  /**
   * ID column name.
   */
  val ID_COLUMN_NAME: String = "id"
}