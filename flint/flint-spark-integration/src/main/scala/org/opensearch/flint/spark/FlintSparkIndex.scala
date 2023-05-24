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
   * Query current Flint index by Spark data frame.
   *
   * @return
   *   data frame
   */
  def query(): DataFrame
}
