/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.apache.spark.sql.flint

import org.opensearch.flint.core.FlintOptions

import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.connector.read.{Scan, ScanBuilder}
import org.apache.spark.sql.types.StructType

case class FlintScanBuilder(
    tableName: String,
    sparkSession: SparkSession,
    schema: StructType,
    options: FlintOptions)
    extends ScanBuilder
    with Logging {

  override def build(): Scan = {
    FlintScan(tableName, schema, options)
  }
}
