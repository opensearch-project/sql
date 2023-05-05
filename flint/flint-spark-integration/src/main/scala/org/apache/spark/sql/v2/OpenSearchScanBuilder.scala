/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.apache.spark.sql.v2

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.connector.read.{Scan, ScanBuilder}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

case class OpenSearchScanBuilder(
    tableName: String,
    sparkSession: SparkSession,
    schema: StructType,
    options: CaseInsensitiveStringMap)
    extends ScanBuilder {
  override def build(): Scan = {
    OpenSearchScan(tableName, schema, options)
  }
}
