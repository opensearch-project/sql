/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.apache.spark.sql.v2

import java.util

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.connector.catalog.{SupportsRead, Table, TableCapability}
import org.apache.spark.sql.connector.catalog.TableCapability.BATCH_READ
import org.apache.spark.sql.connector.read.ScanBuilder
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

/**
 * OpenSearchTable represent an index in OpenSearch.
 * @param tableName
 *   OpenSearch index name.
 * @param sparkSession
 *   sparkSession
 * @param options
 *   options.
 * @param userSpecifiedSchema
 *   userSpecifiedSchema
 */
case class OpenSearchTable(
    name: String,
    sparkSession: SparkSession,
    options: CaseInsensitiveStringMap,
    userSpecifiedSchema: Option[StructType])
    extends Table
    with SupportsRead {

  override def schema(): StructType = userSpecifiedSchema.get

  override def capabilities(): util.Set[TableCapability] =
    util.EnumSet.of(BATCH_READ)

  override def newScanBuilder(options: CaseInsensitiveStringMap): ScanBuilder = {
    OpenSearchScanBuilder(name, sparkSession, schema(), options)
  }
}
