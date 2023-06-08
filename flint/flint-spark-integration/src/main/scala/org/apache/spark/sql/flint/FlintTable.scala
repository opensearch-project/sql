/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.apache.spark.sql.flint

import java.util

import org.opensearch.flint.core.FlintClientBuilder

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.connector.catalog.{SupportsRead, SupportsWrite, Table, TableCapability}
import org.apache.spark.sql.connector.catalog.TableCapability.{BATCH_READ, BATCH_WRITE, STREAMING_WRITE, TRUNCATE}
import org.apache.spark.sql.connector.read.ScanBuilder
import org.apache.spark.sql.connector.write.{LogicalWriteInfo, WriteBuilder}
import org.apache.spark.sql.flint.config.FlintSparkConf
import org.apache.spark.sql.flint.datatype.FlintDataType
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

/**
 * FlintTable.
 * @param conf
 *   configuration
 * @param userSpecifiedSchema
 *   userSpecifiedSchema
 */
case class FlintTable(conf: util.Map[String, String], userSpecifiedSchema: Option[StructType])
    extends Table
    with SupportsRead
    with SupportsWrite {

  lazy val sparkSession = SparkSession.active

  lazy val flintSparkConf: FlintSparkConf = FlintSparkConf(conf)

  lazy val name: String = flintSparkConf.tableName()

  var schema: StructType = {
    if (schema == null) {
      schema = if (userSpecifiedSchema.isDefined) {
        userSpecifiedSchema.get
      } else {
        FlintDataType.deserialize(
          FlintClientBuilder
            .build(flintSparkConf.flintOptions())
            .getIndexMetadata(name)
            .getContent)
      }
    }
    schema
  }

  override def capabilities(): util.Set[TableCapability] =
    util.EnumSet.of(BATCH_READ, BATCH_WRITE, TRUNCATE, STREAMING_WRITE)

  override def newScanBuilder(options: CaseInsensitiveStringMap): ScanBuilder = {
    FlintScanBuilder(name, schema, flintSparkConf)
  }

  override def newWriteBuilder(info: LogicalWriteInfo): WriteBuilder = {
    FlintWriteBuilder(name, info, flintSparkConf)
  }
}
