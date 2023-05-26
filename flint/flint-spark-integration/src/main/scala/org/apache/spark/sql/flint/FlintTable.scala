/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.apache.spark.sql.flint

import java.util

import org.apache.spark.sql.connector.catalog.{SupportsRead, SupportsWrite, Table, TableCapability}
import org.apache.spark.sql.connector.catalog.TableCapability.{BATCH_READ, BATCH_WRITE, STREAMING_WRITE, TRUNCATE}
import org.apache.spark.sql.connector.read.ScanBuilder
import org.apache.spark.sql.connector.write.{LogicalWriteInfo, WriteBuilder}
import org.apache.spark.sql.flint.config.FlintSparkConf
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

  val name = FlintSparkConf(conf).tableName()

  var schema: StructType = {
    if (schema == null) {
      schema = if (userSpecifiedSchema.isDefined) {
        userSpecifiedSchema.get
      } else {
        throw new UnsupportedOperationException("infer schema not supported yet")
      }
    }
    schema
  }

  override def capabilities(): util.Set[TableCapability] =
    util.EnumSet.of(BATCH_READ, BATCH_WRITE, TRUNCATE, STREAMING_WRITE)

  override def newScanBuilder(options: CaseInsensitiveStringMap): ScanBuilder = {
    FlintScanBuilder(name, schema, FlintSparkConf(options.asCaseSensitiveMap()))
  }

  override def newWriteBuilder(info: LogicalWriteInfo): WriteBuilder = {
    FlintWriteBuilder(name, info)
  }
}
