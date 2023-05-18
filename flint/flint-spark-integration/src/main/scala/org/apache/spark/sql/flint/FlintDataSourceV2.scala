/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.apache.spark.sql.flint

import java.util
import java.util.NoSuchElementException

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.connector.catalog.{Table, TableProvider}
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.sources.DataSourceRegister
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

class FlintDataSourceV2 extends TableProvider with DataSourceRegister {

  private var table: FlintTable = null

  override def inferSchema(options: CaseInsensitiveStringMap): StructType = {
    if (table == null) {
      table = getFlintTable(Option.empty, options.asCaseSensitiveMap())
    }
    table.schema
  }

  override def getTable(
      schema: StructType,
      partitioning: Array[Transform],
      properties: util.Map[String, String]): Table = {
    if (table == null) {
      getFlintTable(Some(schema), properties)
    } else {
      table
    }
  }

  protected def getTableName(properties: util.Map[String, String]): String = {
    if (properties.containsKey("path")) properties.get("path")
    else if (properties.containsKey("index")) properties.get("index")
    else throw new NoSuchElementException("index or path not found")
  }

  protected def getFlintTable(
      schema: Option[StructType],
      properties: util.Map[String, String]): FlintTable = {
    FlintTable(getTableName(properties), SparkSession.active, schema)
  }

  /**
   * format name. for instance, `sql.read.format("flint")`
   */
  override def shortName(): String = "flint"

  override def supportsExternalMetadata(): Boolean = true
}
