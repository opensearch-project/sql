/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.apache.spark.sql.flint

import java.util

import org.apache.spark.sql.connector.catalog.{SessionConfigSupport, Table, TableProvider}
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.flint.FlintDataSourceV2.FLINT_DATASOURCE
import org.apache.spark.sql.sources.DataSourceRegister
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

class FlintDataSourceV2 extends TableProvider with DataSourceRegister with SessionConfigSupport {

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

  protected def getFlintTable(
      schema: Option[StructType],
      properties: util.Map[String, String]): FlintTable = FlintTable(properties, schema)

  /**
   * format name. for instance, `sql.read.format("flint")`
   */
  override def shortName(): String = FLINT_DATASOURCE

  override def supportsExternalMetadata(): Boolean = true

  // scalastyle:off
  /**
   * extract datasource session configs and remove prefix. for example, it extract xxx.yyy from
   * spark.datasource.flint.xxx.yyy. more
   * reading.https://github.com/apache/spark/blob/master/sql/core/src/main/scala/org/apache
   * /spark/sql/execution/datasources/v2/DataSourceV2Utils.scala#L52
   */
  // scalastyle:off
  override def keyPrefix(): String = FLINT_DATASOURCE
}

object FlintDataSourceV2 {

  val FLINT_DATASOURCE = "flint"
}
