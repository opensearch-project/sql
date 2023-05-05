/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.apache.spark.sql.v2

import java.util

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.connector.catalog.{Table, TableProvider}
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.sources.DataSourceRegister
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

class OpenSearchJsonDataSourceV2 extends TableProvider with DataSourceRegister {

  /**
   * Todo. add inferSchema for read path
   */
  override def inferSchema(options: CaseInsensitiveStringMap): StructType = {
    throw new UnsupportedOperationException("inferSchema is not supported")
  }

  override def getTable(
      schema: StructType,
      partitioning: Array[Transform],
      properties: util.Map[String, String]): Table = {
    OpenSearchTable(
      getTableName(properties),
      SparkSession.active,
      new CaseInsensitiveStringMap(properties),
      Some(schema))
  }

  /**
   * format name. for instance, `sql.read.format("flint")`
   */
  override def shortName(): String = "flint"

  override def supportsExternalMetadata(): Boolean = true

  /**
   * get from paths property.
   */
  private def getTableName(properties: util.Map[String, String]): String = {
    properties.get("path")
  }
}
