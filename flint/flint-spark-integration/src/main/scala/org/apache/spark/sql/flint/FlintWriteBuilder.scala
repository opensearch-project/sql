/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.apache.spark.sql.flint

import org.apache.spark.sql.connector.write.{LogicalWriteInfo, SupportsOverwrite, Write, WriteBuilder}
import org.apache.spark.sql.sources.Filter

case class FlintWriteBuilder(tableName: String, info: LogicalWriteInfo)
    extends SupportsOverwrite {

  /**
   * Flint client support overwrite docs with same id and does not use filters.
   */
  override def overwrite(filters: Array[Filter]): WriteBuilder = this

  override def build(): Write = FlintWrite(tableName, info)
}
