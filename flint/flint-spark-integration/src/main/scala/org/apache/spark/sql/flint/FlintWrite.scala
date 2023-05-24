/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.apache.spark.sql.flint

import org.apache.spark.internal.Logging
import org.apache.spark.sql.connector.write._

case class FlintWrite(tableName: String, logicalWriteInfo: LogicalWriteInfo)
    extends Write
    with BatchWrite
    with Logging {

  override def createBatchWriterFactory(info: PhysicalWriteInfo): DataWriterFactory = {
    FlintPartitionWriterFactory(
      tableName,
      logicalWriteInfo.schema(),
      logicalWriteInfo.options().asCaseSensitiveMap())
  }

  override def commit(messages: Array[WriterCommitMessage]): Unit = {
    logDebug(s"Write of ${logicalWriteInfo.queryId()} committed for: ${messages.length} tasks")
  }

  override def abort(messages: Array[WriterCommitMessage]): Unit = {}

  override def toBatch: BatchWrite = this
}
