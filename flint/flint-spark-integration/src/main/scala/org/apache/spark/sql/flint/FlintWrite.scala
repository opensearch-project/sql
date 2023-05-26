/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.apache.spark.sql.flint

import org.apache.spark.internal.Logging
import org.apache.spark.sql.connector.write._
import org.apache.spark.sql.connector.write.streaming.{StreamingDataWriterFactory, StreamingWrite}
import org.apache.spark.sql.flint.config.FlintSparkConf

case class FlintWrite(tableName: String, logicalWriteInfo: LogicalWriteInfo)
    extends Write
    with BatchWrite
    with StreamingWrite
    with Logging {

  override def createBatchWriterFactory(info: PhysicalWriteInfo): DataWriterFactory = {
    logDebug(s"""Create batch write factory of ${logicalWriteInfo.queryId()} with ${info
        .numPartitions()} partitions""")
    FlintPartitionWriterFactory(
      tableName,
      logicalWriteInfo.schema(),
      FlintSparkConf(logicalWriteInfo.options().asCaseSensitiveMap()))
  }

  override def commit(messages: Array[WriterCommitMessage]): Unit = {
    logDebug(s"Write of ${logicalWriteInfo.queryId()} committed for: ${messages.length} tasks")
  }

  override def abort(messages: Array[WriterCommitMessage]): Unit = {}

  override def createStreamingWriterFactory(
      info: PhysicalWriteInfo): StreamingDataWriterFactory = {
    logDebug(s"""Create streaming write factory of ${logicalWriteInfo.queryId()} with ${info
        .numPartitions()} partitions""")
    FlintPartitionWriterFactory(
      tableName,
      logicalWriteInfo.schema(),
      FlintSparkConf(logicalWriteInfo.options().asCaseSensitiveMap()))
  }

  override def commit(epochId: Long, messages: Array[WriterCommitMessage]): Unit = {
    logDebug(s"""Write of ${logicalWriteInfo
        .queryId()} committed for epochId: $epochId, ${messages.length} tasks""")
  }

  override def abort(epochId: Long, messages: Array[WriterCommitMessage]): Unit = {}

  override def toBatch: BatchWrite = this

  override def toStreaming: StreamingWrite = this
}
