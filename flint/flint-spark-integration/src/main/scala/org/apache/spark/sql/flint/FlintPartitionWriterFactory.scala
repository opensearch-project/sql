/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.apache.spark.sql.flint

import org.opensearch.flint.core.FlintClientBuilder

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.write.{DataWriter, DataWriterFactory}
import org.apache.spark.sql.connector.write.streaming.StreamingDataWriterFactory
import org.apache.spark.sql.flint.config.FlintSparkConf
import org.apache.spark.sql.types.StructType

case class FlintPartitionWriterFactory(
    tableName: String,
    schema: StructType,
    options: FlintSparkConf)
    extends DataWriterFactory
    with StreamingDataWriterFactory
    with Logging {

  private lazy val flintClient = FlintClientBuilder.build(options.flintOptions())

  override def createWriter(partitionId: Int, taskId: Long): DataWriter[InternalRow] = {
    logDebug(s"create writer for partition: $partitionId, task: $taskId")
    FlintPartitionWriter(
      flintClient.createWriter(tableName),
      schema,
      options,
      partitionId,
      taskId)
  }

  override def createWriter(
      partitionId: Int,
      taskId: Long,
      epochId: Long): DataWriter[InternalRow] = {
    FlintPartitionWriter(
      flintClient.createWriter(tableName),
      schema,
      options,
      partitionId,
      taskId,
      epochId)
  }
}
