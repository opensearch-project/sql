/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.apache.spark.sql.flint

import java.util

import org.opensearch.flint.core.{FlintClientBuilder, FlintOptions}

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.write.{DataWriter, DataWriterFactory}
import org.apache.spark.sql.types.StructType

case class FlintPartitionWriterFactory(
    tableName: String,
    schema: StructType,
    properties: util.Map[String, String])
    extends DataWriterFactory
    with Logging {
  override def createWriter(partitionId: Int, taskId: Long): DataWriter[InternalRow] = {
    logDebug(s"create writer for partition: $partitionId, task: $taskId")
    val flintClient = FlintClientBuilder.build(new FlintOptions(properties))
    FlintPartitionWriter(
      flintClient.createWriter(tableName),
      schema,
      properties,
      partitionId,
      taskId)
  }
}
