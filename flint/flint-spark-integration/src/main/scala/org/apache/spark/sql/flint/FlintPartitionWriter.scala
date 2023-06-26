/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.apache.spark.sql.flint

import org.opensearch.flint.core.storage.FlintWriter

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.json.JSONOptions
import org.apache.spark.sql.catalyst.util.CaseInsensitiveMap
import org.apache.spark.sql.connector.write.{DataWriter, WriterCommitMessage}
import org.apache.spark.sql.flint.config.FlintSparkConf
import org.apache.spark.sql.flint.datatype.FlintDataType.DATE_FORMAT_PARAMETERS
import org.apache.spark.sql.flint.json.FlintJacksonGenerator
import org.apache.spark.sql.types.StructType

/**
 * Submit create(put if absent) bulk request using FlintWriter. Using "create" action to avoid
 * delete-create docs.
 */
case class FlintPartitionWriter(
    flintWriter: FlintWriter,
    dataSchema: StructType,
    options: FlintSparkConf,
    partitionId: Int,
    taskId: Long,
    epochId: Long = -1)
    extends DataWriter[InternalRow]
    with Logging {

  private lazy val jsonOptions = {
    new JSONOptions(CaseInsensitiveMap(DATE_FORMAT_PARAMETERS), options.timeZone, "")
  }
  private lazy val gen =
    FlintJacksonGenerator(dataSchema, flintWriter, jsonOptions, ignoredFieldName)

  private lazy val idFieldName = options.docIdColumnName()

  private lazy val idOrdinal =
    idFieldName.flatMap(filedName => dataSchema.getFieldIndex(filedName))

  private lazy val ignoredFieldName: Option[String] =
    idFieldName.filter(_ => options.ignoreIdColumn())

  /**
   * total write doc count.
   */
  private var docCount = 0;

  /**
   * { "create": { "_id": "id1" } } { "title": "Prisoners", "year": 2013 }
   */
  override def write(record: InternalRow): Unit = {
    gen.writeAction(FlintWriter.ACTION_CREATE, idOrdinal, record)
    gen.writeLineEnding()
    gen.write(record)
    gen.writeLineEnding()

    docCount += 1
    if (docCount >= options.batchSize()) {
      gen.flush()
      docCount = 0
    }
  }

  override def commit(): WriterCommitMessage = {
    gen.flush()
    logDebug(s"Write commit on partitionId: $partitionId, taskId: $taskId, epochId: $epochId")
    FlintWriterCommitMessage(partitionId, taskId, epochId)
  }

  override def abort(): Unit = {
    // do nothing.
  }

  override def close(): Unit = {
    gen.close()
    logDebug(s"Write close on partitionId: $partitionId, taskId: $taskId, epochId: $epochId")
  }
}

case class FlintWriterCommitMessage(partitionId: Int, taskId: Long, epochId: Long)
    extends WriterCommitMessage
