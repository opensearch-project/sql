/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.apache.spark.sql.flint

import java.util
import java.util.TimeZone

import scala.collection.JavaConverters.mapAsScalaMapConverter

import org.opensearch.flint.core.storage.FlintWriter

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.json.JSONOptions
import org.apache.spark.sql.catalyst.util.CaseInsensitiveMap
import org.apache.spark.sql.connector.write.{DataWriter, WriterCommitMessage}
import org.apache.spark.sql.flint.FlintPartitionWriter.{BATCH_SIZE, ID_NAME}
import org.apache.spark.sql.flint.json.FlintJacksonGenerator
import org.apache.spark.sql.types.StructType

/**
 * Submit create(put if absent) bulk request using FlintWriter. Using "create" action to avoid
 * delete-create docs.
 */
case class FlintPartitionWriter(
    flintWriter: FlintWriter,
    dataSchema: StructType,
    properties: util.Map[String, String],
    partitionId: Int,
    taskId: Long)
    extends DataWriter[InternalRow]
    with Logging {

  private lazy val jsonOptions = {
    new JSONOptions(CaseInsensitiveMap(Map.empty[String, String]), TimeZone.getDefault.getID, "")
  }
  private lazy val gen = FlintJacksonGenerator(dataSchema, flintWriter, jsonOptions)

  private lazy val idOrdinal = properties.asScala.toMap
    .get(ID_NAME)
    .flatMap(filedName => dataSchema.getFieldIndex(filedName))

  private lazy val batchSize =
    properties.asScala.toMap.get(BATCH_SIZE).map(_.toInt).filter(_ > 0).getOrElse(1000)

  private var count = 0;

  /**
   * { "create": { "_id": "id1" } } { "title": "Prisoners", "year": 2013 }
   */
  override def write(record: InternalRow): Unit = {
    gen.writeAction(FlintWriter.ACTION_CREATE, idOrdinal, record)
    gen.writeLineEnding()
    gen.write(record)
    gen.writeLineEnding()

    count += 1
    if (count >= batchSize) {
      gen.flush()
      count = 0
    }
  }

  override def commit(): WriterCommitMessage = {
    gen.flush()
    logDebug(s"Write finish on partitionId: $partitionId, taskId: $taskId")
    FlintWriterCommitMessage(partitionId, taskId)
  }

  override def abort(): Unit = {
    // do nothing.
  }

  override def close(): Unit = {
    gen.close()
  }
}

case class FlintWriterCommitMessage(partitionId: Int, taskId: Long) extends WriterCommitMessage

object FlintPartitionWriter {
  val ID_NAME = "spark.flint.write.id.name"
  val BATCH_SIZE = "spark.flint.write.batch.size"
}
