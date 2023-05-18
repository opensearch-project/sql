/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.apache.spark.sql.flint

import java.util

import org.apache.spark.sql.connector.read.{Batch, InputPartition, PartitionReaderFactory, Scan}
import org.apache.spark.sql.types.StructType

case class FlintScan(tableName: String, schema: StructType, properties: util.Map[String, String])
    extends Scan
    with Batch {

  override def readSchema(): StructType = schema

  override def planInputPartitions(): Array[InputPartition] = {
    Array(OpenSearchInputPartition())
  }

  override def createReaderFactory(): PartitionReaderFactory = {
    FlintPartitionReaderFactory(tableName, schema, properties)
  }

  override def toBatch: Batch = this
}

// todo. add partition support.
private[spark] case class OpenSearchInputPartition() extends InputPartition {}
