/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.apache.spark.sql.flint

import org.opensearch.flint.core.FlintOptions

import org.apache.spark.sql.connector.read.{Batch, InputPartition, PartitionReaderFactory, Scan}
import org.apache.spark.sql.types.StructType

case class FLintScan(tableName: String, schema: StructType, options: FlintOptions)
    extends Scan
    with Batch {

  override def readSchema(): StructType = schema

  override def planInputPartitions(): Array[InputPartition] = {
    Array(OpenSearchInputPartition())
  }

  override def createReaderFactory(): PartitionReaderFactory = {
    FlintPartitionReaderFactory(tableName, schema, options)
  }

  override def toBatch: Batch = this
}

// todo. add partition support.
private[spark] case class OpenSearchInputPartition() extends InputPartition {}
