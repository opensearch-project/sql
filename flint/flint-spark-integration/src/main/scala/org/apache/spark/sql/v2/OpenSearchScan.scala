/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.apache.spark.sql.v2

import scala.collection.JavaConverters._

import org.apache.spark.sql.connector.read.{Batch, InputPartition, PartitionReaderFactory, Scan}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

case class OpenSearchScan(
    tableName: String,
    schema: StructType,
    options: CaseInsensitiveStringMap)
    extends Scan
    with Batch {

  override def readSchema(): StructType = schema

  override def planInputPartitions(): Array[InputPartition] = {
    Array(OpenSearchInputPartition())
  }

  override def createReaderFactory(): PartitionReaderFactory = {
    OpenSearchPartitionReaderFactory(
      tableName,
      schema,
      options.asCaseSensitiveMap().asScala.toMap)
  }

  override def toBatch: Batch = this
}

private[spark] case class OpenSearchInputPartition() extends InputPartition {}
