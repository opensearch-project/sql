/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.apache.spark.sql.flint

import org.opensearch.flint.core.{FlintClient, FlintOptions}

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.read.{InputPartition, PartitionReader, PartitionReaderFactory}
import org.apache.spark.sql.types.StructType

case class FlintPartitionReaderFactory(
    tableName: String,
    schema: StructType,
    option: FlintOptions)
    extends PartitionReaderFactory {
  override def createReader(partition: InputPartition): PartitionReader[InternalRow] = {
    val flintClient = FlintClient.create(option)
    new FlintPartitionReader(flintClient.createReader(tableName, null, option), schema, option)
  }
}
