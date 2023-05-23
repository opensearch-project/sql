/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.apache.spark.sql.flint

import java.util

import org.opensearch.flint.core.{FlintClientBuilder, FlintOptions}

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.expressions.filter.Predicate
import org.apache.spark.sql.connector.read.{InputPartition, PartitionReader, PartitionReaderFactory}
import org.apache.spark.sql.flint.storage.FlintQueryCompiler
import org.apache.spark.sql.types.StructType

case class FlintPartitionReaderFactory(
    tableName: String,
    schema: StructType,
    properties: util.Map[String, String],
    pushedPredicates: Array[Predicate])
    extends PartitionReaderFactory {
  override def createReader(partition: InputPartition): PartitionReader[InternalRow] = {
    val query = FlintQueryCompiler(schema).compile(pushedPredicates)
    val flintClient = FlintClientBuilder.build(new FlintOptions(properties))
    new FlintPartitionReader(flintClient.createReader(tableName, query), schema)
  }
}
