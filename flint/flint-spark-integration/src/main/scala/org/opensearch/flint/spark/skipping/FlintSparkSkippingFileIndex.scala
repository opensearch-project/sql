/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark.skipping

import org.apache.hadoop.fs.Path

import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.execution.datasources.{FileIndex, PartitionDirectory}
import org.apache.spark.sql.types.StructType

/**
 * File index that skips files by Flint Spark skipping index.
 *
 * @param baseFileIndex
 * @param selectedFiles
 */
class FlintSparkSkippingFileIndex(baseFileIndex: FileIndex, selectedFiles: Set[String])
    extends FileIndex {

  override def listFiles(
      partitionFilters: Seq[Expression],
      dataFilters: Seq[Expression]): Seq[PartitionDirectory] = {

    val partitions = baseFileIndex.listFiles(partitionFilters, dataFilters)
    partitions
      .map(p =>
        p.copy(files = p.files.filter(f => selectedFiles.contains(f.getPath.toUri.toString))))
      .filter(_.files.nonEmpty)
  }

  override def rootPaths: Seq[Path] = baseFileIndex.rootPaths

  override def inputFiles: Array[String] = baseFileIndex.inputFiles

  override def refresh(): Unit = baseFileIndex.refresh()

  override def sizeInBytes: Long = baseFileIndex.sizeInBytes

  override def partitionSchema: StructType = baseFileIndex.partitionSchema
}
