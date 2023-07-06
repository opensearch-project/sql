/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark.skipping

import org.apache.hadoop.fs.{FileStatus, Path}

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.execution.datasources.{FileIndex, PartitionDirectory}
import org.apache.spark.sql.types.StructType

/**
 * File index that skips source files based on the selected files by Flint skipping index.
 *
 * @param baseFileIndex
 *   original file index
 * @param queryIndex
 *   query skipping index DF with pushed down filters
 */
case class FlintSparkSkippingFileIndex(baseFileIndex: FileIndex, queryIndex: DataFrame)
    extends FileIndex {

  override def listFiles(
      partitionFilters: Seq[Expression],
      dataFilters: Seq[Expression]): Seq[PartitionDirectory] = {

    val selectedFiles =
      queryIndex.collect
        .map(_.getString(0))
        .toSet

    val partitions = baseFileIndex.listFiles(partitionFilters, dataFilters)
    partitions
      .map(p => p.copy(files = p.files.filter(f => isFileNotSkipped(selectedFiles, f))))
      .filter(p => p.files.nonEmpty)
  }

  override def rootPaths: Seq[Path] = baseFileIndex.rootPaths

  override def inputFiles: Array[String] = baseFileIndex.inputFiles

  override def refresh(): Unit = baseFileIndex.refresh()

  override def sizeInBytes: Long = baseFileIndex.sizeInBytes

  override def partitionSchema: StructType = baseFileIndex.partitionSchema

  private def isFileNotSkipped(selectedFiles: Set[String], f: FileStatus) = {
    selectedFiles.contains(f.getPath.toUri.toString)
  }
}
