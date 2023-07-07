/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark.skipping

import org.apache.hadoop.fs.{FileStatus, Path}
import org.opensearch.flint.spark.skipping.FlintSparkSkippingIndex.FILE_PATH_COLUMN

import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.catalyst.expressions.{Expression, Predicate}
import org.apache.spark.sql.execution.datasources.{FileIndex, PartitionDirectory}
import org.apache.spark.sql.flint.config.FlintSparkConf
import org.apache.spark.sql.types.StructType

/**
 * File index that skips source files based on the selected files by Flint skipping index.
 *
 * @param baseFileIndex
 *   original file index
 * @param indexScan
 *   query skipping index DF with pushed down filters
 */
case class FlintSparkSkippingFileIndex(
    baseFileIndex: FileIndex,
    indexScan: DataFrame,
    indexFilter: Predicate)
    extends FileIndex {

  override def listFiles(
      partitionFilters: Seq[Expression],
      dataFilters: Seq[Expression]): Seq[PartitionDirectory] = {

    // TODO: try to avoid the list call if no hybrid scan
    val partitions = baseFileIndex.listFiles(partitionFilters, dataFilters)

    if (FlintSparkConf().isHybridScanEnabled) {
      scanFilesFromIndexAndSource(partitions)
    } else {
      scanFilesFromIndex(partitions)
    }
  }

  override def rootPaths: Seq[Path] = baseFileIndex.rootPaths

  override def inputFiles: Array[String] = baseFileIndex.inputFiles

  override def refresh(): Unit = baseFileIndex.refresh()

  override def sizeInBytes: Long = baseFileIndex.sizeInBytes

  override def partitionSchema: StructType = baseFileIndex.partitionSchema

  private def scanFilesFromIndexAndSource(
      partitions: Seq[PartitionDirectory]): Seq[PartitionDirectory] = {
    Seq.empty
  }

  private def scanFilesFromIndex(partitions: Seq[PartitionDirectory]): Seq[PartitionDirectory] = {
    val selectedFiles =
      indexScan
        .filter(new Column(indexFilter))
        .select(FILE_PATH_COLUMN)
        .collect
        .map(_.getString(0))
        .toSet

    partitions
      .map(p => p.copy(files = p.files.filter(f => isFileNotSkipped(selectedFiles, f))))
      .filter(p => p.files.nonEmpty)
  }

  private def isFileNotSkipped(selectedFiles: Set[String], f: FileStatus) = {
    selectedFiles.contains(f.getPath.toUri.toString)
  }
}
