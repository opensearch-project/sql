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
import org.apache.spark.sql.functions.isnull
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

    // TODO: make this listFile call only in hybrid scan mode
    val partitions = baseFileIndex.listFiles(partitionFilters, dataFilters)
    val selectedFiles =
      if (FlintSparkConf().isHybridScanEnabled) {
        selectFilesFromIndexAndSource(partitions)
      } else {
        selectFilesFromIndexOnly()
      }

    // Keep partition files present in selected file list above
    partitions
      .map(p => p.copy(files = p.files.filter(f => isFileNotSkipped(selectedFiles, f))))
      .filter(p => p.files.nonEmpty)
  }

  override def rootPaths: Seq[Path] = baseFileIndex.rootPaths

  override def inputFiles: Array[String] = baseFileIndex.inputFiles

  override def refresh(): Unit = baseFileIndex.refresh()

  override def sizeInBytes: Long = baseFileIndex.sizeInBytes

  override def partitionSchema: StructType = baseFileIndex.partitionSchema

  /*
   * Left join source partitions and index data to keep unknown source files:
   * Express the logic in SQL:
   *   SELECT left.file_path
   *   FROM partitions AS left
   *   LEFT JOIN indexScan AS right
   *     ON left.file_path = right.file_path
   *   WHERE right.file_path IS NULL
   *     OR [indexFilter]
   */
  private def selectFilesFromIndexAndSource(partitions: Seq[PartitionDirectory]): Set[String] = {
    val sparkSession = indexScan.sparkSession
    import sparkSession.implicits._

    partitions
      .flatMap(_.files.map(f => f.getPath.toUri.toString))
      .toDF(FILE_PATH_COLUMN)
      .join(indexScan, Seq(FILE_PATH_COLUMN), "left")
      .filter(isnull(indexScan(FILE_PATH_COLUMN)) || new Column(indexFilter))
      .select(FILE_PATH_COLUMN)
      .collect()
      .map(_.getString(0))
      .toSet
  }

  /*
   * Consider file paths in index data alone. In this case, index filter can be pushed down
   * to index store.
   */
  private def selectFilesFromIndexOnly(): Set[String] = {
    indexScan
      .filter(new Column(indexFilter))
      .select(FILE_PATH_COLUMN)
      .collect
      .map(_.getString(0))
      .toSet
  }

  private def isFileNotSkipped(selectedFiles: Set[String], f: FileStatus) = {
    selectedFiles.contains(f.getPath.toUri.toString)
  }
}
