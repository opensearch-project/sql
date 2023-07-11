/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark.skipping

import org.apache.hadoop.fs.{FileStatus, Path}
import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito.when
import org.opensearch.flint.spark.skipping.FlintSparkSkippingIndex.FILE_PATH_COLUMN
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.mockito.MockitoSugar.mock

import org.apache.spark.FlintSuite
import org.apache.spark.sql.{Column, DataFrame, Row}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Literal, Predicate}
import org.apache.spark.sql.execution.datasources.{FileIndex, PartitionDirectory}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types._

class FlintSparkSkippingFileIndexSuite extends FlintSuite with Matchers {

  /** Test source partition data. */
  private val partition1 = "partition-1" -> Seq("file-1", "file-2")
  private val partition2 = "partition-2" -> Seq("file-3")

  /** Test index data schema. */
  private val schema = Map((FILE_PATH_COLUMN, StringType), ("year", IntegerType))

  test("should keep files returned from index") {
    assertFlintFileIndex()
      .withSourceFiles(Map(partition1))
      .withIndexData(schema, Seq(Row("file-1", 2023), Row("file-2", 2022)))
      .withIndexFilter(col("year") === 2023)
      .shouldScanSourceFiles(Map("partition-1" -> Seq("file-1")))
  }

  test("should keep files of multiple partitions returned from index") {
    assertFlintFileIndex()
      .withSourceFiles(Map(partition1, partition2))
      .withIndexData(schema, Seq(Row("file-1", 2023), Row("file-2", 2022), Row("file-3", 2023)))
      .withIndexFilter(col("year") === 2023)
      .shouldScanSourceFiles(Map("partition-1" -> Seq("file-1"), "partition-2" -> Seq("file-3")))
  }

  test("should skip unknown source files by default") {
    assertFlintFileIndex()
      .withSourceFiles(Map(partition1))
      .withIndexData(
        schema,
        Seq(Row("file-1", 2023)) // file-2 is not refreshed to index yet
      )
      .withIndexFilter(col("year") === 2023)
      .shouldScanSourceFiles(Map("partition-1" -> Seq("file-1")))
  }

  test("should not skip unknown source files in hybrid-scan mode") {
    withHybridScanEnabled {
      assertFlintFileIndex()
        .withSourceFiles(Map(partition1))
        .withIndexData(
          schema,
          Seq(Row("file-1", 2023)) // file-2 is not refreshed to index yet
        )
        .withIndexFilter(col("year") === 2023)
        .shouldScanSourceFiles(Map("partition-1" -> Seq("file-1", "file-2")))
    }
  }

  test("should not skip unknown source files of multiple partitions in hybrid-scan mode") {
    withHybridScanEnabled {
      assertFlintFileIndex()
        .withSourceFiles(Map(partition1, partition2))
        .withIndexData(
          schema,
          Seq(Row("file-1", 2023)) // file-2 is not refreshed to index yet
        )
        .withIndexFilter(col("year") === 2023)
        .shouldScanSourceFiles(
          Map("partition-1" -> Seq("file-1", "file-2"), "partition-2" -> Seq("file-3")))
    }
  }

  private def assertFlintFileIndex(): AssertionHelper = {
    new AssertionHelper
  }

  private class AssertionHelper {
    private val baseFileIndex = mock[FileIndex]
    private var indexScan: DataFrame = _
    private var indexFilter: Predicate = _

    def withSourceFiles(partitions: Map[String, Seq[String]]): AssertionHelper = {
      when(baseFileIndex.listFiles(any(), any()))
        .thenReturn(mockPartitions(partitions))
      this
    }

    def withIndexData(columns: Map[String, DataType], data: Seq[Row]): AssertionHelper = {
      val schema = StructType(columns.map { case (colName, colType) =>
        StructField(colName, colType, nullable = false)
      }.toSeq)
      indexScan = spark.createDataFrame(spark.sparkContext.parallelize(data), schema)
      this
    }

    def withIndexFilter(pred: Column): AssertionHelper = {
      indexFilter = pred.expr.asInstanceOf[Predicate]
      this
    }

    def shouldScanSourceFiles(partitions: Map[String, Seq[String]]): Unit = {
      val fileIndex = FlintSparkSkippingFileIndex(baseFileIndex, indexScan, indexFilter)
      fileIndex.listFiles(Seq.empty, Seq.empty) shouldBe mockPartitions(partitions)
    }

    private def mockPartitions(partitions: Map[String, Seq[String]]): Seq[PartitionDirectory] = {
      partitions.map { case (partitionName, filePaths) =>
        val files = filePaths.map(path => new FileStatus(0, false, 0, 0, 0, new Path(path)))
        PartitionDirectory(InternalRow(Literal(partitionName)), files)
      }.toSeq
    }
  }
}
