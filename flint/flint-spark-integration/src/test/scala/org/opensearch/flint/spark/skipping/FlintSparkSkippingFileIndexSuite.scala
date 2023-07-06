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
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Literal
import org.apache.spark.sql.execution.datasources.{FileIndex, PartitionDirectory}

class FlintSparkSkippingFileIndexSuite extends FlintSuite with Matchers {

  test("") {
    val baseFileIndex = mock[FileIndex]
    when(baseFileIndex.listFiles(any(), any()))
      .thenReturn(mockPartitions(Map("partition-1" -> Seq("filepath-1"))))

    val queryIndex = mockQueryIndexDf(Seq("filepath-1"))

    val fileIndex = FlintSparkSkippingFileIndex(baseFileIndex, queryIndex)
    fileIndex.listFiles(Seq.empty, Seq.empty) shouldBe
      mockPartitions(Map("partition-1" -> Seq("filepath-1")))
  }

  private def mockPartitions(partitions: Map[String, Seq[String]]): Seq[PartitionDirectory] = {
    partitions.map { case (partitionName, filePaths) =>
      val files = filePaths.map(path => new FileStatus(0, false, 0, 0, 0, new Path(path)))
      PartitionDirectory(InternalRow(Literal(partitionName)), files)
    }.toSeq
  }

  private def mockQueryIndexDf(filePaths: Seq[String]): DataFrame = {
    val mySpark = spark
    import mySpark.implicits._

    val columns = Seq(FILE_PATH_COLUMN)
    filePaths.toDF(columns: _*)
  }
}
