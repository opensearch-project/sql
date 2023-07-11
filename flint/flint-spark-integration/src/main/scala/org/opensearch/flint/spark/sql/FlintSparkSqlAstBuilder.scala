/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark.sql

import org.opensearch.flint.spark.skipping.FlintSparkSkippingIndex
import org.opensearch.flint.spark.skipping.FlintSparkSkippingIndex.getSkippingIndexName
import org.opensearch.flint.spark.sql.FlintSparkSqlExtensionsParser.{DescribeSkippingIndexStatementContext, DropSkippingIndexStatementContext}

import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.expressions.AttributeReference
import org.apache.spark.sql.catalyst.plans.logical.Command
import org.apache.spark.sql.types.StringType

/**
 * Flint Spark AST builder that builds Spark command for Flint index statement.
 */
class FlintSparkSqlAstBuilder extends FlintSparkSqlExtensionsBaseVisitor[Command] {

  override def visitDescribeSkippingIndexStatement(
      ctx: DescribeSkippingIndexStatementContext): Command = {
    val outputSchema = Seq(
      AttributeReference("indexed_col_name", StringType, nullable = false)(),
      AttributeReference("data_type", StringType, nullable = false)(),
      AttributeReference("skip_type", StringType, nullable = false)())

    FlintSparkSqlCommand(outputSchema) { flint =>
      val indexName = getSkippingIndexName(ctx.tableName.getText)
      flint
        .describeIndex(indexName)
        .map { case index: FlintSparkSkippingIndex =>
          index.indexedColumns.map(strategy =>
            Row(strategy.columnName, strategy.columnType, strategy.kind.toString))
        }
        .getOrElse(Seq.empty)
    }
  }

  override def visitDropSkippingIndexStatement(ctx: DropSkippingIndexStatementContext): Command =
    FlintSparkSqlCommand() { flint =>
      val tableName = ctx.tableName.getText // TODO: handle schema name
      val indexName = getSkippingIndexName(tableName)
      flint.deleteIndex(indexName)
      Seq.empty
    }

  override def aggregateResult(aggregate: Command, nextResult: Command): Command =
    if (nextResult != null) nextResult else aggregate
}
