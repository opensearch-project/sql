/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark.sql

import org.opensearch.flint.spark.skipping.FlintSparkSkippingIndex
import org.opensearch.flint.spark.sql.FlintSparkSqlExtensionsParser.DropSkippingIndexStatementContext

import org.apache.spark.sql.catalyst.plans.logical.Command

/**
 * Flint Spark AST builder that builds Spark command for Flint index statement.
 */
class FlintSparkSqlCommandBuilder extends FlintSparkSqlExtensionsBaseVisitor[Command] {

  override def visitDropSkippingIndexStatement(
      ctx: DropSkippingIndexStatementContext): Command = {
    FlintSparkSqlCommand { flint =>
      val tableName = ctx.tableName.getText
      val indexName = FlintSparkSkippingIndex.getSkippingIndexName(tableName)
      flint.deleteIndex(indexName)
      Seq.empty
    }
  }

  override def aggregateResult(aggregate: Command, nextResult: Command): Command =
    if (nextResult != null) nextResult else aggregate;
}
