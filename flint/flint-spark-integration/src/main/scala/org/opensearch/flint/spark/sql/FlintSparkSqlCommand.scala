/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark.sql

import org.opensearch.flint.spark.FlintSpark

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.catalyst.expressions.AttributeReference
import org.apache.spark.sql.execution.command.LeafRunnableCommand
import org.apache.spark.sql.types.StringType

/**
 * Flint Spark SQL command.
 *
 * Note that currently Flint SQL layer is thin with all core logic in FlintSpark. May create
 * separate command for each Flint SQL statement in future as needed.
 *
 * @param block
 *   code block that triggers Flint core API
 */
case class FlintSparkSqlCommand(block: FlintSpark => Seq[Row]) extends LeafRunnableCommand {

  override def run(sparkSession: SparkSession): Seq[Row] = block(new FlintSpark(sparkSession))
}

object FlintSparkSqlCommand {
  val DEFAULT_OUTPUT = Seq(AttributeReference("Result", StringType, nullable = true)())
}
