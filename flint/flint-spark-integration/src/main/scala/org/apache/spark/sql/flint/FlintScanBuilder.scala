/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.apache.spark.sql.flint

import org.apache.spark.internal.Logging
import org.apache.spark.sql.connector.expressions.filter.Predicate
import org.apache.spark.sql.connector.read.{Scan, ScanBuilder, SupportsPushDownV2Filters}
import org.apache.spark.sql.flint.config.FlintSparkConf
import org.apache.spark.sql.flint.storage.FlintQueryCompiler
import org.apache.spark.sql.types.StructType

case class FlintScanBuilder(tableName: String, schema: StructType, options: FlintSparkConf)
    extends ScanBuilder
    with SupportsPushDownV2Filters
    with Logging {

  private var pushedPredicate = Array.empty[Predicate]

  override def build(): Scan = {
    FlintScan(tableName, schema, options, pushedPredicate)
  }

  override def pushPredicates(predicates: Array[Predicate]): Array[Predicate] = {
    val (pushed, unSupported) =
      predicates.partition(FlintQueryCompiler(schema).compile(_).nonEmpty)
    pushedPredicate = pushed
    unSupported
  }

  override def pushedPredicates(): Array[Predicate] = pushedPredicate
}
