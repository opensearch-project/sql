/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.apache.spark

import org.opensearch.flint.spark.FlintSparkExtensions

import org.apache.spark.sql.catalyst.expressions.CodegenObjectFactoryMode
import org.apache.spark.sql.catalyst.optimizer.ConvertToLocalRelation
import org.apache.spark.sql.flint.config.FlintConfigEntry
import org.apache.spark.sql.flint.config.FlintSparkConf.HYBRID_SCAN_ENABLED
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession

trait FlintSuite extends SharedSparkSession {
  override protected def sparkConf = {
    val conf = new SparkConf()
      .set("spark.ui.enabled", "false")
      .set(SQLConf.CODEGEN_FALLBACK.key, "false")
      .set(SQLConf.CODEGEN_FACTORY_MODE.key, CodegenObjectFactoryMode.CODEGEN_ONLY.toString)
      // Disable ConvertToLocalRelation for better test coverage. Test cases built on
      // LocalRelation will exercise the optimization rules better by disabling it as
      // this rule may potentially block testing of other optimization rules such as
      // ConstantPropagation etc.
      .set(SQLConf.OPTIMIZER_EXCLUDED_RULES.key, ConvertToLocalRelation.ruleName)
      .set("spark.sql.extensions", classOf[FlintSparkExtensions].getName)
    conf
  }

  /**
   * Set Flint Spark configuration. (Generic "value: T" has problem with FlintConfigEntry[Any])
   */
  protected def setFlintSparkConf[T](config: FlintConfigEntry[T], value: Any): Unit = {
    spark.conf.set(config.key, value.toString)
  }

  protected def withHybridScanEnabled(block: => Unit): Unit = {
    setFlintSparkConf(HYBRID_SCAN_ENABLED, "true")
    try {
      block
    } finally {
      setFlintSparkConf(HYBRID_SCAN_ENABLED, "false")
    }
  }
}
