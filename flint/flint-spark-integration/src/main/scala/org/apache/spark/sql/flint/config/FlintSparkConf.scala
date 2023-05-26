/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.apache.spark.sql.flint.config

import java.util.{Map => JMap, NoSuchElementException}

import scala.collection.JavaConverters._

import org.opensearch.flint.core.FlintOptions

import org.apache.spark.internal.config.ConfigReader
import org.apache.spark.sql.RuntimeConfig
import org.apache.spark.sql.flint.config.FlintSparkConf._

/**
 * Define all the Flint Spark Related configuration. <p> User define the config as xxx.yyy using
 * {@link FlintConfig}.
 *
 * <p> How to use config <ol> <li> define config using spark.datasource.flint.xxx.yyy in spark
 * conf. <li> define config using xxx.yyy in datasource options. <li> Configurations defined in
 * the datasource options will override the same configurations present in the Spark
 * configuration. </ol>
 */
object FlintSparkConf {

  val PREFIX = "spark.datasource.flint."

  def apply(conf: JMap[String, String]): FlintSparkConf = new FlintSparkConf(conf)

  /**
   * Helper class, create {@link FlintOptions} from spark conf.
   */
  def apply(sparkConf: RuntimeConfig): FlintOptions = new FlintOptions(
    Seq(HOST_ENDPOINT, HOST_PORT, REFRESH_POLICY, SCROLL_SIZE)
      .map(conf => (conf.key, sparkConf.get(PREFIX + conf.key, conf.defaultValue.get)))
      .toMap
      .asJava)

  def sparkConf(key: String): String = PREFIX + key

  val HOST_ENDPOINT = FlintConfig("host")
    .createWithDefault("localhost")

  val HOST_PORT = FlintConfig("port")
    .createWithDefault("9200")

  val DOC_ID_COLUMN_NAME = FlintConfig("write.id_name")
    .doc(
      "spark write task use spark.flint.write.id.name defined column as doc id when write to " +
        "flint. if not provided, use system generated random id")
    .createOptional()

  val BATCH_SIZE = FlintConfig("write.batch_size")
    .doc(
      "The number of documents written to Flint in a single batch request is determined by the " +
        "overall size of the HTTP request, which should not exceed 100MB. The actual number of " +
        "documents will vary depending on the individual size of each document.")
    .createWithDefault("1000")

  val REFRESH_POLICY = FlintConfig("write.refresh_policy")
    .doc("refresh_policy, possible value are NONE(false), IMMEDIATE(true), WAIT_UNTIL(wait_for)")
    .createWithDefault("false")

  val SCROLL_SIZE = FlintConfig("read.scroll_size")
    .doc("scroll read size")
    .createWithDefault("100")
}

class FlintSparkConf(properties: JMap[String, String]) extends Serializable {

  lazy val reader = new ConfigReader(properties)

  def batchSize(): Int = BATCH_SIZE.readFrom(reader).toInt

  def docIdColumnName(): Option[String] = DOC_ID_COLUMN_NAME.readFrom(reader)

  def tableName(): String = {
    if (properties.containsKey("path")) properties.get("path")
    else throw new NoSuchElementException("index or path not found")
  }

  /**
   * Helper class, create {@link FlintOptions}.
   */
  def flintOptions(): FlintOptions = {
    new FlintOptions(
      Seq(HOST_ENDPOINT, HOST_PORT, REFRESH_POLICY, SCROLL_SIZE)
        .map(conf => (conf.key, conf.readFrom(reader)))
        .toMap
        .asJava)
  }
}
