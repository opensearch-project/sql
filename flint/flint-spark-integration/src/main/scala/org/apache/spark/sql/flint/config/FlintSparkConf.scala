/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.apache.spark.sql.flint.config

import java.util
import java.util.{Map => JMap, NoSuchElementException}

import scala.collection.JavaConverters._

import org.opensearch.flint.core.FlintOptions

import org.apache.spark.internal.config.ConfigReader
import org.apache.spark.sql.flint.config.FlintSparkConf._
import org.apache.spark.sql.internal.SQLConf

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

  val DATASOURCE_FLINT_PREFIX = "spark.datasource.flint."

  /**
   * Create FlintSparkConf from Datasource options. if no options provided, FlintSparkConf will
   * read configuraiton from SQLConf.
   */
  def apply(options: JMap[String, String] = new util.HashMap[String, String]()): FlintSparkConf =
    new FlintSparkConf(options)

  val HOST_ENDPOINT = FlintConfig("spark.datasource.flint.host")
    .datasourceOption()
    .createWithDefault("localhost")

  val HOST_PORT = FlintConfig("spark.datasource.flint.port")
    .datasourceOption()
    .createWithDefault("9200")

  val SCHEME = FlintConfig("spark.datasource.flint.scheme")
    .datasourceOption()
    .doc("http or https")
    .createWithDefault("http")

  val AUTH = FlintConfig("spark.datasource.flint.auth")
    .datasourceOption()
    .doc("authentication type. supported value: NONE_AUTH(false), SIGV4_AUTH(sigv4)")
    .createWithDefault(FlintOptions.NONE_AUTH)

  val REGION = FlintConfig("spark.datasource.flint.region")
    .datasourceOption()
    .doc("AWS service region")
    .createWithDefault(FlintOptions.DEFAULT_REGION)

  val DOC_ID_COLUMN_NAME = FlintConfig("spark.datasource.flint.write.id_name")
    .datasourceOption()
    .doc(
      "spark write task use spark.flint.write.id.name defined column as doc id when write to " +
        "flint. if not provided, use system generated random id")
    .createOptional()

  val IGNORE_DOC_ID_COLUMN = FlintConfig("spark.datasource.flint.ignore.id_column")
    .datasourceOption()
    .doc("Enable spark write task ignore doc_id column. the default value is ture")
    .createWithDefault("true")

  val BATCH_SIZE = FlintConfig("spark.datasource.flint.write.batch_size")
    .datasourceOption()
    .doc(
      "The number of documents written to Flint in a single batch request is determined by the " +
        "overall size of the HTTP request, which should not exceed 100MB. The actual number of " +
        "documents will vary depending on the individual size of each document.")
    .createWithDefault("1000")

  val REFRESH_POLICY = FlintConfig("spark.datasource.flint.write.refresh_policy")
    .datasourceOption()
    .doc("refresh_policy, possible value are NONE(false), IMMEDIATE(true), WAIT_UNTIL(wait_for)")
    .createWithDefault("false")

  val SCROLL_SIZE = FlintConfig("spark.datasource.flint.read.scroll_size")
    .datasourceOption()
    .doc("scroll read size")
    .createWithDefault("100")

  val OPTIMIZER_RULE_ENABLED = FlintConfig("spark.flint.optimizer.enabled")
    .doc("Enable Flint optimizer rule for query rewrite with Flint index")
    .createWithDefault("true")
}

/**
 * if no options provided, FlintSparkConf read configuration from SQLConf.
 */
class FlintSparkConf(properties: JMap[String, String]) extends Serializable {

  @transient lazy val reader = new ConfigReader(properties)

  def batchSize(): Int = BATCH_SIZE.readFrom(reader).toInt

  def docIdColumnName(): Option[String] = DOC_ID_COLUMN_NAME.readFrom(reader)

  def ignoreIdColumn(): Boolean = IGNORE_DOC_ID_COLUMN.readFrom(reader).toBoolean

  def tableName(): String = {
    if (properties.containsKey("path")) properties.get("path")
    else throw new NoSuchElementException("index or path not found")
  }

  def isOptimizerEnabled: Boolean = OPTIMIZER_RULE_ENABLED.readFrom(reader).toBoolean

  /**
   * spark.sql.session.timeZone
   */
  def timeZone: String = SQLConf.get.sessionLocalTimeZone

  /**
   * Helper class, create {@link FlintOptions}.
   */
  def flintOptions(): FlintOptions = {
    new FlintOptions(
      Seq(HOST_ENDPOINT, HOST_PORT, REFRESH_POLICY, SCROLL_SIZE, SCHEME, AUTH, REGION)
        .map(conf => (conf.optionKey, conf.readFrom(reader)))
        .toMap
        .asJava)
  }
}
