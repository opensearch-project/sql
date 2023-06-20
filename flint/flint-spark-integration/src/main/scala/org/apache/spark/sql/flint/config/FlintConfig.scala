/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.apache.spark.sql.flint.config

import org.apache.spark.internal.config.ConfigReader
import org.apache.spark.sql.internal.SQLConf

/**
 * Similar to SPARK ConfigEntry. ConfigEntry register the configuration which can not been
 * modified.
 */
private case class FlintConfig(key: String) {

  private var doc = ""

  private var dataSourcePrefix: Option[String] = None

  val DATASOURCE_FLINT_PREFIX = "spark.datasource.flint."

  def doc(s: String): FlintConfig = {
    doc = s
    this
  }

  /**
   * if the configuration is datasource option also. which means user could define it using
   * dataframe.option() interface. for example, sql.read.format("flint").options(Map("host" ->
   * "localhost"))
   * @return
   */
  def datasourceOption(): FlintConfig = {
    dataSourcePrefix = Some(DATASOURCE_FLINT_PREFIX)
    this
  }

  def createWithDefault(defaultValue: String): FlintConfigEntry[String] = {
    new FlintConfigEntryWithDefault(key, defaultValue, doc, dataSourcePrefix)
  }

  def createOptional(): FlintConfigEntry[Option[String]] = {
    new FlintOptionalConfigEntry(key, doc, dataSourcePrefix)
  }
}

abstract class FlintConfigEntry[T](val key: String, val doc: String, val prefix: Option[String]) {

  protected val DATASOURCE_FLINT_PREFIX = "spark.datasource.flint."

  protected def readOptionKeyString(reader: ConfigReader): Option[String] = reader.get(optionKey)

  /**
   * Get configuration from {@link ConfigReader}
   */
  def readFrom(reader: ConfigReader): T

  /**
   * Get configuration defined by key from {@link SQLConf}.
   */
  protected def readFromConf(): Option[String] = {
    if (SQLConf.get.contains(key)) {
      Some(SQLConf.get.getConfString(key))
    } else {
      None
    }
  }

  def defaultValue: Option[T] = None

  /**
   * DataSource option key. for example, the key = spark.datasource.flint.host, prefix = spark
   * .datasource.flint. the optionKey is host.
   */
  def optionKey: String = prefix.map(key.stripPrefix(_)).getOrElse(key)
}

private class FlintConfigEntryWithDefault(
    key: String,
    defaultValue: String,
    doc: String,
    prefix: Option[String])
    extends FlintConfigEntry[String](key, doc, prefix) {

  override def defaultValue: Option[String] = Some(defaultValue)

  override def readFrom(reader: ConfigReader): String = {
    readOptionKeyString(reader)
      .orElse(readFromConf())
      .getOrElse(defaultValue)
  }
}

private class FlintOptionalConfigEntry(key: String, doc: String, prefix: Option[String])
    extends FlintConfigEntry[Option[String]](key, doc, prefix) {

  override def readFrom(reader: ConfigReader): Option[String] = {
    readOptionKeyString(reader)
      .orElse(readFromConf())
  }
}
