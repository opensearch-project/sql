/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.apache.spark.sql.flint.config

import org.apache.spark.internal.config.ConfigReader

/**
 * Similar to SPARK ConfigEntry. ConfigEntry register the configuration which can not been
 * modified.
 */
private case class FlintConfig(key: String) {

  private var doc = ""

  def doc(s: String): FlintConfig = {
    doc = s
    this
  }

  def createWithDefault(defaultValue: String): FlintConfigEntry[String] = {
    new FlintConfigEntryWithDefault(key, defaultValue, doc)
  }

  def createOptional(): FlintConfigEntry[Option[String]] = {
    new FlintOptionalConfigEntry(key, doc)
  }
}

abstract class FlintConfigEntry[T](val key: String, val doc: String) {
  protected def readString(reader: ConfigReader): Option[String] = {
    reader.get(key)
  }

  def readFrom(reader: ConfigReader): T

  def defaultValue: Option[String] = None
}

private class FlintConfigEntryWithDefault(key: String, defaultValue: String, doc: String)
    extends FlintConfigEntry[String](key, doc) {

  override def defaultValue: Option[String] = Some(defaultValue)

  def readFrom(reader: ConfigReader): String = {
    readString(reader).getOrElse(defaultValue)
  }
}

private class FlintOptionalConfigEntry(key: String, doc: String)
    extends FlintConfigEntry[Option[String]](key, doc) {

  def readFrom(reader: ConfigReader): Option[String] = {
    readString(reader)
  }
}
