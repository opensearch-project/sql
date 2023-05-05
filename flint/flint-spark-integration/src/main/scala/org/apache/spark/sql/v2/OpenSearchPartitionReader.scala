/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.apache.spark.sql.v2

import java.util.TimeZone

import scala.collection.JavaConverters._

import com.fasterxml.jackson.core.{JsonFactory, JsonParser}
import org.opensearch.io.{OpenSearchOptions, OpenSearchReader}

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.json.{CreateJacksonParser, JacksonParser, JSONOptionsInRead}
import org.apache.spark.sql.catalyst.util.{CaseInsensitiveMap, FailureSafeParser}
import org.apache.spark.sql.connector.read.PartitionReader
import org.apache.spark.sql.types.StructType
import org.apache.spark.unsafe.types.UTF8String

/**
 * OpenSearchPartitionReader. Todo, add partition support.
 * @param tableName
 *   tableName
 * @param schema
 *   schema
 */
class OpenSearchPartitionReader(
    tableName: String,
    schema: StructType,
    options: Map[String, String])
    extends PartitionReader[InternalRow] {

  lazy val parser = new JacksonParser(
    schema,
    new JSONOptionsInRead(
      CaseInsensitiveMap(Map.empty[String, String]),
      TimeZone.getDefault.getID,
      ""),
    allowArrayAsStructs = true)
  lazy val stringParser: (JsonFactory, String) => JsonParser =
    CreateJacksonParser.string(_: JsonFactory, _: String)
  lazy val safeParser = new FailureSafeParser[String](
    input => parser.parse(input, stringParser, UTF8String.fromString),
    parser.options.parseMode,
    schema,
    parser.options.columnNameOfCorruptRecord)

  lazy val openSearchReader = {
    val reader = new OpenSearchReader(tableName, new OpenSearchOptions(options.asJava))
    reader.open()
    reader
  }

  var rows: Iterator[InternalRow] = Iterator.empty

  /**
   * Todo. consider multiple-line json.
   * @return
   */
  override def next: Boolean = {
    if (rows.hasNext) {
      true
    } else if (openSearchReader.hasNext) {
      rows = safeParser.parse(openSearchReader.next())
      rows.hasNext
    } else {
      false
    }
  }

  override def get(): InternalRow = {
    rows.next()
  }

  override def close(): Unit = {
    openSearchReader.close()
  }
}
