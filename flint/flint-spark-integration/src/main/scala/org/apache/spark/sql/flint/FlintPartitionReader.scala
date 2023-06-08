/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.apache.spark.sql.flint

import com.fasterxml.jackson.core.{JsonFactory, JsonParser}
import org.opensearch.flint.core.storage.FlintReader

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.json.{CreateJacksonParser, JSONOptionsInRead}
import org.apache.spark.sql.catalyst.util.{CaseInsensitiveMap, FailureSafeParser}
import org.apache.spark.sql.connector.read.PartitionReader
import org.apache.spark.sql.flint.config.FlintSparkConf
import org.apache.spark.sql.flint.datatype.FlintDataType.DATE_FORMAT_PARAMETERS
import org.apache.spark.sql.flint.json.FlintJacksonParser
import org.apache.spark.sql.types.StructType
import org.apache.spark.unsafe.types.UTF8String

/**
 * OpenSearchPartitionReader. Todo, add partition support.
 * @param tableName
 *   tableName
 * @param schema
 *   schema
 */
class FlintPartitionReader(reader: FlintReader, schema: StructType, options: FlintSparkConf)
    extends PartitionReader[InternalRow] {

  lazy val parser = new FlintJacksonParser(
    schema,
    new JSONOptionsInRead(CaseInsensitiveMap(DATE_FORMAT_PARAMETERS), options.timeZone, ""),
    allowArrayAsStructs = true)
  lazy val stringParser: (JsonFactory, String) => JsonParser =
    CreateJacksonParser.string(_: JsonFactory, _: String)
  lazy val safeParser = new FailureSafeParser[String](
    input => parser.parse(input, stringParser, UTF8String.fromString),
    parser.options.parseMode,
    schema,
    parser.options.columnNameOfCorruptRecord)

  var rows: Iterator[InternalRow] = Iterator.empty

  /**
   * Todo. consider multiple-line json.
   * @return
   */
  override def next: Boolean = {
    if (rows.hasNext) {
      true
    } else if (reader.hasNext) {
      rows = safeParser.parse(reader.next())
      rows.hasNext
    } else {
      false
    }
  }

  override def get(): InternalRow = {
    rows.next()
  }

  override def close(): Unit = {
    reader.close()
  }
}
