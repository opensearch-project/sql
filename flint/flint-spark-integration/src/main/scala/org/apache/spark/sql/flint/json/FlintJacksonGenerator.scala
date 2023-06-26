/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.apache.spark.sql.flint.json

import java.io.Writer

import com.fasterxml.jackson.core.JsonFactory
import com.fasterxml.jackson.core.util.DefaultPrettyPrinter

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.SpecializedGetters
import org.apache.spark.sql.catalyst.json.JSONOptions
import org.apache.spark.sql.catalyst.util.{ArrayData, DateFormatter, DateTimeUtils, IntervalStringStyles, IntervalUtils, MapData, TimestampFormatter}
import org.apache.spark.sql.catalyst.util.LegacyDateFormats.FAST_DATE_FORMAT
import org.apache.spark.sql.errors.QueryExecutionErrors
import org.apache.spark.sql.types._

/**
 * copy from spark {@link JacksonGenerator}.
 *   1. Add ignoredFieldName, the column is ignored when write.
 */
case class FlintJacksonGenerator(
    dataType: DataType,
    writer: Writer,
    options: JSONOptions,
    ignoredFieldName: Option[String] = None) {
  // A `ValueWriter` is responsible for writing a field of an `InternalRow` to appropriate
  // JSON data. Here we are using `SpecializedGetters` rather than `InternalRow` so that
  // we can directly access data in `ArrayData` without the help of `SpecificMutableRow`.
  private type ValueWriter = (SpecializedGetters, Int) => Unit

  // `JackGenerator` can only be initialized with a `StructType`, a `MapType` or a `ArrayType`.
  require(
    dataType.isInstanceOf[StructType] || dataType.isInstanceOf[MapType]
      || dataType.isInstanceOf[ArrayType],
    s"JacksonGenerator only supports to be initialized with a ${StructType.simpleString}, " +
      s"${MapType.simpleString} or ${ArrayType.simpleString} but got ${dataType.catalogString}")

  // `ValueWriter`s for all fields of the schema
  private lazy val rootFieldWriters: Array[ValueWriter] = dataType match {
    case st: StructType => st.map(_.dataType).map(makeWriter).toArray
    case _ =>
      throw QueryExecutionErrors.initialTypeNotTargetDataTypeError(
        dataType,
        StructType.simpleString)
  }

  // `ValueWriter` for array data storing rows of the schema.
  private lazy val arrElementWriter: ValueWriter = dataType match {
    case at: ArrayType => makeWriter(at.elementType)
    case _: StructType | _: MapType => makeWriter(dataType)
    case _ => throw QueryExecutionErrors.initialTypeNotTargetDataTypesError(dataType)
  }

  private lazy val mapElementWriter: ValueWriter = dataType match {
    case mt: MapType => makeWriter(mt.valueType)
    case _ =>
      throw QueryExecutionErrors.initialTypeNotTargetDataTypeError(dataType, MapType.simpleString)
  }

  private val gen = {
    val generator = new JsonFactory().createGenerator(writer).setRootValueSeparator(null)
    if (options.pretty) {
      generator.setPrettyPrinter(new DefaultPrettyPrinter(""))
    }
    if (options.writeNonAsciiCharacterAsCodePoint) {
      generator.setHighestNonEscapedChar(0x7f)
    }
    generator
  }

  private val lineSeparator: String = options.lineSeparatorInWrite

  private val timestampFormatter = TimestampFormatter(
    options.timestampFormatInWrite,
    options.zoneId,
    options.locale,
    legacyFormat = FAST_DATE_FORMAT,
    isParsing = false)
  private val timestampNTZFormatter = TimestampFormatter(
    options.timestampNTZFormatInWrite,
    options.zoneId,
    legacyFormat = FAST_DATE_FORMAT,
    isParsing = false,
    forTimestampNTZ = true)
  private val dateFormatter = DateFormatter(
    options.dateFormatInWrite,
    options.locale,
    legacyFormat = FAST_DATE_FORMAT,
    isParsing = false)

  private def makeWriter(dataType: DataType): ValueWriter = dataType match {
    case NullType =>
      (row: SpecializedGetters, ordinal: Int) => gen.writeNull()

    case BooleanType =>
      (row: SpecializedGetters, ordinal: Int) => gen.writeBoolean(row.getBoolean(ordinal))

    case ByteType =>
      (row: SpecializedGetters, ordinal: Int) => gen.writeNumber(row.getByte(ordinal))

    case ShortType =>
      (row: SpecializedGetters, ordinal: Int) => gen.writeNumber(row.getShort(ordinal))

    case IntegerType =>
      (row: SpecializedGetters, ordinal: Int) => gen.writeNumber(row.getInt(ordinal))

    case LongType =>
      (row: SpecializedGetters, ordinal: Int) => gen.writeNumber(row.getLong(ordinal))

    case FloatType =>
      (row: SpecializedGetters, ordinal: Int) => gen.writeNumber(row.getFloat(ordinal))

    case DoubleType =>
      (row: SpecializedGetters, ordinal: Int) => gen.writeNumber(row.getDouble(ordinal))

    case StringType =>
      (row: SpecializedGetters, ordinal: Int) =>
        gen.writeString(row.getUTF8String(ordinal).toString)

    case TimestampType =>
      (row: SpecializedGetters, ordinal: Int) =>
        val timestampString = timestampFormatter.format(row.getLong(ordinal))
        gen.writeString(timestampString)

    case TimestampNTZType =>
      (row: SpecializedGetters, ordinal: Int) =>
        val timestampString =
          timestampNTZFormatter.format(DateTimeUtils.microsToLocalDateTime(row.getLong(ordinal)))
        gen.writeString(timestampString)

    case DateType =>
      (row: SpecializedGetters, ordinal: Int) =>
        val dateString = dateFormatter.format(row.getInt(ordinal))
        gen.writeString(dateString)

    case CalendarIntervalType =>
      (row: SpecializedGetters, ordinal: Int) =>
        gen.writeString(row.getInterval(ordinal).toString)

    case YearMonthIntervalType(start, end) =>
      (row: SpecializedGetters, ordinal: Int) =>
        val ymString = IntervalUtils.toYearMonthIntervalString(
          row.getInt(ordinal),
          IntervalStringStyles.ANSI_STYLE,
          start,
          end)
        gen.writeString(ymString)

    case DayTimeIntervalType(start, end) =>
      (row: SpecializedGetters, ordinal: Int) =>
        val dtString = IntervalUtils.toDayTimeIntervalString(
          row.getLong(ordinal),
          IntervalStringStyles.ANSI_STYLE,
          start,
          end)
        gen.writeString(dtString)

    case BinaryType =>
      (row: SpecializedGetters, ordinal: Int) => gen.writeBinary(row.getBinary(ordinal))

    case dt: DecimalType =>
      (row: SpecializedGetters, ordinal: Int) =>
        gen.writeNumber(row.getDecimal(ordinal, dt.precision, dt.scale).toJavaBigDecimal)

    case st: StructType =>
      val fieldWriters = st.map(_.dataType).map(makeWriter)
      (row: SpecializedGetters, ordinal: Int) =>
        writeObject(writeFields(row.getStruct(ordinal, st.length), st, fieldWriters))

    case at: ArrayType =>
      val elementWriter = makeWriter(at.elementType)
      (row: SpecializedGetters, ordinal: Int) =>
        writeArray(writeArrayData(row.getArray(ordinal), elementWriter))

    case mt: MapType =>
      val valueWriter = makeWriter(mt.valueType)
      (row: SpecializedGetters, ordinal: Int) =>
        writeObject(writeMapData(row.getMap(ordinal), mt, valueWriter))

      // For UDT values, they should be in the SQL type's corresponding value type.
      // We should not see values in the user-defined class at here.
      // For example, VectorUDT's SQL type is an array of double. So, we should expect that v is
      // an ArrayData at here, instead of a Vector.
    case t: UserDefinedType[_] =>
      makeWriter(t.sqlType)

    case _ =>
      (row: SpecializedGetters, ordinal: Int) =>
        val v = row.get(ordinal, dataType)
        throw QueryExecutionErrors.failToConvertValueToJsonError(v, v.getClass, dataType)
  }

  private def writeObject(f: => Unit): Unit = {
    gen.writeStartObject()
    f
    gen.writeEndObject()
  }

  private def writeFields(
      row: InternalRow,
      schema: StructType,
      fieldWriters: Seq[ValueWriter]): Unit = {
    var i = 0
    while (i < row.numFields) {
      val field = schema(i)
      if (!ignoredFieldName.contains(field.name)) {
        if (!row.isNullAt(i)) {
          gen.writeFieldName(field.name)
          fieldWriters(i).apply(row, i)
        } else if (!options.ignoreNullFields) {
          gen.writeFieldName(field.name)
          gen.writeNull()
        }
      }
      i += 1
    }
  }

  private def writeArray(f: => Unit): Unit = {
    gen.writeStartArray()
    f
    gen.writeEndArray()
  }

  private def writeArrayData(array: ArrayData, fieldWriter: ValueWriter): Unit = {
    var i = 0
    while (i < array.numElements()) {
      if (!array.isNullAt(i)) {
        fieldWriter.apply(array, i)
      } else {
        gen.writeNull()
      }
      i += 1
    }
  }

  private def writeMapData(map: MapData, mapType: MapType, fieldWriter: ValueWriter): Unit = {
    val keyArray = map.keyArray()
    val valueArray = map.valueArray()
    var i = 0
    while (i < map.numElements()) {
      gen.writeFieldName(keyArray.get(i, mapType.keyType).toString)
      if (!valueArray.isNullAt(i)) {
        fieldWriter.apply(valueArray, i)
      } else {
        gen.writeNull()
      }
      i += 1
    }
  }

  def close(): Unit = gen.close()

  def flush(): Unit = gen.flush()

  /**
   * Transforms a single `InternalRow` to JSON object using Jackson. This api calling will be
   * validated through accessing `rootFieldWriters`.
   *
   * @param row
   *   The row to convert
   */
  def write(row: InternalRow): Unit = {
    writeObject(
      writeFields(
        fieldWriters = rootFieldWriters,
        row = row,
        schema = dataType.asInstanceOf[StructType]))
  }

  /**
   * Transforms multiple `InternalRow`s or `MapData`s to JSON array using Jackson
   *
   * @param array
   *   The array of rows or maps to convert
   */
  def write(array: ArrayData): Unit = writeArray(writeArrayData(array, arrElementWriter))

  /**
   * Transforms a single `MapData` to JSON object using Jackson This api calling will will be
   * validated through accessing `mapElementWriter`.
   *
   * @param map
   *   a map to convert
   */
  def write(map: MapData): Unit = {
    writeObject(
      writeMapData(
        fieldWriter = mapElementWriter,
        map = map,
        mapType = dataType.asInstanceOf[MapType]))
  }

  def writeLineEnding(): Unit = {
    // Note that JSON uses writer with UTF-8 charset. This string will be written out as UTF-8.
    gen.writeRaw(lineSeparator)
  }

  /**
   * customized action. for instance. {"create": {"id": "value"}}
   */
  def writeAction(action: String, idOrdinal: Option[Int], row: InternalRow): Unit = {
    writeObject({
      gen.writeFieldName(action)
      writeObject(idOrdinal match {
        case Some(i) =>
          gen.writeFieldName("_id")
          rootFieldWriters(i).apply(row, i)
        case _ => None
      })
    })
  }
}
