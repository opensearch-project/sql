/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.apache.spark.sql.flint.datatype

import org.json4s.{Formats, JField, JValue, NoTypeHints}
import org.json4s.JsonAST.{JNothing, JObject, JString}
import org.json4s.jackson.JsonMethods
import org.json4s.native.Serialization

import org.apache.spark.sql.types._

object FlintDataType {

  implicit val formats: Formats = Serialization.formats(NoTypeHints)

  def deserialize(metadata: String): StructType = {
    deserializeJValue(JsonMethods.parse(metadata))
  }

  def deserializeJValue(json: JValue): StructType = {
    val properties = (json \ "properties").extract[Map[String, JValue]]
    val fields = properties.map { case (fieldName, fieldProperties) =>
      deserializeFiled(fieldName, fieldProperties)
    }

    StructType(fields.toSeq)
  }

  def deserializeFiled(fieldName: String, fieldProperties: JValue): StructField = {
    val metadataBuilder = new MetadataBuilder()
    val dataType = fieldProperties \ "type" match {
      // boolean
      case JString("boolean") => BooleanType

      // Keywords
      case JString("keyword") => StringType

      // Numbers
      case JString("long") => LongType
      case JString("integer") => IntegerType
      case JString("short") => ShortType
      case JString("byte") => ByteType
      case JString("double") => DoubleType
      case JString("float") => FloatType

      // Dates
      case JString("date") =>
        metadataBuilder.putString("format", (fieldProperties \ "format").extract[String])
        DateType

      // Text
      case JString("text") =>
        metadataBuilder.putString("osType", "text")
        StringType

      // object types
      case JString("object") | JNothing => deserializeJValue(fieldProperties)

      // not supported
      case _ => throw new IllegalStateException(s"unsupported data type")
    }
    DataTypes.createStructField(fieldName, dataType, true, metadataBuilder.build())
  }

  def serialize(structType: StructType): String = {
    val jValue = serializeJValue(structType)
    JsonMethods.compact(JsonMethods.render(jValue))
  }

  def serializeJValue(structType: StructType): JValue = {
    JObject("properties" -> JObject(structType.fields.map(field => serializeField(field)).toList))
  }

  def serializeField(structField: StructField): JField = {
    val metadata = structField.metadata
    val dataType = structField.dataType match {
      // boolean
      case BooleanType => JObject("type" -> JString("boolean"))

      // string
      case StringType =>
        if (metadata.contains("osType") && metadata.getString("osType") == "text") {
          JObject("type" -> JString("text"))
        } else {
          JObject("type" -> JString("keyword"))
        }

      // Numbers
      case LongType => JObject("type" -> JString("long"))
      case IntegerType => JObject("type" -> JString("integer"))
      case ShortType => JObject("type" -> JString("short"))
      case ByteType => JObject("type" -> JString("byte"))
      case DoubleType => JObject("type" -> JString("double"))
      case FloatType => JObject("type" -> JString("float"))

      // date
      case DateType =>
        JObject("type" -> JString("date"), "format" -> JString(metadata.getString("format")))

      // objects
      case st: StructType => serializeJValue(st)
      case _ => throw new IllegalStateException(s"unsupported data type")
    }
    JField(structField.name, dataType)
  }
}
