/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.apache.spark.sql.flint.datatype

import org.json4s.JValue
import org.json4s.jackson.JsonMethods
import org.scalatest.matchers.should.Matchers

import org.apache.spark.FlintSuite
import org.apache.spark.sql.types._

class FlintDataTypeSuite extends FlintSuite with Matchers {
  test("basic deserialize and serialize") {
    val flintDataType = """{
                          |  "properties": {
                          |    "booleanField": {
                          |      "type": "boolean"
                          |    },
                          |    "keywordField": {
                          |      "type": "keyword"
                          |    },
                          |    "longField": {
                          |      "type": "long"
                          |    },
                          |    "integerField": {
                          |      "type": "integer"
                          |    },
                          |    "shortField": {
                          |      "type": "short"
                          |    },
                          |    "byteField": {
                          |      "type": "byte"
                          |    },
                          |    "doubleField": {
                          |      "type": "double"
                          |    },
                          |    "floatField": {
                          |      "type": "float"
                          |    },
                          |    "textField": {
                          |      "type": "text"
                          |    }
                          |  }
                          |}""".stripMargin
    val sparkStructType = StructType(
      StructField("booleanField", BooleanType, true) ::
        StructField("keywordField", StringType, true) ::
        StructField("longField", LongType, true) ::
        StructField("integerField", IntegerType, true) ::
        StructField("shortField", ShortType, true) ::
        StructField("byteField", ByteType, true) ::
        StructField("doubleField", DoubleType, true) ::
        StructField("floatField", FloatType, true) ::
        StructField(
          "textField",
          StringType,
          true,
          new MetadataBuilder().putString("osType", "text").build()) ::
        Nil)

    FlintDataType.serialize(sparkStructType) shouldBe compactJson(flintDataType)
    FlintDataType.deserialize(flintDataType) should contain theSameElementsAs sparkStructType
  }

  test("deserialize unsupported flint data type throw exception") {
    val unsupportedField = """{
      "properties": {
        "rangeField": {
          "type": "integer_range"
        }
      }
    }"""
    an[IllegalStateException] should be thrownBy FlintDataType.deserialize(unsupportedField)
  }

  test("flint object type deserialize and serialize") {
    val flintDataType = """{
                             |  "properties": {
                             |    "object1": {
                             |      "properties": {
                             |        "shortField": {
                             |          "type": "short"
                             |        }
                             |      }
                             |    },
                             |    "object2": {
                             |      "type": "object",
                             |      "properties": {
                             |        "integerField": {
                             |          "type": "integer"
                             |        }
                             |      }
                             |    }
                             |  }
                             |}""".stripMargin
    val sparkStructType = StructType(
      StructField("object1", StructType(StructField("shortField", ShortType) :: Nil)) ::
        StructField("object2", StructType(StructField("integerField", IntegerType) :: Nil)) ::
        Nil)

    FlintDataType.deserialize(flintDataType) should contain theSameElementsAs sparkStructType

    FlintDataType.serialize(sparkStructType) shouldBe compactJson("""{
                                                                    |  "properties": {
                                                                    |    "object1": {
                                                                    |      "properties": {
                                                                    |        "shortField": {
                                                                    |          "type": "short"
                                                                    |        }
                                                                    |      }
                                                                    |    },
                                                                    |    "object2": {
                                                                    |      "properties": {
                                                                    |        "integerField": {
                                                                    |          "type": "integer"
                                                                    |        }
                                                                    |      }
                                                                    |    }
                                                                    |  }
                                                                    |}""".stripMargin)
  }

  def compactJson(json: String): String = {
    val data: JValue = JsonMethods.parse(json)
    JsonMethods.compact(JsonMethods.render(data))
  }
}
