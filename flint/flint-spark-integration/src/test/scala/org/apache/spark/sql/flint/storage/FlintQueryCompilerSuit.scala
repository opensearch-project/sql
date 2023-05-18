/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.apache.spark.sql.flint.storage

import org.apache.spark.FlintSuite
import org.apache.spark.sql.connector.expressions.{FieldReference, GeneralScalarExpression}
import org.apache.spark.sql.connector.expressions.filter.Predicate
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types._

class FlintQueryCompilerSuit extends FlintSuite {

  test("compile a list of expressions should successfully") {
    val query =
      FlintQueryCompiler(schema()).compile(
        Array(EqualTo("aInt", 1).toV2, EqualTo("aString", "s").toV2))
    assertResult(
      """{"bool":{"filter":[{"term":{"aInt":{"value":1}}},{"term":{"aString":{"value":"s"}}}]}}""")(
      query)
  }

  test("compile a list of expressions contain one expression should successfully") {
    val query =
      FlintQueryCompiler(schema()).compile(Array(EqualTo("aInt", 1).toV2))
    assertResult("""{"term":{"aInt":{"value":1}}}""")(query)
  }

  test("compile a empty list of expressions should return empty") {
    val query =
      FlintQueryCompiler(schema()).compile(Array.empty[Predicate])
    assert(query.isEmpty)
  }

  test("compile unsupported expression abs(aInt) should return empty string") {
    val query = FlintQueryCompiler(schema()).compile(
      // SPARK V2ExpressionBuilder define the expression.
      new GeneralScalarExpression("ABS", Array(FieldReference.apply("aInt"))))
    assert(query.isEmpty)
  }

  test("compile and(aInt=1, aString=s) should successfully") {
    val query =
      FlintQueryCompiler(schema()).compile(And(EqualTo("aInt", 1), EqualTo("aString", "s")).toV2)
    assertResult(
      """{"bool":{"filter":[{"term":{"aInt":{"value":1}}},{"term":{"aString":{"value":"s"}}}]}}""")(
      query)
  }

  test("compile or(aInt=1, aString=s) should successfully") {
    val query =
      FlintQueryCompiler(schema()).compile(Or(EqualTo("aInt", 1), EqualTo("aString", "s")).toV2)
    // scalastyle:off
    assertResult(
      """{"bool":{"should":[{"bool":{"filter":{"term":{"aInt":{"value":1}}}}},{"bool":{"filter":{"term":{"aString":{"value":"s"}}}}}]}}""")(
      query)
    // scalastyle:on
  }

  test("compile and(aInt>1, aString>s) should successfully") {
    val query =
      FlintQueryCompiler(schema()).compile(
        And(GreaterThan("aInt", 1), GreaterThan("aString", "s")).toV2)
    assertResult(
      """{"bool":{"filter":[{"range":{"aInt":{"gt":1}}},{"range":{"aString":{"gt":"s"}}}]}}""")(
      query)
  }

  test("compile and(aInt>=1, aString>=s) should successfully") {
    val query =
      FlintQueryCompiler(schema()).compile(
        And(GreaterThanOrEqual("aInt", 1), GreaterThanOrEqual("aString", "s")).toV2)
    assertResult(
      """{"bool":{"filter":[{"range":{"aInt":{"gte":1}}},{"range":{"aString":{"gte":"s"}}}]}}""")(
      query)
  }

  test("compile and(aInt<1, aString<s) should successfully") {
    val query =
      FlintQueryCompiler(schema()).compile(
        And(LessThan("aInt", 1), LessThan("aString", "s")).toV2)
    assertResult(
      """{"bool":{"filter":[{"range":{"aInt":{"lt":1}}},{"range":{"aString":{"lt":"s"}}}]}}""")(
      query)
  }

  test("compile and(aInt<=1, aString<=s) should successfully") {
    val query =
      FlintQueryCompiler(schema()).compile(
        And(LessThanOrEqual("aInt", 1), LessThanOrEqual("aString", "s")).toV2)
    assertResult(
      """{"bool":{"filter":[{"range":{"aInt":{"lte":1}}},{"range":{"aString":{"lte":"s"}}}]}}""")(
      query)
  }

  test("compile aInt IN (1, 2, 3) should successfully") {
    val query =
      FlintQueryCompiler(schema()).compile(In("aInt", Array(1, 2, 3)).toV2)
    assertResult("""{"terms":{"aInt":[1,2,3]}}""")(query)
  }

  test("compile STARTS_WITH(aString, s) should successfully") {
    val query =
      FlintQueryCompiler(schema()).compile(StringStartsWith("aString", "s").toV2)
    assertResult("""{"prefix":{"aString":{"value":"s"}}}""")(query)
  }

  test("compile CONTAINS(aString, s) should successfully") {
    val query =
      FlintQueryCompiler(schema()).compile(StringContains("aString", "s").toV2)
    assertResult("""{"wildcard":{"aString":{"value":"*s*"}}}""")(query)
  }

  test("compile CONTAINS(aText, s) should use match query") {
    val query =
      FlintQueryCompiler(schema()).compile(StringContains("aText", "s").toV2)
    assertResult("""{"match":{"aText":{"query":"s"}}}""")(query)
  }

  test("compile ENDS_WITH(aString, s) should successfully") {
    val query =
      FlintQueryCompiler(schema()).compile(StringEndsWith("aString", "s").toV2)
    assertResult("""{"wildcard":{"aString":{"value":"*s"}}}""")(query)
  }

  test("compile IS_NULL(aString) should successfully") {
    val query =
      FlintQueryCompiler(schema()).compile(IsNull("aString").toV2)
    assertResult("""{"bool":{"must_not":{"exists":{"field":"aString"}}}}""")(query)
  }

  test("compile IS_NOT_NULL(aString) should successfully") {
    val query =
      FlintQueryCompiler(schema()).compile(IsNotNull("aString").toV2)
    assertResult("""{"exists":{"field":"aString"}}""")(query)
  }

  protected def schema(): StructType = {
    StructType(
      Seq(
        StructField("aString", StringType, nullable = true),
        StructField("aInt", IntegerType, nullable = true),
        StructField(
          "aText",
          StringType,
          nullable = true,
          new MetadataBuilder().putString("osType", "text").build())))
  }
}
