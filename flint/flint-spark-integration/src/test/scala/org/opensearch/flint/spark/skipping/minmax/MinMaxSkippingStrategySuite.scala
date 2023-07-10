/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark.skipping.minmax

import org.scalatest.matchers.should.Matchers

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.catalyst.expressions.{And, AttributeReference, EqualTo, GreaterThan, GreaterThanOrEqual, In, LessThan, LessThanOrEqual, Literal, Or}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.IntegerType

class MinMaxSkippingStrategySuite extends SparkFunSuite with Matchers {

  private val strategy = MinMaxSkippingStrategy(columnName = "age", columnType = "integer")

  private val indexCol = AttributeReference("age", IntegerType, nullable = false)()

  private val minCol = col("MinMax_age_0").expr
  private val maxCol = col("MinMax_age_1").expr

  test("should rewrite EqualTo(<indexCol>, <value>)") {
    strategy.rewritePredicate(EqualTo(indexCol, Literal(30))) shouldBe Some(
      And(LessThanOrEqual(minCol, Literal(30)), GreaterThanOrEqual(maxCol, Literal(30))))
  }

  test("should rewrite LessThan(<indexCol>, <value>)") {
    strategy.rewritePredicate(LessThan(indexCol, Literal(30))) shouldBe Some(
      LessThan(minCol, Literal(30)))
  }

  test("should rewrite LessThanOrEqual(<indexCol>, <value>)") {
    strategy.rewritePredicate(LessThanOrEqual(indexCol, Literal(30))) shouldBe Some(
      LessThanOrEqual(minCol, Literal(30)))
  }

  test("should rewrite GreaterThan(<indexCol>, <value>)") {
    strategy.rewritePredicate(GreaterThan(indexCol, Literal(30))) shouldBe Some(
      GreaterThan(maxCol, Literal(30)))
  }

  test("should rewrite GreaterThanOrEqual(<indexCol>, <value>)") {
    strategy.rewritePredicate(GreaterThanOrEqual(indexCol, Literal(30))) shouldBe Some(
      GreaterThanOrEqual(maxCol, Literal(30)))
  }

  test("should rewrite In(<indexCol>, <value1, value2 ...>") {
    strategy.rewritePredicate(In(indexCol, Seq(Literal(25), Literal(30)))) shouldBe Some(
      Or(
        And(LessThanOrEqual(minCol, Literal(25)), GreaterThanOrEqual(maxCol, Literal(25))),
        And(LessThanOrEqual(minCol, Literal(30)), GreaterThanOrEqual(maxCol, Literal(30)))))
  }
}
