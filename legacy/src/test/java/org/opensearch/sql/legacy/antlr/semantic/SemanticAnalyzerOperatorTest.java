/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.legacy.antlr.semantic;

import org.junit.Test;

/** Semantic analysis test cases for operator */
public class SemanticAnalyzerOperatorTest extends SemanticAnalyzerTestBase {

  @Test
  public void compareNumberIsBooleanShouldFail() {
    expectValidationFailWithErrorMessages(
        "SELECT * FROM semantics WHERE age IS FALSE",
        "Operator [IS] cannot work with [INTEGER, BOOLEAN].");
  }

  @Test
  public void compareTextIsNotBooleanShouldFail() {
    expectValidationFailWithErrorMessages(
        "SELECT * FROM semantics WHERE address IS NOT TRUE",
        "Operator [IS] cannot work with [TEXT, BOOLEAN].");
  }

  @Test
  public void compareNumberEqualsToStringShouldFail() {
    expectValidationFailWithErrorMessages(
        "SELECT * FROM semantics WHERE balance = 'test'",
        "Operator [=] cannot work with [DOUBLE, STRING].");
  }

  @Test
  public void compareSubstringFunctionCallEqualsToNumberShouldFail() {
    expectValidationFailWithErrorMessages(
        "SELECT * FROM semantics WHERE SUBSTRING(address, 0, 3) = 1",
        "Operator [=] cannot work with [TEXT, INTEGER].");
  }

  @Test
  public void compareLogFunctionCallWithIntegerSmallerThanStringShouldFail() {
    expectValidationFailWithErrorMessages(
        "SELECT * FROM semantics WHERE LOG(age) < 'test'",
        "Operator [<] cannot work with [DOUBLE, STRING].");
  }

  @Test
  public void compareDoubleWithIntegerShouldPass() {
    validate("SELECT * FROM semantics WHERE balance >= 1000");
    validate("SELECT * FROM semantics WHERE balance <> 1000");
    validate("SELECT * FROM semantics WHERE balance != 1000");
  }

  @Test
  public void compareDateWithStringShouldPass() {
    validate("SELECT * FROM semantics WHERE birthday = '2019-09-30'");
  }

  @Test
  public void namedArgumentShouldSkipOperatorTypeCheck() {
    validate("SELECT TOPHITS('size'=3, age='desc') FROM semantics GROUP BY city");
  }
}
