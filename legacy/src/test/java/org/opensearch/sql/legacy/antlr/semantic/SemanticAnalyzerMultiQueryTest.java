/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.legacy.antlr.semantic;

import org.junit.Ignore;
import org.junit.Test;

/** Semantic analyzer tests for multi query like UNION and MINUS */
public class SemanticAnalyzerMultiQueryTest extends SemanticAnalyzerTestBase {

  @Test
  public void unionDifferentResultTypeOfTwoQueriesShouldFail() {
    expectValidationFailWithErrorMessages(
        "SELECT balance FROM semantics UNION SELECT address FROM semantics",
        "Operator [UNION] cannot work with [DOUBLE, TEXT].");
  }

  @Test
  public void unionDifferentNumberOfResultTypeOfTwoQueriesShouldFail() {
    expectValidationFailWithErrorMessages(
        "SELECT balance FROM semantics UNION SELECT balance, age FROM semantics",
        "Operator [UNION] cannot work with [DOUBLE, (DOUBLE, INTEGER)].");
  }

  @Test
  public void minusDifferentResultTypeOfTwoQueriesShouldFail() {
    expectValidationFailWithErrorMessages(
        "SELECT p.active FROM semantics s, s.projects p MINUS SELECT address FROM semantics",
        "Operator [MINUS] cannot work with [BOOLEAN, TEXT].");
  }

  @Test
  public void unionSameResultTypeOfTwoQueriesShouldPass() {
    validate("SELECT balance FROM semantics UNION SELECT balance FROM semantics");
  }

  @Test
  public void unionCompatibleResultTypeOfTwoQueriesShouldPass() {
    validate("SELECT balance FROM semantics UNION SELECT age FROM semantics");
    validate("SELECT address FROM semantics UNION ALL SELECT city FROM semantics");
  }

  @Test
  public void minusSameResultTypeOfTwoQueriesShouldPass() {
    validate(
        "SELECT s.projects.active FROM semantics s UNION SELECT p.active FROM semantics s,"
            + " s.projects p");
  }

  @Test
  public void minusCompatibleResultTypeOfTwoQueriesShouldPass() {
    validate("SELECT address FROM semantics MINUS SELECT manager.name.keyword FROM semantics");
  }

  @Test
  public void unionSelectStarWithExtraFieldOfTwoQueriesShouldFail() {
    expectValidationFailWithErrorMessages(
        "SELECT * FROM semantics UNION SELECT *, city FROM semantics",
        "Operator [UNION] cannot work with [(*), KEYWORD].");
  }

  @Test
  public void minusSelectStarWithExtraFieldOfTwoQueriesShouldFail() {
    expectValidationFailWithErrorMessages(
        "SELECT *, address, balance FROM semantics MINUS SELECT * FROM semantics",
        "Operator [MINUS] cannot work with [(TEXT, DOUBLE), (*)].");
  }

  @Test
  public void unionSelectStarOfTwoQueriesShouldPass() {
    validate("SELECT * FROM semantics UNION SELECT * FROM semantics");
    validate("SELECT *, age FROM semantics UNION SELECT *, balance FROM semantics");
  }

  @Test
  public void unionSelectFunctionCallWithSameReturnTypeOfTwoQueriesShouldPass() {
    validate("SELECT LOG(balance) FROM semantics UNION SELECT ABS(age) FROM semantics");
  }

  @Ignore("* is empty and ignored in product of select items for now")
  @Test
  public void unionSelectFieldWithExtraStarOfTwoQueriesShouldFail() {
    expectValidationFailWithErrorMessages(
        "SELECT age FROM semantics UNION SELECT *, age FROM semantics");
  }

  @Test
  public void unionSelectWithAliasOfTwoQueriesShouldPass() {
    validate(
        "SELECT balance AS numeric FROM semantics UNION SELECT balance AS numeric FROM semantics");
    validate("SELECT balance AS numeric FROM semantics UNION SELECT age AS numeric FROM semantics");
    validate("SELECT balance AS age FROM semantics UNION SELECT age FROM semantics");
    validate("SELECT balance FROM semantics UNION SELECT age AS balance FROM semantics");
  }
}
