/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.legacy.antlr.semantic;

import org.junit.Test;
import org.opensearch.sql.legacy.antlr.visitor.EarlyExitAnalysisException;

/** Semantic analysis test for subquery */
public class SemanticAnalyzerSubqueryTest extends SemanticAnalyzerTestBase {

  @Test
  public void useExistClauseOnNestedFieldShouldPass() {
    validate(
        "SELECT * FROM semantics AS s WHERE EXISTS "
            + " ( SELECT * FROM s.projects AS p WHERE p.active IS TRUE ) "
            + " AND s.age > 10");
  }

  @Test
  public void useNotExistClauseOnNestedFieldShouldPass() {
    validate(
        "SELECT * FROM semantics AS s WHERE NOT EXISTS "
            + " ( SELECT * FROM s.projects AS p WHERE p.active IS TRUE ) "
            + " AND s.age > 10");
  }

  @Test
  public void useInClauseOnAgeWithIntegerLiteralListShouldPass() {
    validate("SELECT * FROM semantics WHERE age IN (30, 40)");
  }

  @Test
  public void useAliasInSubqueryShouldFail() {
    expectValidationFailWithErrorMessages(
        "SELECT * FROM semantics s WHERE EXISTS (SELECT * FROM s.projects p) AND p.active IS TRUE",
        "Field [p.active] cannot be found or used here.",
        "Did you mean [projects.active]?");
  }

  @Test
  public void useInClauseWithIncompatibleFieldTypesShouldFail() {
    expectValidationFailWithErrorMessages(
        "SELECT * FROM semantics s WHERE age IN (SELECT p.active FROM s.projects p)",
        "Operator [IN] cannot work with [INTEGER, BOOLEAN].");
  }

  @Test
  public void useInClauseWithCompatibleFieldTypesShouldPass() {
    validate("SELECT * FROM semantics s WHERE address IN (SELECT city FROM s.projects p)");
  }

  @Test
  public void useNotInClauseWithCompatibleFieldTypesShouldPass() {
    validate("SELECT * FROM semantics s WHERE address NOT IN (SELECT city FROM s.projects p)");
  }

  @Test
  public void useInClauseWithCompatibleConstantShouldPass() {
    validate("SELECT * FROM semantics WHERE age IN (10, 20, 30)");
    validate("SELECT * FROM semantics WHERE city IN ('Seattle', 'Bellevue')");
    validate("SELECT * FROM semantics WHERE birthday IN ('2000-01-01', '2010-01-01')");
  }

  @Test
  public void useInClauseWithIncompatibleConstantShouldPass() {
    expectValidationFailWithErrorMessages(
        "SELECT * FROM semantics s WHERE age IN ('abc', 'def')",
        "Operator [IN] cannot work with [INTEGER, STRING].");
  }

  @Test
  public void useInClauseWithSelectStarShouldFail() {
    expectValidationFailWithErrorMessages(
        "SELECT * FROM semantics s WHERE address IN (SELECT * FROM s.projects p)",
        "Operator [IN] cannot work with [TEXT, (*)]");
  }

  @Test
  public void useExistsClauseWithSelectStarShouldPass() {
    validate("SELECT * FROM semantics s WHERE EXISTS (SELECT * FROM s.projects p)");
  }

  @Test
  public void useExistsClauseWithSelectConstantShouldPass() {
    validate("SELECT * FROM semantics s WHERE EXISTS (SELECT 1 FROM s.projects p)");
  }

  /** Ignore the semantic analyzer by using {@link EarlyExitAnalysisException} */
  @Test
  public void useSubqueryInFromClauseWithSelectConstantShouldPass() {
    validate("SELECT t.TEMP as count FROM (SELECT COUNT(*) as TEMP FROM semantics) t");
  }
}
