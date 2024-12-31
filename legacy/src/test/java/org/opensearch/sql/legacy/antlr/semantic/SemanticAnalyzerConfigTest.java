/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.legacy.antlr.semantic;

import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.not;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.opensearch.sql.legacy.antlr.OpenSearchLegacySqlAnalyzer;
import org.opensearch.sql.legacy.antlr.SqlAnalysisConfig;
import org.opensearch.sql.legacy.esdomain.LocalClusterState;

/** Test cases for semantic analysis configuration */
public class SemanticAnalyzerConfigTest extends SemanticAnalyzerTestBase {

  @Rule public final ExpectedException exceptionWithoutSuggestion = ExpectedException.none();

  @Test
  public void noAnalysisShouldPerformForNonSelectStatement() {
    String sql = "DELETE FROM semantics WHERE age12 = 123";
    expectValidationPassWithConfig(sql, new SqlAnalysisConfig(true, true, 1000));
  }

  @Test
  public void noAnalysisShouldPerformIfDisabledAnalysis() {
    String sql = "SELECT * FROM semantics WHERE age12 = 123";
    expectValidationFailWithErrorMessages(sql, "Field [age12] cannot be found or used here.");
    expectValidationPassWithConfig(sql, new SqlAnalysisConfig(false, true, 1000));
  }

  @Test
  public void noFieldNameSuggestionIfDisabledSuggestion() {
    String sql = "SELECT * FROM semantics WHERE age12 = 123";
    expectValidationFailWithErrorMessages(
        sql, "Field [age12] cannot be found or used here.", "Did you mean [age]?");

    exceptionWithoutSuggestion.expect(SemanticAnalysisException.class);
    exceptionWithoutSuggestion.expectMessage(
        allOf(
            containsString("Field [age12] cannot be found or used here"),
            not(containsString("Did you mean"))));
    new OpenSearchLegacySqlAnalyzer(new SqlAnalysisConfig(true, false, 1000))
        .analyze(sql, LocalClusterState.state());
  }

  @Test
  public void noAnalysisShouldPerformIfIndexMappingIsLargerThanThreshold() {
    String sql = "SELECT * FROM semantics WHERE test = 123";
    expectValidationFailWithErrorMessages(sql, "Field [test] cannot be found or used here.");
    expectValidationPassWithConfig(sql, new SqlAnalysisConfig(true, true, 1));
  }

  private void expectValidationPassWithConfig(String sql, SqlAnalysisConfig config) {
    new OpenSearchLegacySqlAnalyzer(config).analyze(sql, LocalClusterState.state());
  }
}
