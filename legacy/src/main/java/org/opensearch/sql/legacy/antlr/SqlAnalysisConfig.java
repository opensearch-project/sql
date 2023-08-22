/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.legacy.antlr;

/** Configuration for SQL analysis. */
public class SqlAnalysisConfig {

  /** Is entire analyzer enabled to perform the analysis */
  private final boolean isAnalyzerEnabled;

  /** Is suggestion enabled for field name typo */
  private final boolean isFieldSuggestionEnabled;

  /** Skip entire analysis for index mapping larger than this threhold */
  private final int analysisThreshold;

  public SqlAnalysisConfig(
      boolean isAnalyzerEnabled, boolean isFieldSuggestionEnabled, int analysisThreshold) {
    this.isAnalyzerEnabled = isAnalyzerEnabled;
    this.isFieldSuggestionEnabled = isFieldSuggestionEnabled;
    this.analysisThreshold = analysisThreshold;
  }

  public boolean isAnalyzerEnabled() {
    return isAnalyzerEnabled;
  }

  public boolean isFieldSuggestionEnabled() {
    return isFieldSuggestionEnabled;
  }

  public int getAnalysisThreshold() {
    return analysisThreshold;
  }

  @Override
  public String toString() {
    return "SqlAnalysisConfig{"
        + "isAnalyzerEnabled="
        + isAnalyzerEnabled
        + ", isFieldSuggestionEnabled="
        + isFieldSuggestionEnabled
        + ", analysisThreshold="
        + analysisThreshold
        + '}';
  }
}
