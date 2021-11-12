/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.doctest.core.builder;

/**
 * Example value object.
 */
public class Example {

  /**
   * Title for the example section
   */
  private String title;

  /**
   * Description for the example
   */
  private String description;

  /**
   * Sample SQL query
   */
  private String query;

  /**
   * Query result set
   */
  private String result;

  /**
   * Is result set formatted in table (markup handle table in different way
   */
  private boolean isTable;

  /**
   * Explain query correspondent to the sample query
   */
  private String explainQuery;

  /**
   * Result of explain
   */
  private String explainResult;

  public String getTitle() {
    return title;
  }

  public String getDescription() {
    return description;
  }

  public String getQuery() {
    return query;
  }

  public String getResult() {
    return result;
  }

  public boolean isTable() {
    return isTable;
  }

  public String getExplainQuery() {
    return explainQuery;
  }

  public String getExplainResult() {
    return explainResult;
  }

  public void setTitle(String title) {
    this.title = title;
  }

  public void setDescription(String description) {
    this.description = description;
  }

  public void setQuery(String query) {
    this.query = query;
  }

  public void setResult(String result) {
    this.result = result;
  }

  public void setTable(boolean table) {
    isTable = table;
  }

  public void setExplainQuery(String explainQuery) {
    this.explainQuery = explainQuery;
  }

  public void setExplainResult(String explainResult) {
    this.explainResult = explainResult;
  }
}
