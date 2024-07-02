/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.spark.rest.model;

import lombok.Data;
import org.apache.commons.lang3.Validate;

@Data
public class CreateAsyncQueryRequest {
  private String query;
  private String datasource;
  private LangType lang;
  // optional sessionId
  private String sessionId;

  public CreateAsyncQueryRequest(String query, String datasource, LangType lang) {
    this.query = Validate.notNull(query, "Query can't be null");
    this.datasource = Validate.notNull(datasource, "Datasource can't be null");
    this.lang = Validate.notNull(lang, "lang can't be null");
  }

  public CreateAsyncQueryRequest(String query, String datasource, LangType lang, String sessionId) {
    this.query = Validate.notNull(query, "Query can't be null");
    this.datasource = Validate.notNull(datasource, "Datasource can't be null");
    this.lang = Validate.notNull(lang, "lang can't be null");
    this.sessionId = sessionId;
  }
}
