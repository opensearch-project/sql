/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.legacy.query.join;

import java.util.List;
import org.opensearch.action.search.SearchRequestBuilder;
import org.opensearch.sql.legacy.domain.Field;
import org.opensearch.sql.legacy.domain.Select;

/** Created by Eliran on 28/8/2015. */
public class TableInJoinRequestBuilder {
  private SearchRequestBuilder requestBuilder;
  private String alias;
  private List<Field> returnedFields;
  private Select originalSelect;
  private Integer hintLimit;

  public TableInJoinRequestBuilder() {}

  public SearchRequestBuilder getRequestBuilder() {
    return requestBuilder;
  }

  public void setRequestBuilder(SearchRequestBuilder requestBuilder) {
    this.requestBuilder = requestBuilder;
  }

  public String getAlias() {
    return alias;
  }

  public void setAlias(String alias) {
    this.alias = alias;
  }

  public List<Field> getReturnedFields() {
    return returnedFields;
  }

  public void setReturnedFields(List<Field> returnedFields) {
    this.returnedFields = returnedFields;
  }

  public Select getOriginalSelect() {
    return originalSelect;
  }

  public void setOriginalSelect(Select originalSelect) {
    this.originalSelect = originalSelect;
  }

  public Integer getHintLimit() {
    return hintLimit;
  }

  public void setHintLimit(Integer hintLimit) {
    this.hintLimit = hintLimit;
  }
}
