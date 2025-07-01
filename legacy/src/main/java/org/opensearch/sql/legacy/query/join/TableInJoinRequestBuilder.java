/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.legacy.query.join;

import java.util.List;
import java.util.Optional;
import org.opensearch.action.search.SearchRequestBuilder;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.sql.legacy.domain.Field;
import org.opensearch.sql.legacy.domain.Select;
import org.opensearch.sql.legacy.query.planner.core.Config;

/** Created by Eliran on 28/8/2015. */
public class TableInJoinRequestBuilder {
  private SearchRequestBuilder requestBuilder;
  private String alias;
  private List<Field> returnedFields;
  private Select originalSelect;
  private Integer hintLimit;
  private Config hintConfig;

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

  /**
   * Get the hint configuration containing JOIN_TIME_OUT and other hint values
   *
   * @return Config object with hint values, or null if not set
   */
  public Config getHintConfig() {
    return hintConfig;
  }

  /**
   * Set the hint configuration containing JOIN_TIME_OUT and other hint values
   *
   * @param hintConfig Config object with hint values
   */
  public void setHintConfig(Config hintConfig) {
    this.hintConfig = hintConfig;
  }

  /**
   * Check if this table has a custom JOIN_TIME_OUT hint configured
   *
   * @return true if JOIN_TIME_OUT hint is present
   */
  public boolean hasJoinTimeoutHint() {
    return hintConfig != null && hintConfig.hasCustomPitKeepAlive();
  }

  /**
   * Get the JOIN_TIME_OUT hint value if present
   *
   * @return Optional containing the timeout value, or empty if not set
   */
  public Optional<TimeValue> getJoinTimeoutHint() {
    if (hintConfig != null) {
      return hintConfig.getCustomPitKeepAlive();
    }
    return Optional.empty();
  }
}
