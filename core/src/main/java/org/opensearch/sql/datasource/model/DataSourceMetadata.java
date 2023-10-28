/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.datasource.model;

import static org.opensearch.sql.analysis.DataSourceSchemaIdentifierNameResolver.DEFAULT_DATASOURCE_NAME;

import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableMap;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;
import org.apache.commons.lang3.StringUtils;
import org.opensearch.sql.datasource.DataSourceService;

@Getter
@Setter
@EqualsAndHashCode
@JsonIgnoreProperties(ignoreUnknown = true)
public class DataSourceMetadata {

  public static final String DEFAULT_RESULT_INDEX = "query_execution_result";
  public static final int MAX_RESULT_INDEX_NAME_SIZE = 255;
  // OS doesn’t allow uppercase: https://tinyurl.com/yse2xdbx
  public static final String RESULT_INDEX_NAME_PATTERN = "[a-z0-9_-]+";
  public static String INVALID_RESULT_INDEX_NAME_SIZE =
      "Result index name size must contains less than "
          + MAX_RESULT_INDEX_NAME_SIZE
          + " characters";
  public static String INVALID_CHAR_IN_RESULT_INDEX_NAME =
      "Result index name has invalid character. Valid characters are a-z, 0-9, -(hyphen) and"
          + " _(underscore)";
  public static String INVALID_RESULT_INDEX_PREFIX =
      "Result index must start with " + DEFAULT_RESULT_INDEX;

  @JsonProperty private String name;

  @JsonProperty private String description;

  @JsonProperty
  @JsonFormat(with = JsonFormat.Feature.ACCEPT_CASE_INSENSITIVE_PROPERTIES)
  private DataSourceType connector;

  @JsonProperty private List<String> allowedRoles;

  @JsonProperty private Map<String, String> properties;

  @JsonProperty private String resultIndex;

  public static Function<String, String> DATASOURCE_TO_RESULT_INDEX =
      datasourceName -> String.format("%s_%s", DEFAULT_RESULT_INDEX, datasourceName);

  public DataSourceMetadata(
      String name,
      String description,
      DataSourceType connector,
      List<String> allowedRoles,
      Map<String, String> properties,
      String resultIndex) {
    this.name = name;
    String errorMessage = validateCustomResultIndex(resultIndex);
    if (errorMessage != null) {
      throw new IllegalArgumentException(errorMessage);
    }
    if (resultIndex == null) {
      this.resultIndex = fromNameToCustomResultIndex();
    } else {
      this.resultIndex = resultIndex;
    }

    this.connector = connector;
    this.description = description;
    this.properties = properties;
    this.allowedRoles = allowedRoles;
  }

  public DataSourceMetadata() {
    this.description = StringUtils.EMPTY;
    this.allowedRoles = new ArrayList<>();
    this.properties = new HashMap<>();
  }

  /**
   * Default OpenSearch {@link DataSourceMetadata}. Which is used to register default OpenSearch
   * {@link DataSource} to {@link DataSourceService}.
   */
  public static DataSourceMetadata defaultOpenSearchDataSourceMetadata() {
    return new DataSourceMetadata(
        DEFAULT_DATASOURCE_NAME,
        StringUtils.EMPTY,
        DataSourceType.OPENSEARCH,
        Collections.emptyList(),
        ImmutableMap.of(),
        null);
  }

  public String validateCustomResultIndex(String resultIndex) {
    if (resultIndex == null) {
      return null;
    }
    if (resultIndex.length() > MAX_RESULT_INDEX_NAME_SIZE) {
      return INVALID_RESULT_INDEX_NAME_SIZE;
    }
    if (!resultIndex.matches(RESULT_INDEX_NAME_PATTERN)) {
      return INVALID_CHAR_IN_RESULT_INDEX_NAME;
    }
    if (resultIndex != null && !resultIndex.startsWith(DEFAULT_RESULT_INDEX)) {
      return INVALID_RESULT_INDEX_PREFIX;
    }
    return null;
  }

  /**
   * Since we are using datasource name to create result index, we need to make sure that the final
   * name is valid
   *
   * @param resultIndex result index name
   * @return valid result index name
   */
  private String convertToValidResultIndex(String resultIndex) {
    // Limit Length
    if (resultIndex.length() > MAX_RESULT_INDEX_NAME_SIZE) {
      resultIndex = resultIndex.substring(0, MAX_RESULT_INDEX_NAME_SIZE);
    }

    // Pattern Matching: Remove characters that don't match the pattern
    StringBuilder validChars = new StringBuilder();
    for (char c : resultIndex.toCharArray()) {
      if (String.valueOf(c).matches(RESULT_INDEX_NAME_PATTERN)) {
        validChars.append(c);
      }
    }
    return validChars.toString();
  }

  public String fromNameToCustomResultIndex() {
    if (name == null) {
      throw new IllegalArgumentException("Datasource name cannot be null");
    }
    return convertToValidResultIndex(DATASOURCE_TO_RESULT_INDEX.apply(name.toLowerCase()));
  }
}
