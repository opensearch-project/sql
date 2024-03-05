/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.datasource.model;

import static org.opensearch.sql.analysis.DataSourceSchemaIdentifierNameResolver.DEFAULT_DATASOURCE_NAME;

import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import org.apache.commons.lang3.StringUtils;
import org.opensearch.sql.datasource.DataSourceService;

@Getter
@EqualsAndHashCode
@JsonIgnoreProperties(ignoreUnknown = true)
public class DataSourceMetadata {

  public static final String DEFAULT_RESULT_INDEX = "query_execution_result";
  public static final int MAX_RESULT_INDEX_NAME_SIZE = 255;
  private static String DATASOURCE_NAME_REGEX = "[@*A-Za-z]+?[*a-zA-Z_\\-0-9]*";
  // OS doesn’t allow uppercase: https://tinyurl.com/yse2xdbx
  public static final String RESULT_INDEX_NAME_PATTERN = "[a-z0-9_-]+";
  public static String INVALID_RESULT_INDEX_NAME_SIZE =
      "Result index name size must contains less than "
          + MAX_RESULT_INDEX_NAME_SIZE
          + " characters.";
  public static String INVALID_CHAR_IN_RESULT_INDEX_NAME =
      "Result index name has invalid character. Valid characters are a-z, 0-9, -(hyphen) and"
          + " _(underscore).";
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

  @JsonProperty private DataSourceStatus status;

  public static Function<String, String> DATASOURCE_TO_RESULT_INDEX =
      datasourceName -> String.format("%s_%s", DEFAULT_RESULT_INDEX, datasourceName);

  private DataSourceMetadata(Builder builder) {
    this.name = builder.name;
    this.description = builder.description;
    this.connector = builder.connector;
    this.allowedRoles = builder.allowedRoles;
    this.properties = builder.properties;
    this.resultIndex = builder.resultIndex;
    this.status = builder.status;
  }

  public static class Builder {
    private String name;
    private String description;
    private DataSourceType connector;
    private List<String> allowedRoles;
    private Map<String, String> properties;
    private String resultIndex; // Optional
    private DataSourceStatus status;

    public Builder() {}

    public Builder(DataSourceMetadata dataSourceMetadata) {
      this.name = dataSourceMetadata.getName();
      this.description = dataSourceMetadata.getDescription();
      this.connector = dataSourceMetadata.getConnector();
      this.resultIndex = dataSourceMetadata.getResultIndex();
      this.status = dataSourceMetadata.getStatus();
      this.allowedRoles = new ArrayList<>(dataSourceMetadata.getAllowedRoles());
      this.properties = new HashMap<>(dataSourceMetadata.getProperties());
    }

    public Builder setName(String name) {
      this.name = name;
      return this;
    }

    public Builder setDescription(String description) {
      this.description = description;
      return this;
    }

    public Builder setConnector(DataSourceType connector) {
      this.connector = connector;
      return this;
    }

    public Builder setAllowedRoles(List<String> allowedRoles) {
      this.allowedRoles = allowedRoles;
      return this;
    }

    public Builder setProperties(Map<String, String> properties) {
      this.properties = properties;
      return this;
    }

    public Builder setResultIndex(String resultIndex) {
      this.resultIndex = resultIndex;
      return this;
    }

    public Builder setDataSourceStatus(DataSourceStatus status) {
      this.status = status;
      return this;
    }

    public DataSourceMetadata build() {
      validateMissingAttributes();
      validateName();
      validateCustomResultIndex();
      fillNullAttributes();
      return new DataSourceMetadata(this);
    }

    private void fillNullAttributes() {
      if (resultIndex == null) {
        this.resultIndex = fromNameToCustomResultIndex();
      }
      if (status == null) {
        this.status = DataSourceStatus.ACTIVE;
      }
      if (description == null) {
        this.description = StringUtils.EMPTY;
      }
      if (properties == null) {
        this.properties = ImmutableMap.of();
      }
      if (allowedRoles == null) {
        this.allowedRoles = ImmutableList.of();
      }
    }

    private void validateMissingAttributes() {
      List<String> missingAttributes = new ArrayList<>();
      if (name == null) {
        missingAttributes.add("name");
      }
      if (connector == null) {
        missingAttributes.add("connector");
      }
      if (!missingAttributes.isEmpty()) {
        String errorMessage =
            "Datasource configuration error: "
                + String.join(", ", missingAttributes)
                + " cannot be null or empty.";
        throw new IllegalArgumentException(errorMessage);
      }
    }

    private void validateName() {
      if (!name.matches(DATASOURCE_NAME_REGEX)) {
        throw new IllegalArgumentException(
            String.format(
                "DataSource Name: %s contains illegal characters. Allowed characters:"
                    + " a-zA-Z0-9_-*@.",
                name));
      }
    }

    private void validateCustomResultIndex() {
      if (resultIndex == null) {
        return;
      }
      StringBuilder errorMessage = new StringBuilder();
      if (resultIndex.length() > MAX_RESULT_INDEX_NAME_SIZE) {
        errorMessage.append(INVALID_RESULT_INDEX_NAME_SIZE);
      }
      if (!resultIndex.matches(RESULT_INDEX_NAME_PATTERN)) {
        errorMessage.append(INVALID_CHAR_IN_RESULT_INDEX_NAME);
      }
      if (!resultIndex.startsWith(DEFAULT_RESULT_INDEX)) {
        errorMessage.append(INVALID_RESULT_INDEX_PREFIX);
      }
      if (errorMessage.length() > 0) {
        throw new IllegalArgumentException(errorMessage.toString());
      }
    }

    /**
     * Since we are using datasource name to create result index, we need to make sure that the
     * final name is valid
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

    private String fromNameToCustomResultIndex() {
      return convertToValidResultIndex(DATASOURCE_TO_RESULT_INDEX.apply(name.toLowerCase()));
    }
  }

  /**
   * Default OpenSearch {@link DataSourceMetadata}. Which is used to register default OpenSearch
   * {@link DataSource} to {@link DataSourceService}.
   */
  public static DataSourceMetadata defaultOpenSearchDataSourceMetadata() {
    return new DataSourceMetadata.Builder()
        .setName(DEFAULT_DATASOURCE_NAME)
        .setDescription(StringUtils.EMPTY)
        .setConnector(DataSourceType.OPENSEARCH)
        .setAllowedRoles(Collections.emptyList())
        .setProperties(ImmutableMap.of())
        .build();
  }
}
