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
import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;
import org.apache.commons.lang3.StringUtils;
import org.opensearch.sql.datasource.DataSourceService;

@Getter
@Setter
@AllArgsConstructor
@EqualsAndHashCode
@JsonIgnoreProperties(ignoreUnknown = true)
public class DataSourceMetadata {

  @JsonProperty private String name;

  @JsonProperty private String description;

  @JsonProperty
  @JsonFormat(with = JsonFormat.Feature.ACCEPT_CASE_INSENSITIVE_PROPERTIES)
  private DataSourceType connector;

  @JsonProperty private List<String> allowedRoles;

  @JsonProperty private Map<String, String> properties;

  @JsonProperty private String resultIndex;

  public DataSourceMetadata(
      String name,
      DataSourceType connector,
      List<String> allowedRoles,
      Map<String, String> properties,
      String resultIndex) {
    this.name = name;
    this.connector = connector;
    this.description = StringUtils.EMPTY;
    this.properties = properties;
    this.allowedRoles = allowedRoles;
    this.resultIndex = resultIndex;
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
        DataSourceType.OPENSEARCH,
        Collections.emptyList(),
        ImmutableMap.of(),
        null);
  }
}
