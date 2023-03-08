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
import java.util.Map;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;
import org.opensearch.sql.datasource.DataSourceService;

@JsonIgnoreProperties(ignoreUnknown = true)
@Getter
@Setter
@EqualsAndHashCode
public class DataSourceMetadata {

  @JsonProperty(required = true)
  private String name;

  @JsonProperty(required = true)
  @JsonFormat(with = JsonFormat.Feature.ACCEPT_CASE_INSENSITIVE_PROPERTIES)
  private DataSourceType connector;

  @JsonProperty(required = true)
  private Map<String, String> properties;

  /**
   * Default OpenSearch {@link DataSourceMetadata}. Which is used to register default OpenSearch
   * {@link DataSource} to {@link DataSourceService}.
   */
  public static DataSourceMetadata defaultOpenSearchDataSourceMetadata() {
    DataSourceMetadata dataSourceMetadata = new DataSourceMetadata();
    dataSourceMetadata.setName(DEFAULT_DATASOURCE_NAME);
    dataSourceMetadata.setConnector(DataSourceType.OPENSEARCH);
    dataSourceMetadata.setProperties(ImmutableMap.of());
    return dataSourceMetadata;
  }

  public String toString() {
    return properties.toString() + " " + name + " " + connector;
  }
}
