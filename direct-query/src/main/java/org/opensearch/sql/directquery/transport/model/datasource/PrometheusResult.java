/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.directquery.transport.model.datasource;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import java.util.List;
import java.util.Map;
import lombok.Getter;
import lombok.Setter;

/** Represents a Prometheus query result. */
@Getter
@Setter
@JsonTypeName("prometheus")
@JsonIgnoreProperties(ignoreUnknown = true)
public class PrometheusResult implements DataSourceResult {

  @JsonProperty("resultType")
  private String resultType;

  @JsonProperty("result")
  private List<PrometheusResultItem> result;

  // Required public no-args constructor for OpenSearch serialization
  public PrometheusResult() {}

  @Getter
  @Setter
  @JsonIgnoreProperties(ignoreUnknown = true)
  public static class PrometheusResultItem {
    @JsonProperty("metric")
    private Map<String, String> metric;

    @JsonProperty("values")
    private List<List<Object>> values;

    @JsonProperty("value")
    private List<Object> value;

    // Required public no-args constructor for OpenSearch serialization
    public PrometheusResultItem() {}
  }
}
