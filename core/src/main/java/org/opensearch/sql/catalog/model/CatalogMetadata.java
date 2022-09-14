/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.catalog.model;

import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import lombok.Getter;
import lombok.Setter;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonTypeInfo(
    use = JsonTypeInfo.Id.NAME,
    include = JsonTypeInfo.As.EXISTING_PROPERTY,
    property = "connector",
    defaultImpl = AbstractAuthenticationData.class,
    visible = true)
@JsonSubTypes({
    @JsonSubTypes.Type(value = PrometheusCatalogMetadata.class, name = "prometheus")
})
@Getter
@Setter
public abstract class CatalogMetadata {

  @JsonProperty(required = true)
  private String name;

  @JsonProperty(required = true)
  private String uri;

  @JsonProperty(required = true)
  @JsonFormat(with = JsonFormat.Feature.ACCEPT_CASE_INSENSITIVE_PROPERTIES)
  private ConnectorType connector;

  private AbstractAuthenticationData authentication;

}
