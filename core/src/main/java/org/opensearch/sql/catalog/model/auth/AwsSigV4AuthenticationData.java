/*
 *
 *  * Copyright OpenSearch Contributors
 *  * SPDX-License-Identifier: Apache-2.0
 *
 */

package org.opensearch.sql.catalog.model.auth;


import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;
import org.opensearch.sql.catalog.model.auth.AbstractAuthenticationData;

@Getter
@Setter
@JsonIgnoreProperties(ignoreUnknown = true)
public class AwsSigV4AuthenticationData extends AbstractAuthenticationData {

  @JsonProperty(required = true)
  private String secretKey;

  @JsonProperty(required = true)
  private String accessKey;

  @JsonProperty(required = true)
  private String region;

}
