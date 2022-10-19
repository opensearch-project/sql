/*
 *
 *  * Copyright OpenSearch Contributors
 *  * SPDX-License-Identifier: Apache-2.0
 *
 */

package org.opensearch.sql.catalog.model.auth;

import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import lombok.Getter;
import lombok.Setter;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonTypeInfo(
    use = JsonTypeInfo.Id.NAME,
    include = JsonTypeInfo.As.EXISTING_PROPERTY,
    property = "type",
    defaultImpl = AbstractAuthenticationData.class,
    visible = true)
@JsonSubTypes({
    @JsonSubTypes.Type(value = BasicAuthenticationData.class, name = "basicauth"),
    @JsonSubTypes.Type(value = AwsSigV4AuthenticationData.class, name = "sigv4auth"),
})
@Getter
@Setter
public abstract class AbstractAuthenticationData {

  @JsonFormat(with = JsonFormat.Feature.ACCEPT_CASE_INSENSITIVE_PROPERTIES)
  private AuthenticationType type;

}
