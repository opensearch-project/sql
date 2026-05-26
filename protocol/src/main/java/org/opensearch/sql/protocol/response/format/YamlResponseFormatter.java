/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.protocol.response.format;

import lombok.RequiredArgsConstructor;
import org.opensearch.sql.utils.YamlFormatter;

/**
 * Abstract class for all YAML formatter.
 *
 * @param <R> response generic type which could be DQL or DML response
 */
@RequiredArgsConstructor
public abstract class YamlResponseFormatter<R> implements ResponseFormatter<R> {

  public static final String CONTENT_TYPE = "application/yaml; charset=UTF-8";

  @Override
  public String format(R response) {
    return yamlify(buildYamlObject(response));
  }

  @Override
  public String format(Throwable t) {
    return yamlify(t);
  }

  public String contentType() {
    return CONTENT_TYPE;
  }

  /**
   * Build YAML object to generate response yaml string.
   *
   * @param response response
   * @return yaml object for response
   */
  protected abstract Object buildYamlObject(R response);

  protected String yamlify(Object yamlObject) {
    return YamlFormatter.formatToYaml(yamlObject);
  }
}
