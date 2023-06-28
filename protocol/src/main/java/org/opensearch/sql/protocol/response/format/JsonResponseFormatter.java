/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.protocol.response.format;

import static org.opensearch.sql.protocol.response.format.ErrorFormatter.compactFormat;
import static org.opensearch.sql.protocol.response.format.ErrorFormatter.compactJsonify;
import static org.opensearch.sql.protocol.response.format.ErrorFormatter.prettyFormat;
import static org.opensearch.sql.protocol.response.format.ErrorFormatter.prettyJsonify;
import static org.opensearch.sql.protocol.response.format.JsonResponseFormatter.Style.PRETTY;

import java.security.AccessController;
import java.security.PrivilegedAction;
import lombok.RequiredArgsConstructor;

/**
 * Abstract class for all JSON formatter.
 *
 * @param <R> response generic type which could be DQL or DML response
 */
@RequiredArgsConstructor
public abstract class JsonResponseFormatter<R> implements ResponseFormatter<R> {

  /**
   * JSON format styles: pretty format or compact format without indent and space.
   */
  public enum Style {
    PRETTY, COMPACT
  }

  /**
   * JSON format style.
   */
  private final Style style;

  public static final String CONTENT_TYPE = "application/json; charset=UTF-8";

  @Override
  public String format(R response) {
    return jsonify(buildJsonObject(response));
  }

  @Override
  public String format(Throwable t) {
    return AccessController.doPrivileged((PrivilegedAction<String>) () ->
        (style == PRETTY) ? prettyFormat(t) : compactFormat(t));
  }

  public String contentType() {
    return CONTENT_TYPE;
  }

  /**
   * Build JSON object to generate response json string.
   *
   * @param response response
   * @return json object for response
   */
  protected abstract Object buildJsonObject(R response);

  protected String jsonify(Object jsonObject) {
    return AccessController.doPrivileged((PrivilegedAction<String>) () ->
        (style == PRETTY) ? prettyJsonify(jsonObject) : compactJsonify(jsonObject));
  }
}
