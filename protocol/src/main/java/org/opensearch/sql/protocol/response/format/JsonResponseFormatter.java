/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

/*
 *    Copyright 2020 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 *    Licensed under the Apache License, Version 2.0 (the "License").
 *    You may not use this file except in compliance with the License.
 *    A copy of the License is located at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *    or in the "license" file accompanying this file. This file is distributed
 *    on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 *    express or implied. See the License for the specific language governing
 *    permissions and limitations under the License.
 *
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

  @Override
  public String format(R response) {
    return jsonify(buildJsonObject(response));
  }

  @Override
  public String format(Throwable t) {
    return AccessController.doPrivileged((PrivilegedAction<String>) () ->
        (style == PRETTY) ? prettyFormat(t) : compactFormat(t));
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
