/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.legacy.request;

import java.util.Map;
import java.util.Optional;
import org.opensearch.sql.legacy.executor.Format;

/** Utils class for parse the request params. */
public class SqlRequestParam {
  public static final String QUERY_PARAMS_FORMAT = "format";
  public static final String QUERY_PARAMS_PRETTY = "pretty";
  public static final String QUERY_PARAMS_ESCAPE = "escape";

  private static final String DEFAULT_RESPONSE_FORMAT = "jdbc";
  private static final String DEFAULT_EXPLAIN_FORMAT = "standard";

  /**
   * Parse the pretty params to decide whether the response should be pretty formatted.
   *
   * @param requestParams request params.
   * @return return true if the response required pretty format, otherwise return false.
   */
  public static boolean isPrettyFormat(Map<String, String> requestParams) {
    return requestParams.containsKey(QUERY_PARAMS_PRETTY)
        && ("".equals(requestParams.get(QUERY_PARAMS_PRETTY))
            || "true".equals(requestParams.get(QUERY_PARAMS_PRETTY)));
  }

  public static Format getFormat(Map<String, String> requestParams) {
    return getFormat(requestParams, null);
  }

  /**
   * Parse the request params and return the {@link Format} of the response
   *
   * @param requestParams request params
   * @return The response Format.
   */
  public static Format getFormat(Map<String, String> requestParams, String path) {
    String formatName =
        requestParams.containsKey(QUERY_PARAMS_FORMAT)
            ? requestParams.get(QUERY_PARAMS_FORMAT).toLowerCase()
            : isExplainRequest(path) ? DEFAULT_EXPLAIN_FORMAT : DEFAULT_RESPONSE_FORMAT;
    Optional<Format> optionalFormat =
        isExplainRequest(path) ? Format.ofExplain(formatName) : Format.of(formatName);
    if (optionalFormat.isPresent()) {
      return optionalFormat.get();
    } else {
      throw new IllegalArgumentException(
          "Failed to create executor due to unknown response format: " + formatName);
    }
  }

  private static boolean isExplainRequest(String path) {
    return path != null && path.endsWith("/_explain");
  }

  public static boolean getEscapeOption(Map<String, String> requestParams) {
    if (requestParams.containsKey(QUERY_PARAMS_ESCAPE)) {
      return Boolean.parseBoolean(requestParams.get(QUERY_PARAMS_ESCAPE));
    }
    return false;
  }
}
