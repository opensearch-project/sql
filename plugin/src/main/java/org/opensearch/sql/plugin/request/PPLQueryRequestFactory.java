/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.plugin.request;

import java.util.Map;
import java.util.Optional;
import org.json.JSONException;
import org.json.JSONObject;
import org.opensearch.rest.RestRequest;
import org.opensearch.sql.ppl.domain.PPLQueryRequest;
import org.opensearch.sql.protocol.response.format.Format;
import org.opensearch.sql.protocol.response.format.JsonResponseFormatter;

/** Factory of {@link PPLQueryRequest}. */
public class PPLQueryRequestFactory {
  public static final String PPL_URL_PARAM_KEY = "ppl";
  private static final String PPL_FIELD_NAME = "query";
  private static final String QUERY_PARAMS_FORMAT = "format";
  private static final String QUERY_PARAMS_SANITIZE = "sanitize";
  private static final String DEFAULT_RESPONSE_FORMAT = "jdbc";
  private static final String QUERY_PARAMS_PRETTY = "pretty";

  /**
   * Build {@link PPLQueryRequest} from {@link RestRequest}.
   *
   * @param request {@link PPLQueryRequest}
   * @return {@link RestRequest}
   */
  public static PPLQueryRequest getPPLRequest(RestRequest request) {
    switch (request.method()) {
      case GET:
        return parsePPLRequestFromUrl(request);
      case POST:
        return parsePPLRequestFromPayload(request);
      default:
        throw new IllegalArgumentException(
            "OpenSearch PPL doesn't supported HTTP " + request.method().name());
    }
  }

  private static PPLQueryRequest parsePPLRequestFromUrl(RestRequest restRequest) {
    String ppl;
    ppl = restRequest.param(PPL_URL_PARAM_KEY);
    if (ppl == null) {
      throw new IllegalArgumentException("Cannot find ppl parameter from the URL");
    }
    return new PPLQueryRequest(ppl, null, restRequest.path());
  }

  private static PPLQueryRequest parsePPLRequestFromPayload(RestRequest restRequest) {
    String content = restRequest.content().utf8ToString();
    JSONObject jsonContent;
    Format format = getFormat(restRequest.params());
    boolean pretty = getPrettyOption(restRequest.params());
    try {
      jsonContent = new JSONObject(content);
    } catch (JSONException e) {
      throw new IllegalArgumentException("Failed to parse request payload", e);
    }
    PPLQueryRequest pplRequest =
        new PPLQueryRequest(
            jsonContent.getString(PPL_FIELD_NAME),
            jsonContent,
            restRequest.path(),
            format.getFormatName());
    // set sanitize option if csv format
    if (format.equals(Format.CSV)) {
      pplRequest.sanitize(getSanitizeOption(restRequest.params()));
    }
    // set pretty option
    if (pretty) {
      pplRequest.style(JsonResponseFormatter.Style.PRETTY);
    }
    return pplRequest;
  }

  private static Format getFormat(Map<String, String> requestParams) {
    String formatName =
        requestParams.containsKey(QUERY_PARAMS_FORMAT)
            ? requestParams.get(QUERY_PARAMS_FORMAT).toLowerCase()
            : DEFAULT_RESPONSE_FORMAT;
    Optional<Format> optionalFormat = Format.of(formatName);
    if (optionalFormat.isPresent()) {
      return optionalFormat.get();
    } else {
      throw new IllegalArgumentException(
          "Failed to create executor due to unknown response format: " + formatName);
    }
  }

  private static boolean getSanitizeOption(Map<String, String> requestParams) {
    if (requestParams.containsKey(QUERY_PARAMS_SANITIZE)) {
      return Boolean.parseBoolean(requestParams.get(QUERY_PARAMS_SANITIZE));
    }
    return true;
  }

  private static boolean getPrettyOption(Map<String, String> requestParams) {
    if (requestParams.containsKey(QUERY_PARAMS_PRETTY)) {
      String prettyValue = requestParams.get(QUERY_PARAMS_PRETTY);
      if (prettyValue.isEmpty()) {
        return true;
      }
      return Boolean.parseBoolean(prettyValue);
    }
    return false;
  }
}
