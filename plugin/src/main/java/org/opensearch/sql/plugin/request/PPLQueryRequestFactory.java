/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.plugin.request;

import java.util.Map;
import java.util.Optional;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json.JSONException;
import org.json.JSONObject;
import org.opensearch.rest.RestRequest;
import org.opensearch.sql.ppl.domain.PPLQueryRequest;
import org.opensearch.sql.protocol.response.format.Format;
import org.opensearch.sql.protocol.response.format.JsonResponseFormatter;

/** Factory of {@link PPLQueryRequest}. */
public class PPLQueryRequestFactory {
  private static final Logger LOG = LogManager.getLogger(PPLQueryRequestFactory.class);

  private static final String PPL_URL_PARAM_KEY = "ppl";
  private static final String PPL_FIELD_NAME = "query";
  private static final String QUERY_PARAMS_FORMAT = "format";
  private static final String QUERY_PARAMS_EXPLAIN_MODE = "mode";
  private static final String QUERY_PARAMS_SANITIZE = "sanitize";
  private static final String DEFAULT_RESPONSE_FORMAT = "jdbc";
  private static final String DEFAULT_EXPLAIN_FORMAT = "json";
  private static final String DEFAULT_EXPLAIN_MODE = "standard";
  private static final String QUERY_PARAMS_PRETTY = "pretty";
  private static final String QUERY_PARAMS_PROFILE = "profile";

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
    Format format = getFormat(restRequest.params(), restRequest.rawPath());
    String explainMode;
    // For backward compatible consideration, if the format=[simple, standard, extended, cost], we
    // accept it as well and view it as mode and use json format.
    // TODO: deprecated after 4.x
    if (Format.isExplainMode(format)) {
      // Log deprecation warning for legacy format-based explain mode usage
      LOG.warn(
          "Using 'format' parameter for explain mode is deprecated. Please use 'mode' parameter"
              + " instead. This will be removed in 4.x.");
      explainMode = format.getFormatName();
      format = Format.JSON;
    } else {
      explainMode = getExplainMode(restRequest.params(), restRequest.rawPath());
    }
    boolean pretty = getPrettyOption(restRequest.params());
    try {
      jsonContent = new JSONObject(content);
      boolean profileRequested = jsonContent.optBoolean(QUERY_PARAMS_PROFILE, false);
      String queryString = jsonContent.optString(PPL_FIELD_NAME, "");
      boolean enableProfile =
          profileRequested && isProfileSupported(restRequest.path(), format, queryString);
      PPLQueryRequest pplRequest =
          new PPLQueryRequest(
              jsonContent.getString(PPL_FIELD_NAME),
              jsonContent,
              restRequest.path(),
              format.getFormatName(),
              explainMode,
              enableProfile);
      // set sanitize option if csv format
      if (format.equals(Format.CSV)) {
        pplRequest.sanitize(getSanitizeOption(restRequest.params()));
      }
      // set pretty option
      if (pretty) {
        pplRequest.style(JsonResponseFormatter.Style.PRETTY);
      }
      return pplRequest;
    } catch (JSONException e) {
      throw new IllegalArgumentException("Failed to parse request payload", e);
    }
  }

  private static Format getFormat(Map<String, String> requestParams, String path) {
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

  private static boolean isProfileSupported(String path, Format format, String query) {
    boolean explainPath = isExplainRequest(path);
    boolean explainQuery = query != null && query.trim().toLowerCase().startsWith("explain");
    boolean isJdbcFormat =
        format != null && DEFAULT_RESPONSE_FORMAT.equalsIgnoreCase(format.getFormatName());
    return !explainPath && !explainQuery && isJdbcFormat;
  }

  private static String getExplainMode(Map<String, String> requestParams, String path) {
    if (!isExplainRequest(path)) {
      return null;
    }
    return requestParams
        .getOrDefault(QUERY_PARAMS_EXPLAIN_MODE, DEFAULT_EXPLAIN_MODE)
        .toLowerCase();
  }
}
