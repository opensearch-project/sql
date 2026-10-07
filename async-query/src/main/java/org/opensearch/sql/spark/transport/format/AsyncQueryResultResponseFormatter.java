/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.spark.transport.format;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import java.util.List;
import java.util.stream.Collectors;
import lombok.Builder;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import org.opensearch.core.common.Strings;
import org.opensearch.sql.protocol.response.QueryResult;
import org.opensearch.sql.protocol.response.format.JsonResponseFormatter;
import org.opensearch.sql.spark.transport.model.AsyncQueryResult;

/**
 * JSON response format with schema header and data rows. For example,
 *
 * <pre>
 *  {
 *      "schema": [
 *          {
 *              "name": "name",
 *              "type": "string"
 *          }
 *      ],
 *      "datarows": [
 *          ["John"],
 *          ["Smith"]
 *      ],
 *      "total": 2,
 *      "size": 2
 *  }
 * </pre>
 */
public class AsyncQueryResultResponseFormatter extends JsonResponseFormatter<AsyncQueryResult> {

  public AsyncQueryResultResponseFormatter(Style style) {
    super(style);
  }

  @Override
  public Object buildJsonObject(AsyncQueryResult response) {
    JsonResponse.JsonResponseBuilder json = JsonResponse.builder();
    String status = response.getStatus();
    // "SUCCESS" is the legacy Spark job-run state; "SUCCEEDED" is the neutral QueryJobState name
    // emitted by the id-shape routing path (issue #5765).
    if ("success".equalsIgnoreCase(status) || "succeeded".equalsIgnoreCase(status)) {
      json.total(response.size()).size(response.size());
      json.schema(
          response.columnNameTypes().entrySet().stream()
              .map((entry) -> new Column(entry.getKey(), entry.getValue()))
              .collect(Collectors.toList()));
      json.datarows(fetchDataRows(response));
    }
    json.status(response.getStatus());
    // Prefer the structured error map (PPL FAILED path) over the error string (Spark path).
    // Spark results never populate errorDetails, so the string branch here is byte-identical
    // to the pre-change behavior; the Gson field type (`Object`) serializes String as a JSON
    // string and Map as a JSON object.
    if (response.getErrorDetails() != null && !response.getErrorDetails().isEmpty()) {
      json.error(response.getErrorDetails());
    } else if (!Strings.isEmpty(response.getError())) {
      json.error(response.getError());
    }

    return json.build();
  }

  private Object[][] fetchDataRows(QueryResult response) {
    Object[][] rows = new Object[response.size()][];
    int i = 0;
    for (Object[] values : response) {
      rows[i++] = values;
    }
    return rows;
  }

  /** org.json requires these inner data classes be public (and static) */
  @Builder
  @Getter
  @JsonIgnoreProperties(ignoreUnknown = true)
  public static class JsonResponse {

    private final String status;

    private final List<Column> schema;

    private final Object[][] datarows;

    private Integer total;
    private Integer size;

    /**
     * Either a human-readable string (Spark path) or a structured map matching the sync REST
     * error body (PPL {@code FAILED} path). Typed as {@link Object} so Gson emits either shape
     * verbatim without a wrapping envelope.
     */
    private final Object error;
  }

  @RequiredArgsConstructor
  @Getter
  public static class Column {
    private final String name;
    private final String type;
  }
}
