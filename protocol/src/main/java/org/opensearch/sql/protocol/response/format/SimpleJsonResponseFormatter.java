/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.protocol.response.format;

import java.util.List;
import lombok.Builder;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.Singular;
import org.opensearch.sql.protocol.response.QueryResult;

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
public class SimpleJsonResponseFormatter extends JsonResponseFormatter<QueryResult> {

  public SimpleJsonResponseFormatter(Style style) {
    super(style);
  }

  @Override
  public Object buildJsonObject(QueryResult response) {
    JsonResponse.JsonResponseBuilder json = JsonResponse.builder();

    json.total(response.size()).size(response.size());

    response.columnNameTypes().forEach((name, type) -> json.column(new Column(name, type)));

    json.datarows(fetchDataRows(response));
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
  public static class JsonResponse {
    @Singular("column")
    private final List<Column> schema;

    private final Object[][] datarows;

    private long total;
    private long size;
  }

  @RequiredArgsConstructor
  @Getter
  public static class Column {
    private final String name;
    private final String type;
  }
}
