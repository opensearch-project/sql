/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.directquery.rest.model;

import com.fasterxml.jackson.annotation.JsonInclude;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;
import java.util.Map;

/**
 *
 * @opensearch.experimental
 *
 * Response class for direct query resources.
 *
 * @param <T> The type of data contained in the response
 */
@Data
@NoArgsConstructor
@JsonInclude(JsonInclude.Include.NON_NULL)
public class WriteDirectQueryResourcesResponse<T> {
  private T data;

  private WriteDirectQueryResourcesResponse(T data) {
    this.data = data;
  }

  public static WriteDirectQueryResourcesResponse<List<String>> withStringList(List<String> data) {
    return new WriteDirectQueryResourcesResponse<>(data);
  }

  public static <V> WriteDirectQueryResourcesResponse<List<V>> withList(List<V> data) {
    return new WriteDirectQueryResourcesResponse<>(data);
  }

  public static <V> WriteDirectQueryResourcesResponse<Map<String, V>> withMap(Map<String, V> data) {
    return new WriteDirectQueryResourcesResponse<>(data);
  }
}
