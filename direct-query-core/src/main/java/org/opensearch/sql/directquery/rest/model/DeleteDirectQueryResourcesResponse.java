/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.directquery.rest.model;

import com.fasterxml.jackson.annotation.JsonInclude;
import java.util.Map;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @opensearch.experimental
 *     <p>Response class for delete direct query resources.
 * @param <T> The type of data contained in the response
 */
@Data
@NoArgsConstructor
@JsonInclude(JsonInclude.Include.NON_NULL)
public class DeleteDirectQueryResourcesResponse<T> {
  private T data;

  private DeleteDirectQueryResourcesResponse(T data) {
    this.data = data;
  }

  public static DeleteDirectQueryResourcesResponse<String> withMessage(String message) {
    return new DeleteDirectQueryResourcesResponse<>(message);
  }

  public static <V> DeleteDirectQueryResourcesResponse<Map<String, V>> withMap(
      Map<String, V> data) {
    return new DeleteDirectQueryResourcesResponse<>(data);
  }
}
