/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.protocol.response.format;

import com.google.gson.Gson;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.experimental.UtilityClass;
import org.opensearch.sql.utils.SerializeUtils;

@UtilityClass
public class ErrorFormatter {

  private static final Gson PRETTY_PRINT_GSON =
      SerializeUtils.getGsonBuilder().setPrettyPrinting().disableHtmlEscaping().create();
  private static final Gson GSON = SerializeUtils.getGsonBuilder().disableHtmlEscaping().create();

  /** Util method to format {@link Throwable} response to JSON string in compact printing. */
  public static String compactFormat(Throwable t) {
    JsonError error = new ErrorFormatter.JsonError(t.getClass().getSimpleName(), t.getMessage());
    return compactJsonify(error);
  }

  /** Util method to format {@link Throwable} response to JSON string in pretty printing. */
  public static String prettyFormat(Throwable t) {
    JsonError error = new ErrorFormatter.JsonError(t.getClass().getSimpleName(), t.getMessage());
    return prettyJsonify(error);
  }

  public static String compactJsonify(Object jsonObject) {
    return GSON.toJson(jsonObject);
  }

  public static String prettyJsonify(Object jsonObject) {
    return PRETTY_PRINT_GSON.toJson(jsonObject);
  }

  @RequiredArgsConstructor
  @Getter
  public static class JsonError {
    private final String type;
    private final String reason;
  }
}
