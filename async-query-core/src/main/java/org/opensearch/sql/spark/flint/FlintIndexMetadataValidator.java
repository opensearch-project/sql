/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.spark.flint;

import static org.opensearch.sql.spark.dispatcher.model.FlintIndexOptions.AUTO_REFRESH;
import static org.opensearch.sql.spark.dispatcher.model.FlintIndexOptions.CHECKPOINT_LOCATION;
import static org.opensearch.sql.spark.dispatcher.model.FlintIndexOptions.INCREMENTAL_REFRESH;
import static org.opensearch.sql.spark.dispatcher.model.FlintIndexOptions.WATERMARK_DELAY;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class FlintIndexMetadataValidator {
  private static final Logger LOGGER = LogManager.getLogger(FlintIndexMetadataValidator.class);

  public static final Set<String> ALTER_TO_FULL_REFRESH_ALLOWED_OPTIONS =
      new LinkedHashSet<>(Arrays.asList(AUTO_REFRESH, INCREMENTAL_REFRESH));
  public static final Set<String> ALTER_TO_INCREMENTAL_REFRESH_ALLOWED_OPTIONS =
      new LinkedHashSet<>(
          Arrays.asList(AUTO_REFRESH, INCREMENTAL_REFRESH, WATERMARK_DELAY, CHECKPOINT_LOCATION));

  /**
   * Validate if the flint index options contain valid key/value pairs. Throws
   * IllegalArgumentException with description about invalid options.
   */
  public static void validateFlintIndexOptions(
      String kind, Map<String, Object> existingOptions, Map<String, String> newOptions) {
    if ((newOptions.containsKey(INCREMENTAL_REFRESH)
            && Boolean.parseBoolean(newOptions.get(INCREMENTAL_REFRESH)))
        || ((!newOptions.containsKey(INCREMENTAL_REFRESH)
            && Boolean.parseBoolean((String) existingOptions.get(INCREMENTAL_REFRESH))))) {
      validateConversionToIncrementalRefresh(kind, existingOptions, newOptions);
    } else {
      validateConversionToFullRefresh(newOptions);
    }
  }

  private static void validateConversionToFullRefresh(Map<String, String> newOptions) {
    if (!ALTER_TO_FULL_REFRESH_ALLOWED_OPTIONS.containsAll(newOptions.keySet())) {
      throw new IllegalArgumentException(
          String.format(
              "Altering to full refresh only allows: %s options",
              ALTER_TO_FULL_REFRESH_ALLOWED_OPTIONS));
    }
  }

  private static void validateConversionToIncrementalRefresh(
      String kind, Map<String, Object> existingOptions, Map<String, String> newOptions) {
    if (!ALTER_TO_INCREMENTAL_REFRESH_ALLOWED_OPTIONS.containsAll(newOptions.keySet())) {
      throw new IllegalArgumentException(
          String.format(
              "Altering to incremental refresh only allows: %s options",
              ALTER_TO_INCREMENTAL_REFRESH_ALLOWED_OPTIONS));
    }
    HashMap<String, Object> mergedOptions = new HashMap<>();
    mergedOptions.putAll(existingOptions);
    mergedOptions.putAll(newOptions);
    List<String> missingAttributes = new ArrayList<>();
    if (!mergedOptions.containsKey(CHECKPOINT_LOCATION)
        || StringUtils.isEmpty((String) mergedOptions.get(CHECKPOINT_LOCATION))) {
      missingAttributes.add(CHECKPOINT_LOCATION);
    }
    if (kind.equals("mv")
        && (!mergedOptions.containsKey(WATERMARK_DELAY)
            || StringUtils.isEmpty((String) mergedOptions.get(WATERMARK_DELAY)))) {
      missingAttributes.add(WATERMARK_DELAY);
    }
    if (missingAttributes.size() > 0) {
      String errorMessage =
          "Conversion to incremental refresh index cannot proceed due to missing attributes: "
              + String.join(", ", missingAttributes)
              + ".";
      LOGGER.error(errorMessage);
      throw new IllegalArgumentException(errorMessage);
    }
  }
}
