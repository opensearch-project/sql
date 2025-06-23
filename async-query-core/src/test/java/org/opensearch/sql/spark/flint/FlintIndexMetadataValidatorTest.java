/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.spark.flint;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.opensearch.sql.spark.dispatcher.model.FlintIndexOptions.AUTO_REFRESH;
import static org.opensearch.sql.spark.dispatcher.model.FlintIndexOptions.CHECKPOINT_LOCATION;
import static org.opensearch.sql.spark.dispatcher.model.FlintIndexOptions.INCREMENTAL_REFRESH;
import static org.opensearch.sql.spark.dispatcher.model.FlintIndexOptions.WATERMARK_DELAY;

import com.google.common.collect.ImmutableMap;
import java.util.Map;
import org.junit.jupiter.api.Test;

class FlintIndexMetadataValidatorTest {
  @Test
  public void conversionToIncrementalRefreshWithValidOption() {
    Map<String, Object> existingOptions =
        ImmutableMap.<String, Object>builder().put(INCREMENTAL_REFRESH, "false").build();
    Map<String, String> newOptions =
        ImmutableMap.<String, String>builder()
            .put(INCREMENTAL_REFRESH, "true")
            .put(CHECKPOINT_LOCATION, "checkpoint_location")
            .put(WATERMARK_DELAY, "1")
            .build();

    FlintIndexMetadataValidator.validateFlintIndexOptions("mv", existingOptions, newOptions);
  }

  @Test
  public void conversionToIncrementalRefreshWithMissingOptions() {
    Map<String, Object> existingOptions =
        ImmutableMap.<String, Object>builder().put(AUTO_REFRESH, "true").build();
    Map<String, String> newOptions =
        ImmutableMap.<String, String>builder().put(INCREMENTAL_REFRESH, "true").build();

    assertThrows(
        IllegalArgumentException.class,
        () ->
            FlintIndexMetadataValidator.validateFlintIndexOptions(
                "mv", existingOptions, newOptions));
  }

  @Test
  public void conversionToIncrementalRefreshWithInvalidOption() {
    Map<String, Object> existingOptions =
        ImmutableMap.<String, Object>builder().put(INCREMENTAL_REFRESH, "false").build();
    Map<String, String> newOptions =
        ImmutableMap.<String, String>builder()
            .put(INCREMENTAL_REFRESH, "true")
            .put("INVALID_OPTION", "1")
            .build();

    assertThrows(
        IllegalArgumentException.class,
        () ->
            FlintIndexMetadataValidator.validateFlintIndexOptions(
                "mv", existingOptions, newOptions));
  }

  @Test
  public void conversionToFullRefreshWithValidOption() {
    Map<String, Object> existingOptions =
        ImmutableMap.<String, Object>builder().put(AUTO_REFRESH, "false").build();
    Map<String, String> newOptions =
        ImmutableMap.<String, String>builder().put(AUTO_REFRESH, "true").build();

    FlintIndexMetadataValidator.validateFlintIndexOptions("mv", existingOptions, newOptions);
  }

  @Test
  public void conversionToFullRefreshWithInvalidOption() {
    Map<String, Object> existingOptions =
        ImmutableMap.<String, Object>builder().put(AUTO_REFRESH, "false").build();
    Map<String, String> newOptions =
        ImmutableMap.<String, String>builder()
            .put(AUTO_REFRESH, "true")
            .put(WATERMARK_DELAY, "1")
            .build();

    assertThrows(
        IllegalArgumentException.class,
        () ->
            FlintIndexMetadataValidator.validateFlintIndexOptions(
                "mv", existingOptions, newOptions));
  }
}
