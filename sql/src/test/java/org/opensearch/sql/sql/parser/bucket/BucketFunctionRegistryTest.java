/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.sql.parser.bucket;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Optional;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator;
import org.junit.jupiter.api.Test;

@DisplayNameGeneration(DisplayNameGenerator.ReplaceUnderscores.class)
class BucketFunctionRegistryTest {

  @Test
  void lookup_returns_HistogramExpander_for_HISTOGRAM() {
    Optional<BucketFunctionExpander> expander = BucketFunctionRegistry.lookup("HISTOGRAM");
    assertTrue(expander.isPresent());
    assertInstanceOf(HistogramExpander.class, expander.get());
  }

  @Test
  void lookup_returns_DateHistogramExpander_for_DATE_HISTOGRAM() {
    Optional<BucketFunctionExpander> expander = BucketFunctionRegistry.lookup("DATE_HISTOGRAM");
    assertTrue(expander.isPresent());
    assertInstanceOf(DateHistogramExpander.class, expander.get());
  }

  @Test
  void lookup_is_case_insensitive() {
    assertTrue(BucketFunctionRegistry.lookup("histogram").isPresent());
    assertTrue(BucketFunctionRegistry.lookup("Histogram").isPresent());
    assertTrue(BucketFunctionRegistry.lookup("date_histogram").isPresent());
    assertTrue(BucketFunctionRegistry.lookup("Date_Histogram").isPresent());
  }

  @Test
  void lookup_returns_empty_for_unknown_function() {
    assertFalse(BucketFunctionRegistry.lookup("range").isPresent());
    assertFalse(BucketFunctionRegistry.lookup("SUM").isPresent());
    assertFalse(BucketFunctionRegistry.lookup("FLOOR").isPresent());
  }

  @Test
  void lookup_returns_empty_for_null() {
    assertFalse(BucketFunctionRegistry.lookup(null).isPresent());
  }
}
