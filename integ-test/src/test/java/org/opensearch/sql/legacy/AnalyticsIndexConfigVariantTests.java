/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.legacy;

import static org.junit.Assert.assertEquals;

import org.junit.After;
import org.junit.Test;
import org.opensearch.sql.legacy.TestUtils.AnalyticsIndexConfig;

/**
 * Unit coverage for {@link AnalyticsIndexConfig#resolveDatasetPath(String)} — the analytics-engine
 * {@code _ae} dataset-variant swap. Guards the two behaviors the feature depends on: zero change
 * when the flag is off, and {@code _ae} pickup only when a sibling actually exists on disk.
 */
public class AnalyticsIndexConfigVariantTests {

  @After
  public void clearFlag() {
    System.clearProperty(AnalyticsIndexConfig.ENABLED_PROP);
  }

  @Test
  public void flagOff_returnsOriginalUnchanged() {
    System.clearProperty(AnalyticsIndexConfig.ENABLED_PROP);
    String mapping = "src/test/resources/indexDefinitions/datatypes_index_mapping.json";
    String data = "src/test/resources/datatypes.json";
    assertEquals(mapping, AnalyticsIndexConfig.resolveDatasetPath(mapping));
    assertEquals(data, AnalyticsIndexConfig.resolveDatasetPath(data));
  }

  @Test
  public void flagOn_resolvesAeVariantWhenSiblingExists() {
    System.setProperty(AnalyticsIndexConfig.ENABLED_PROP, "true");
    // datasets that ship an _ae sibling
    assertEquals(
        "src/test/resources/indexDefinitions/datatypes_index_mapping_ae.json",
        AnalyticsIndexConfig.resolveDatasetPath(
            "src/test/resources/indexDefinitions/datatypes_index_mapping.json"));
    assertEquals(
        "src/test/resources/datatypes_ae.json",
        AnalyticsIndexConfig.resolveDatasetPath("src/test/resources/datatypes.json"));
    assertEquals(
        "src/test/resources/nested_simple_ae.json",
        AnalyticsIndexConfig.resolveDatasetPath("src/test/resources/nested_simple.json"));
    assertEquals(
        "src/test/resources/deep_nested_index_data_ae.json",
        AnalyticsIndexConfig.resolveDatasetPath("src/test/resources/deep_nested_index_data.json"));
    assertEquals(
        "src/test/resources/merge_test_1_ae.json",
        AnalyticsIndexConfig.resolveDatasetPath("src/test/resources/merge_test_1.json"));
    assertEquals(
        "src/test/resources/merge_test_2_ae.json",
        AnalyticsIndexConfig.resolveDatasetPath("src/test/resources/merge_test_2.json"));
  }

  @Test
  public void flagOn_fallsBackWhenNoSibling() {
    System.setProperty(AnalyticsIndexConfig.ENABLED_PROP, "true");
    // no _ae sibling on disk -> original returned
    String noVariant = "src/test/resources/accounts.json";
    assertEquals(noVariant, AnalyticsIndexConfig.resolveDatasetPath(noVariant));
    String cascaded = "src/test/resources/indexDefinitions/cascaded_nested_index_mapping.json";
    assertEquals(cascaded, AnalyticsIndexConfig.resolveDatasetPath(cascaded));
  }

  @Test
  public void flagOn_pathWithoutExtensionReturnedAsIs() {
    System.setProperty(AnalyticsIndexConfig.ENABLED_PROP, "true");
    assertEquals("src/test/resources/noext", AnalyticsIndexConfig.resolveDatasetPath("noext"));
  }

  @Test
  public void nullPathReturnsNull() {
    System.setProperty(AnalyticsIndexConfig.ENABLED_PROP, "true");
    assertEquals(null, AnalyticsIndexConfig.resolveDatasetPath(null));
  }
}
