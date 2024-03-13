/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.datasource.model;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.opensearch.sql.datasource.model.DataSourceStatus.ACTIVE;
import static org.opensearch.sql.datasource.model.DataSourceType.PROMETHEUS;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.jupiter.api.Test;

public class DataSourceMetadataTest {

  @Test
  public void testBuilderAndGetterMethods() {
    List<String> allowedRoles = Arrays.asList("role1", "role2");
    Map<String, String> properties = new HashMap<>();
    properties.put("key", "value");

    DataSourceMetadata metadata =
        new DataSourceMetadata.Builder()
            .setName("test")
            .setDescription("test description")
            .setConnector(DataSourceType.OPENSEARCH)
            .setAllowedRoles(allowedRoles)
            .setProperties(properties)
            .setResultIndex("query_execution_result_test123")
            .setDataSourceStatus(ACTIVE)
            .build();

    assertEquals("test", metadata.getName());
    assertEquals("test description", metadata.getDescription());
    assertEquals(DataSourceType.OPENSEARCH, metadata.getConnector());
    assertEquals(allowedRoles, metadata.getAllowedRoles());
    assertEquals(properties, metadata.getProperties());
    assertEquals("query_execution_result_test123", metadata.getResultIndex());
    assertEquals(ACTIVE, metadata.getStatus());
  }

  @Test
  public void testDefaultDataSourceMetadata() {
    DataSourceMetadata defaultMetadata = DataSourceMetadata.defaultOpenSearchDataSourceMetadata();
    assertNotNull(defaultMetadata);
    assertEquals(DataSourceType.OPENSEARCH, defaultMetadata.getConnector());
    assertTrue(defaultMetadata.getAllowedRoles().isEmpty());
    assertTrue(defaultMetadata.getProperties().isEmpty());
  }

  @Test
  public void testNameValidation() {
    try {
      new DataSourceMetadata.Builder().setName("Invalid$$$Name").setConnector(PROMETHEUS).build();
      fail("Should have thrown an IllegalArgumentException");
    } catch (IllegalArgumentException e) {
      assertEquals(
          "DataSource Name: Invalid$$$Name contains illegal characters. Allowed characters:"
              + " a-zA-Z0-9_-*@.",
          e.getMessage());
    }
  }

  @Test
  public void testResultIndexValidation() {
    try {
      new DataSourceMetadata.Builder()
          .setName("test")
          .setConnector(PROMETHEUS)
          .setResultIndex("invalid_result_index")
          .build();
      fail("Should have thrown an IllegalArgumentException");
    } catch (IllegalArgumentException e) {
      assertEquals(DataSourceMetadata.INVALID_RESULT_INDEX_PREFIX, e.getMessage());
    }
  }

  @Test
  public void testMissingAttributes() {
    try {
      new DataSourceMetadata.Builder().build();
      fail("Should have thrown an IllegalArgumentException due to missing attributes");
    } catch (IllegalArgumentException e) {
      assertTrue(e.getMessage().contains("name"));
      assertTrue(e.getMessage().contains("connector"));
    }
  }

  @Test
  public void testFillAttributes() {
    DataSourceMetadata metadata =
        new DataSourceMetadata.Builder().setName("test").setConnector(PROMETHEUS).build();

    assertEquals("test", metadata.getName());
    assertEquals(PROMETHEUS, metadata.getConnector());
    assertTrue(metadata.getDescription().isEmpty());
    assertTrue(metadata.getAllowedRoles().isEmpty());
    assertTrue(metadata.getProperties().isEmpty());
    assertEquals("query_execution_result_test", metadata.getResultIndex());
    assertEquals(ACTIVE, metadata.getStatus());
  }

  @Test
  public void testLengthyResultIndexName() {
    try {
      new DataSourceMetadata.Builder()
          .setName("test")
          .setConnector(PROMETHEUS)
          .setResultIndex("query_execution_result_" + RandomStringUtils.randomAlphanumeric(300))
          .build();
      fail("Should have thrown an IllegalArgumentException");
    } catch (IllegalArgumentException e) {
      assertEquals(
          "Result index name size must contains less than 255 characters.Result index name has"
              + " invalid character. Valid characters are a-z, 0-9, -(hyphen) and _(underscore).",
          e.getMessage());
    }
  }

  @Test
  public void testInbuiltLengthyResultIndexName() {
    DataSourceMetadata dataSourceMetadata =
        new DataSourceMetadata.Builder()
            .setName(RandomStringUtils.randomAlphabetic(250))
            .setConnector(PROMETHEUS)
            .build();
    assertEquals(255, dataSourceMetadata.getResultIndex().length());
  }

  @Test
  public void testCopyFromAnotherMetadata() {
    List<String> allowedRoles = Arrays.asList("role1", "role2");
    Map<String, String> properties = new HashMap<>();
    properties.put("key", "value");

    DataSourceMetadata metadata =
        new DataSourceMetadata.Builder()
            .setName("test")
            .setDescription("test description")
            .setConnector(DataSourceType.OPENSEARCH)
            .setAllowedRoles(allowedRoles)
            .setProperties(properties)
            .setResultIndex("query_execution_result_test123")
            .setDataSourceStatus(ACTIVE)
            .build();
    DataSourceMetadata copiedMetadata = new DataSourceMetadata.Builder(metadata).build();
    assertEquals(metadata.getResultIndex(), copiedMetadata.getResultIndex());
    assertEquals(metadata.getProperties(), copiedMetadata.getProperties());
  }
}
