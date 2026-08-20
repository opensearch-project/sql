/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.legacy;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

public class OpenSearchSQLRestTestCaseIT {

  @Test
  public void identifiesPluginManagedIndices() {
    assertTrue(OpenSearchSQLRestTestCase.isPluginManagedIndex(".opensearch-observability"));
    assertTrue(OpenSearchSQLRestTestCase.isPluginManagedIndex(".opendistro_security"));
    assertTrue(OpenSearchSQLRestTestCase.isPluginManagedIndex(".ql-datasources"));
    assertTrue(OpenSearchSQLRestTestCase.isPluginManagedIndex(".plugins-ml-config"));
    assertTrue(
        OpenSearchSQLRestTestCase.isPluginManagedIndex(".scheduler-geospatial-ip2geo-datasource"));
    assertTrue(
        OpenSearchSQLRestTestCase.isPluginManagedIndex(".geospatial-ip2geo-data.datasource"));
    assertTrue(OpenSearchSQLRestTestCase.isPluginManagedIndex("security-auditlog-2026.07.15"));
  }

  @Test
  public void leavesTestIndicesEligibleForCleanup() {
    assertFalse(OpenSearchSQLRestTestCase.isPluginManagedIndex("opensearch-sql_test_index_bank"));
    assertFalse(OpenSearchSQLRestTestCase.isPluginManagedIndex("security-test-index"));
    assertFalse(OpenSearchSQLRestTestCase.isPluginManagedIndex(".test-hidden-index"));
  }
}
