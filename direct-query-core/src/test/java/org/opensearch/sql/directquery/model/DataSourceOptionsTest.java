/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.directquery.model;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

/*
 * @opensearch.experimental
 */
public class DataSourceOptionsTest {

  @Test
  public void testDataSourceOptionsInterface() {
    DataSourceOptions options = new TestDataSourceOptions();
    assertNotNull(options);
    assertTrue(options instanceof DataSourceOptions);
  }

  private static class TestDataSourceOptions implements DataSourceOptions {
    // Empty implementation for testing
  }
}