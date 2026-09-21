/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.datasource.client;

import org.junit.Test;

/*
 * @opensearch.experimental
 */
public class DataSourceClientTest {

  /**
   * {@link DataSourceClient#close()} is a default no-op so an implementation with no transport
   * resources to release does not have to override it. Every implementation in this module does
   * override it, so without this the default body is never executed - and an empty default method
   * still counts as an executable line against the module's 100% coverage rule.
   *
   * <p>Also covers being called twice: the factory drains its cache on shutdown after entries may
   * already have been evicted individually, so a second close must not throw. The test fails on any
   * exception, which is the assertion here.
   */
  @Test
  public void testDefaultCloseIsANoOpAndIsSafeToRepeat() {
    DataSourceClient client = new DataSourceClient() {};

    client.close();
    client.close();
  }
}
