/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.spark.execution.session;

import org.junit.After;
import org.junit.Before;
import org.opensearch.action.admin.indices.delete.DeleteIndexRequest;
import org.opensearch.sql.spark.execution.statestore.StateStore;
import org.opensearch.test.OpenSearchSingleNodeTestCase;

class SessionManagerTest extends OpenSearchSingleNodeTestCase {
  private static final String indexName = "mockindex";

  private StateStore stateStore;

  @Before
  public void setup() {
    stateStore = new StateStore(indexName, client());
    createIndex(indexName);
  }

  @After
  public void clean() {
    client().admin().indices().delete(new DeleteIndexRequest(indexName)).actionGet();
  }
}
