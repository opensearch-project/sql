/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.spark.execution.session;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.After;
import org.junit.Before;
import org.mockito.MockMakers;
import org.mockito.MockSettings;
import org.mockito.Mockito;
import org.opensearch.action.admin.indices.delete.DeleteIndexRequest;
import org.opensearch.sql.spark.execution.statestore.SessionStateStore;
import org.opensearch.test.OpenSearchSingleNodeTestCase;

class SessionManagerTest extends OpenSearchSingleNodeTestCase {
  private static final String indexName = "mockindex";

  // mock-maker-inline does not work with OpenSearchTestCase. make sure use mockSettings when mock.
  private static final MockSettings mockSettings =
      Mockito.withSettings().mockMaker(MockMakers.SUBCLASS);

  private SessionStateStore stateStore;

  @Before
  public void setup() {
    stateStore = new SessionStateStore(indexName, client());
    createIndex(indexName);
  }

  @After
  public void clean() {
    client().admin().indices().delete(new DeleteIndexRequest(indexName)).actionGet();
  }
}
