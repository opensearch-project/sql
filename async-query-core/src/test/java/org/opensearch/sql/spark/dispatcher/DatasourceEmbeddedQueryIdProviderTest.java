/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.spark.dispatcher;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.Mockito.verifyNoInteractions;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.sql.spark.asyncquery.model.AsyncQueryRequestContext;
import org.opensearch.sql.spark.dispatcher.model.DispatchQueryRequest;

@ExtendWith(MockitoExtension.class)
class DatasourceEmbeddedQueryIdProviderTest {
  @Mock AsyncQueryRequestContext asyncQueryRequestContext;

  DatasourceEmbeddedQueryIdProvider datasourceEmbeddedQueryIdProvider =
      new DatasourceEmbeddedQueryIdProvider();

  @Test
  public void test() {
    String queryId =
        datasourceEmbeddedQueryIdProvider.getQueryId(
            DispatchQueryRequest.builder().datasource("DATASOURCE").build(),
            asyncQueryRequestContext);

    assertNotNull(queryId);
    verifyNoInteractions(asyncQueryRequestContext);
  }
}
