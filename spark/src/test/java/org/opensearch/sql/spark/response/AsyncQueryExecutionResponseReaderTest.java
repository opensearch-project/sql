/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.spark.response;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;
import static org.opensearch.sql.spark.constants.TestConstants.EMR_JOB_ID;
import static org.opensearch.sql.spark.data.constants.SparkConstants.SPARK_RESPONSE_BUFFER_INDEX_NAME;

import java.util.Map;
import org.apache.lucene.search.TotalHits;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.client.Client;
import org.opensearch.common.action.ActionFuture;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.index.IndexNotFoundException;
import org.opensearch.search.SearchHit;
import org.opensearch.search.SearchHits;

@ExtendWith(MockitoExtension.class)
public class AsyncQueryExecutionResponseReaderTest {
  @Mock private Client client;
  @Mock private SearchResponse searchResponse;
  @Mock private SearchHit searchHit;
  @Mock private ActionFuture<SearchResponse> searchResponseActionFuture;

  @Test
  public void testGetResultFromOpensearchIndex() {
    when(client.search(any())).thenReturn(searchResponseActionFuture);
    when(searchResponseActionFuture.actionGet()).thenReturn(searchResponse);
    when(searchResponse.status()).thenReturn(RestStatus.OK);
    when(searchResponse.getHits())
        .thenReturn(
            new SearchHits(
                new SearchHit[] {searchHit}, new TotalHits(1, TotalHits.Relation.EQUAL_TO), 1.0F));
    Mockito.when(searchHit.getSourceAsMap()).thenReturn(Map.of("stepId", EMR_JOB_ID));
    JobExecutionResponseReader jobExecutionResponseReader = new JobExecutionResponseReader(client);
    assertFalse(
        jobExecutionResponseReader.getResultFromOpensearchIndex(EMR_JOB_ID, null).isEmpty());
  }

  @Test
  public void testGetResultFromCustomIndex() {
    when(client.search(any())).thenReturn(searchResponseActionFuture);
    when(searchResponseActionFuture.actionGet()).thenReturn(searchResponse);
    when(searchResponse.status()).thenReturn(RestStatus.OK);
    when(searchResponse.getHits())
        .thenReturn(
            new SearchHits(
                new SearchHit[] {searchHit}, new TotalHits(1, TotalHits.Relation.EQUAL_TO), 1.0F));
    Mockito.when(searchHit.getSourceAsMap()).thenReturn(Map.of("stepId", EMR_JOB_ID));
    JobExecutionResponseReader jobExecutionResponseReader = new JobExecutionResponseReader(client);
    assertFalse(
        jobExecutionResponseReader.getResultFromOpensearchIndex(EMR_JOB_ID, "foo").isEmpty());
  }

  @Test
  public void testInvalidSearchResponse() {
    when(client.search(any())).thenReturn(searchResponseActionFuture);
    when(searchResponseActionFuture.actionGet()).thenReturn(searchResponse);
    when(searchResponse.status()).thenReturn(RestStatus.NO_CONTENT);

    JobExecutionResponseReader jobExecutionResponseReader = new JobExecutionResponseReader(client);
    RuntimeException exception =
        assertThrows(
            RuntimeException.class,
            () -> jobExecutionResponseReader.getResultFromOpensearchIndex(EMR_JOB_ID, null));
    Assertions.assertEquals(
        "Fetching result from "
            + SPARK_RESPONSE_BUFFER_INDEX_NAME
            + " index failed with status : "
            + RestStatus.NO_CONTENT,
        exception.getMessage());
  }

  @Test
  public void testSearchFailure() {
    when(client.search(any())).thenThrow(RuntimeException.class);
    JobExecutionResponseReader jobExecutionResponseReader = new JobExecutionResponseReader(client);
    assertThrows(
        RuntimeException.class,
        () -> jobExecutionResponseReader.getResultFromOpensearchIndex(EMR_JOB_ID, null));
  }

  @Test
  public void testIndexNotFoundException() {
    when(client.search(any())).thenThrow(IndexNotFoundException.class);
    JobExecutionResponseReader jobExecutionResponseReader = new JobExecutionResponseReader(client);
    assertTrue(
        jobExecutionResponseReader.getResultFromOpensearchIndex(EMR_JOB_ID, "foo").isEmpty());
  }
}
