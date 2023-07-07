/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.spark.response;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;
import static org.opensearch.sql.spark.constants.TestConstants.EMR_CLUSTER_ID;
import static org.opensearch.sql.spark.data.constants.SparkConstants.SPARK_INDEX_NAME;

import java.util.Map;
import org.apache.lucene.search.TotalHits;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.ResourceNotFoundException;
import org.opensearch.action.ActionFuture;
import org.opensearch.action.DocWriteResponse;
import org.opensearch.action.delete.DeleteResponse;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.client.Client;
import org.opensearch.rest.RestStatus;
import org.opensearch.search.SearchHit;
import org.opensearch.search.SearchHits;

@ExtendWith(MockitoExtension.class)
public class SparkResponseTest {
  @Mock
  private Client client;
  @Mock
  private SearchResponse searchResponse;
  @Mock
  private DeleteResponse deleteResponse;
  @Mock
  private SearchHit searchHit;
  @Mock
  private ActionFuture<SearchResponse> searchResponseActionFuture;
  @Mock
  private ActionFuture<DeleteResponse> deleteResponseActionFuture;

  @Test
  public void testGetResultFromOpensearchIndex() {
    when(client.search(any())).thenReturn(searchResponseActionFuture);
    when(searchResponseActionFuture.actionGet()).thenReturn(searchResponse);
    when(searchResponse.status()).thenReturn(RestStatus.OK);
    when(searchResponse.getHits())
        .thenReturn(
            new SearchHits(
                new SearchHit[] {searchHit},
                new TotalHits(1, TotalHits.Relation.EQUAL_TO),
                1.0F));
    Mockito.when(searchHit.getSourceAsMap())
        .thenReturn(Map.of("stepId", EMR_CLUSTER_ID));


    when(client.delete(any())).thenReturn(deleteResponseActionFuture);
    when(deleteResponseActionFuture.actionGet()).thenReturn(deleteResponse);
    when(deleteResponse.getResult()).thenReturn(DocWriteResponse.Result.DELETED);

    SparkResponse sparkResponse = new SparkResponse(client, EMR_CLUSTER_ID, "stepId");
    assertFalse(sparkResponse.getResultFromOpensearchIndex().isEmpty());
  }

  @Test
  public void testInvalidSearchResponse() {
    when(client.search(any())).thenReturn(searchResponseActionFuture);
    when(searchResponseActionFuture.actionGet()).thenReturn(searchResponse);
    when(searchResponse.status()).thenReturn(RestStatus.NO_CONTENT);

    SparkResponse sparkResponse = new SparkResponse(client, EMR_CLUSTER_ID, "stepId");
    RuntimeException exception = assertThrows(RuntimeException.class,
        () -> sparkResponse.getResultFromOpensearchIndex());
    Assertions.assertEquals(
        "Fetching result from " + SPARK_INDEX_NAME
            + " index failed with status : " + RestStatus.NO_CONTENT,
        exception.getMessage());
  }

  @Test
  public void testSearchFailure() {
    when(client.search(any())).thenThrow(RuntimeException.class);
    SparkResponse sparkResponse = new SparkResponse(client, EMR_CLUSTER_ID, "stepId");
    assertThrows(RuntimeException.class, () -> sparkResponse.getResultFromOpensearchIndex());
  }

  @Test
  public void testDeleteFailure() {
    when(client.delete(any())).thenThrow(RuntimeException.class);
    SparkResponse sparkResponse = new SparkResponse(client, EMR_CLUSTER_ID, "stepId");
    assertThrows(RuntimeException.class, () -> sparkResponse.deleteInSparkIndex("id"));
  }

  @Test
  public void testNotFoundDeleteResponse() {
    when(client.delete(any())).thenReturn(deleteResponseActionFuture);
    when(deleteResponseActionFuture.actionGet()).thenReturn(deleteResponse);
    when(deleteResponse.getResult()).thenReturn(DocWriteResponse.Result.NOT_FOUND);

    SparkResponse sparkResponse = new SparkResponse(client, EMR_CLUSTER_ID, "stepId");
    RuntimeException exception = assertThrows(ResourceNotFoundException.class,
        () -> sparkResponse.deleteInSparkIndex("123"));
    Assertions.assertEquals("Spark result with id 123 doesn't exist", exception.getMessage());
  }

  @Test
  public void testInvalidDeleteResponse() {
    when(client.delete(any())).thenReturn(deleteResponseActionFuture);
    when(deleteResponseActionFuture.actionGet()).thenReturn(deleteResponse);
    when(deleteResponse.getResult()).thenReturn(DocWriteResponse.Result.NOOP);

    SparkResponse sparkResponse = new SparkResponse(client, EMR_CLUSTER_ID, "stepId");
    RuntimeException exception = assertThrows(RuntimeException.class,
        () -> sparkResponse.deleteInSparkIndex("123"));
    Assertions.assertEquals(
        "Deleting spark result information failed with : noop", exception.getMessage());
  }
}
