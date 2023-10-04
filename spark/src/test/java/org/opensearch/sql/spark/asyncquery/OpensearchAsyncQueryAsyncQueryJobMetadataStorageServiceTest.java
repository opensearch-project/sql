/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.spark.asyncquery;

import static org.opensearch.sql.spark.asyncquery.OpensearchAsyncQueryJobMetadataStorageService.JOB_METADATA_INDEX;
import static org.opensearch.sql.spark.constants.TestConstants.EMRS_APPLICATION_ID;
import static org.opensearch.sql.spark.constants.TestConstants.EMR_JOB_ID;

import java.util.Optional;
import org.apache.lucene.search.TotalHits;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Answers;
import org.mockito.ArgumentMatchers;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.action.DocWriteResponse;
import org.opensearch.action.admin.indices.create.CreateIndexResponse;
import org.opensearch.action.index.IndexResponse;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.client.Client;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.action.ActionFuture;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.search.SearchHit;
import org.opensearch.search.SearchHits;
import org.opensearch.sql.spark.asyncquery.model.AsyncQueryJobMetadata;

@ExtendWith(MockitoExtension.class)
public class OpensearchAsyncQueryAsyncQueryJobMetadataStorageServiceTest {

  @Mock(answer = Answers.RETURNS_DEEP_STUBS)
  private Client client;

  @Mock(answer = Answers.RETURNS_DEEP_STUBS)
  private ClusterService clusterService;

  @Mock(answer = Answers.RETURNS_DEEP_STUBS)
  private SearchResponse searchResponse;

  @Mock private ActionFuture<SearchResponse> searchResponseActionFuture;
  @Mock private ActionFuture<CreateIndexResponse> createIndexResponseActionFuture;
  @Mock private ActionFuture<IndexResponse> indexResponseActionFuture;
  @Mock private IndexResponse indexResponse;
  @Mock private SearchHit searchHit;

  @InjectMocks
  private OpensearchAsyncQueryJobMetadataStorageService opensearchJobMetadataStorageService;

  @Test
  public void testStoreJobMetadata() {

    Mockito.when(clusterService.state().routingTable().hasIndex(JOB_METADATA_INDEX))
        .thenReturn(Boolean.FALSE);
    Mockito.when(client.admin().indices().create(ArgumentMatchers.any()))
        .thenReturn(createIndexResponseActionFuture);
    Mockito.when(createIndexResponseActionFuture.actionGet())
        .thenReturn(new CreateIndexResponse(true, true, JOB_METADATA_INDEX));
    Mockito.when(client.index(ArgumentMatchers.any())).thenReturn(indexResponseActionFuture);
    Mockito.when(indexResponseActionFuture.actionGet()).thenReturn(indexResponse);
    Mockito.when(indexResponse.getResult()).thenReturn(DocWriteResponse.Result.CREATED);
    AsyncQueryJobMetadata asyncQueryJobMetadata =
        new AsyncQueryJobMetadata(EMR_JOB_ID, EMRS_APPLICATION_ID);

    this.opensearchJobMetadataStorageService.storeJobMetadata(asyncQueryJobMetadata);

    Mockito.verify(client.admin().indices(), Mockito.times(1)).create(ArgumentMatchers.any());
    Mockito.verify(client, Mockito.times(1)).index(ArgumentMatchers.any());
    Mockito.verify(client.threadPool().getThreadContext(), Mockito.times(2)).stashContext();
  }

  @Test
  public void testStoreJobMetadataWithOutCreatingIndex() {
    Mockito.when(clusterService.state().routingTable().hasIndex(JOB_METADATA_INDEX))
        .thenReturn(Boolean.TRUE);
    Mockito.when(client.index(ArgumentMatchers.any())).thenReturn(indexResponseActionFuture);
    Mockito.when(indexResponseActionFuture.actionGet()).thenReturn(indexResponse);
    Mockito.when(indexResponse.getResult()).thenReturn(DocWriteResponse.Result.CREATED);
    AsyncQueryJobMetadata asyncQueryJobMetadata =
        new AsyncQueryJobMetadata(EMR_JOB_ID, EMRS_APPLICATION_ID);

    this.opensearchJobMetadataStorageService.storeJobMetadata(asyncQueryJobMetadata);

    Mockito.verify(client.admin().indices(), Mockito.times(0)).create(ArgumentMatchers.any());
    Mockito.verify(client, Mockito.times(1)).index(ArgumentMatchers.any());
    Mockito.verify(client.threadPool().getThreadContext(), Mockito.times(1)).stashContext();
  }

  @Test
  public void testStoreJobMetadataWithException() {

    Mockito.when(clusterService.state().routingTable().hasIndex(JOB_METADATA_INDEX))
        .thenReturn(Boolean.FALSE);
    Mockito.when(client.admin().indices().create(ArgumentMatchers.any()))
        .thenReturn(createIndexResponseActionFuture);
    Mockito.when(createIndexResponseActionFuture.actionGet())
        .thenReturn(new CreateIndexResponse(true, true, JOB_METADATA_INDEX));
    Mockito.when(client.index(ArgumentMatchers.any()))
        .thenThrow(new RuntimeException("error while indexing"));

    AsyncQueryJobMetadata asyncQueryJobMetadata =
        new AsyncQueryJobMetadata(EMR_JOB_ID, EMRS_APPLICATION_ID);
    RuntimeException runtimeException =
        Assertions.assertThrows(
            RuntimeException.class,
            () -> this.opensearchJobMetadataStorageService.storeJobMetadata(asyncQueryJobMetadata));
    Assertions.assertEquals(
        "java.lang.RuntimeException: error while indexing", runtimeException.getMessage());

    Mockito.verify(client.admin().indices(), Mockito.times(1)).create(ArgumentMatchers.any());
    Mockito.verify(client, Mockito.times(1)).index(ArgumentMatchers.any());
    Mockito.verify(client.threadPool().getThreadContext(), Mockito.times(2)).stashContext();
  }

  @Test
  public void testStoreJobMetadataWithIndexCreationFailed() {

    Mockito.when(clusterService.state().routingTable().hasIndex(JOB_METADATA_INDEX))
        .thenReturn(Boolean.FALSE);
    Mockito.when(client.admin().indices().create(ArgumentMatchers.any()))
        .thenReturn(createIndexResponseActionFuture);
    Mockito.when(createIndexResponseActionFuture.actionGet())
        .thenReturn(new CreateIndexResponse(false, false, JOB_METADATA_INDEX));

    AsyncQueryJobMetadata asyncQueryJobMetadata =
        new AsyncQueryJobMetadata(EMR_JOB_ID, EMRS_APPLICATION_ID);
    RuntimeException runtimeException =
        Assertions.assertThrows(
            RuntimeException.class,
            () -> this.opensearchJobMetadataStorageService.storeJobMetadata(asyncQueryJobMetadata));
    Assertions.assertEquals(
        "Internal server error while creating.ql-job-metadata index:: "
            + "Index creation is not acknowledged.",
        runtimeException.getMessage());

    Mockito.verify(client.admin().indices(), Mockito.times(1)).create(ArgumentMatchers.any());
    Mockito.verify(client.threadPool().getThreadContext(), Mockito.times(1)).stashContext();
  }

  @Test
  public void testStoreJobMetadataFailedWithNotFoundResponse() {

    Mockito.when(clusterService.state().routingTable().hasIndex(JOB_METADATA_INDEX))
        .thenReturn(Boolean.FALSE);
    Mockito.when(client.admin().indices().create(ArgumentMatchers.any()))
        .thenReturn(createIndexResponseActionFuture);
    Mockito.when(createIndexResponseActionFuture.actionGet())
        .thenReturn(new CreateIndexResponse(true, true, JOB_METADATA_INDEX));
    Mockito.when(client.index(ArgumentMatchers.any())).thenReturn(indexResponseActionFuture);
    Mockito.when(indexResponseActionFuture.actionGet()).thenReturn(indexResponse);
    Mockito.when(indexResponse.getResult()).thenReturn(DocWriteResponse.Result.NOT_FOUND);

    AsyncQueryJobMetadata asyncQueryJobMetadata =
        new AsyncQueryJobMetadata(EMR_JOB_ID, EMRS_APPLICATION_ID);
    RuntimeException runtimeException =
        Assertions.assertThrows(
            RuntimeException.class,
            () -> this.opensearchJobMetadataStorageService.storeJobMetadata(asyncQueryJobMetadata));
    Assertions.assertEquals(
        "Saving job metadata information failed with result : not_found",
        runtimeException.getMessage());

    Mockito.verify(client.admin().indices(), Mockito.times(1)).create(ArgumentMatchers.any());
    Mockito.verify(client, Mockito.times(1)).index(ArgumentMatchers.any());
    Mockito.verify(client.threadPool().getThreadContext(), Mockito.times(2)).stashContext();
  }

  @Test
  public void testGetJobMetadata() {
    Mockito.when(clusterService.state().routingTable().hasIndex(JOB_METADATA_INDEX))
        .thenReturn(true);
    Mockito.when(client.search(ArgumentMatchers.any())).thenReturn(searchResponseActionFuture);
    Mockito.when(searchResponseActionFuture.actionGet()).thenReturn(searchResponse);
    Mockito.when(searchResponse.status()).thenReturn(RestStatus.OK);
    Mockito.when(searchResponse.getHits())
        .thenReturn(
            new SearchHits(
                new SearchHit[] {searchHit}, new TotalHits(21, TotalHits.Relation.EQUAL_TO), 1.0F));
    AsyncQueryJobMetadata asyncQueryJobMetadata =
        new AsyncQueryJobMetadata(EMRS_APPLICATION_ID, EMR_JOB_ID);
    Mockito.when(searchHit.getSourceAsString()).thenReturn(asyncQueryJobMetadata.toString());

    Optional<AsyncQueryJobMetadata> jobMetadataOptional =
        opensearchJobMetadataStorageService.getJobMetadata(EMR_JOB_ID);
    Assertions.assertTrue(jobMetadataOptional.isPresent());
    Assertions.assertEquals(EMR_JOB_ID, jobMetadataOptional.get().getJobId());
    Assertions.assertEquals(EMRS_APPLICATION_ID, jobMetadataOptional.get().getApplicationId());
  }

  @Test
  public void testGetJobMetadataWith404SearchResponse() {
    Mockito.when(clusterService.state().routingTable().hasIndex(JOB_METADATA_INDEX))
        .thenReturn(true);
    Mockito.when(client.search(ArgumentMatchers.any())).thenReturn(searchResponseActionFuture);
    Mockito.when(searchResponseActionFuture.actionGet()).thenReturn(searchResponse);
    Mockito.when(searchResponse.status()).thenReturn(RestStatus.NOT_FOUND);

    RuntimeException runtimeException =
        Assertions.assertThrows(
            RuntimeException.class,
            () -> opensearchJobMetadataStorageService.getJobMetadata(EMR_JOB_ID));
    Assertions.assertEquals(
        "Fetching job metadata information failed with status : NOT_FOUND",
        runtimeException.getMessage());
  }

  @Test
  public void testGetJobMetadataWithParsingFailed() {
    Mockito.when(clusterService.state().routingTable().hasIndex(JOB_METADATA_INDEX))
        .thenReturn(true);
    Mockito.when(client.search(ArgumentMatchers.any())).thenReturn(searchResponseActionFuture);
    Mockito.when(searchResponseActionFuture.actionGet()).thenReturn(searchResponse);
    Mockito.when(searchResponse.status()).thenReturn(RestStatus.OK);
    Mockito.when(searchResponse.getHits())
        .thenReturn(
            new SearchHits(
                new SearchHit[] {searchHit}, new TotalHits(21, TotalHits.Relation.EQUAL_TO), 1.0F));
    Mockito.when(searchHit.getSourceAsString()).thenReturn("..tesJOBs");

    Assertions.assertThrows(
        RuntimeException.class,
        () -> opensearchJobMetadataStorageService.getJobMetadata(EMR_JOB_ID));
  }

  @Test
  public void testGetJobMetadataWithNoIndex() {
    Mockito.when(clusterService.state().routingTable().hasIndex(JOB_METADATA_INDEX))
        .thenReturn(Boolean.FALSE);
    Mockito.when(client.admin().indices().create(ArgumentMatchers.any()))
        .thenReturn(createIndexResponseActionFuture);
    Mockito.when(createIndexResponseActionFuture.actionGet())
        .thenReturn(new CreateIndexResponse(true, true, JOB_METADATA_INDEX));
    Mockito.when(client.index(ArgumentMatchers.any())).thenReturn(indexResponseActionFuture);

    Optional<AsyncQueryJobMetadata> jobMetadata =
        opensearchJobMetadataStorageService.getJobMetadata(EMR_JOB_ID);

    Assertions.assertFalse(jobMetadata.isPresent());
  }
}
