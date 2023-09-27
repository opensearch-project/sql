/*
 *
 *  * Copyright OpenSearch Contributors
 *  * SPDX-License-Identifier: Apache-2.0
 *
 */

package org.opensearch.sql.spark.asyncquery;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import org.apache.commons.io.IOUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.DocWriteRequest;
import org.opensearch.action.DocWriteResponse;
import org.opensearch.action.admin.indices.create.CreateIndexRequest;
import org.opensearch.action.admin.indices.create.CreateIndexResponse;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.action.index.IndexResponse;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.action.support.WriteRequest;
import org.opensearch.client.Client;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.action.ActionFuture;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.search.SearchHit;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.sql.spark.asyncquery.model.AsyncQueryJobMetadata;

/** Opensearch implementation of {@link AsyncQueryJobMetadataStorageService} */
public class OpensearchAsyncQueryJobMetadataStorageService
    implements AsyncQueryJobMetadataStorageService {

  public static final String JOB_METADATA_INDEX = ".ql-job-metadata";
  private static final String JOB_METADATA_INDEX_MAPPING_FILE_NAME =
      "job-metadata-index-mapping.yml";
  private static final String JOB_METADATA_INDEX_SETTINGS_FILE_NAME =
      "job-metadata-index-settings.yml";
  private static final Logger LOG = LogManager.getLogger();
  private final Client client;
  private final ClusterService clusterService;

  /**
   * This class implements JobMetadataStorageService interface using OpenSearch as underlying
   * storage.
   *
   * @param client opensearch NodeClient.
   * @param clusterService ClusterService.
   */
  public OpensearchAsyncQueryJobMetadataStorageService(
      Client client, ClusterService clusterService) {
    this.client = client;
    this.clusterService = clusterService;
  }

  @Override
  public void storeJobMetadata(AsyncQueryJobMetadata asyncQueryJobMetadata) {
    if (!this.clusterService.state().routingTable().hasIndex(JOB_METADATA_INDEX)) {
      createJobMetadataIndex();
    }
    IndexRequest indexRequest = new IndexRequest(JOB_METADATA_INDEX);
    indexRequest.id(asyncQueryJobMetadata.getJobId());
    indexRequest.opType(DocWriteRequest.OpType.CREATE);
    indexRequest.setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
    ActionFuture<IndexResponse> indexResponseActionFuture;
    IndexResponse indexResponse;
    try (ThreadContext.StoredContext storedContext =
        client.threadPool().getThreadContext().stashContext()) {
      indexRequest.source(AsyncQueryJobMetadata.convertToXContent(asyncQueryJobMetadata));
      indexResponseActionFuture = client.index(indexRequest);
      indexResponse = indexResponseActionFuture.actionGet();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }

    if (indexResponse.getResult().equals(DocWriteResponse.Result.CREATED)) {
      LOG.debug("JobMetadata   : {}  successfully created", asyncQueryJobMetadata.getJobId());
    } else {
      throw new RuntimeException(
          "Saving job metadata information failed with result : "
              + indexResponse.getResult().getLowercase());
    }
  }

  @Override
  public Optional<AsyncQueryJobMetadata> getJobMetadata(String jobId) {
    if (!this.clusterService.state().routingTable().hasIndex(JOB_METADATA_INDEX)) {
      createJobMetadataIndex();
      return Optional.empty();
    }
    return searchInJobMetadataIndex(QueryBuilders.termQuery("jobId", jobId)).stream().findFirst();
  }

  private void createJobMetadataIndex() {
    try {
      InputStream mappingFileStream =
          OpensearchAsyncQueryJobMetadataStorageService.class
              .getClassLoader()
              .getResourceAsStream(JOB_METADATA_INDEX_MAPPING_FILE_NAME);
      InputStream settingsFileStream =
          OpensearchAsyncQueryJobMetadataStorageService.class
              .getClassLoader()
              .getResourceAsStream(JOB_METADATA_INDEX_SETTINGS_FILE_NAME);
      CreateIndexRequest createIndexRequest = new CreateIndexRequest(JOB_METADATA_INDEX);
      createIndexRequest
          .mapping(IOUtils.toString(mappingFileStream, StandardCharsets.UTF_8), XContentType.YAML)
          .settings(
              IOUtils.toString(settingsFileStream, StandardCharsets.UTF_8), XContentType.YAML);
      ActionFuture<CreateIndexResponse> createIndexResponseActionFuture;
      try (ThreadContext.StoredContext ignored =
          client.threadPool().getThreadContext().stashContext()) {
        createIndexResponseActionFuture = client.admin().indices().create(createIndexRequest);
      }
      CreateIndexResponse createIndexResponse = createIndexResponseActionFuture.actionGet();
      if (createIndexResponse.isAcknowledged()) {
        LOG.info("Index: {} creation Acknowledged", JOB_METADATA_INDEX);
      } else {
        throw new RuntimeException("Index creation is not acknowledged.");
      }
    } catch (Throwable e) {
      throw new RuntimeException(
          "Internal server error while creating"
              + JOB_METADATA_INDEX
              + " index:: "
              + e.getMessage());
    }
  }

  private List<AsyncQueryJobMetadata> searchInJobMetadataIndex(QueryBuilder query) {
    SearchRequest searchRequest = new SearchRequest();
    searchRequest.indices(JOB_METADATA_INDEX);
    SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
    searchSourceBuilder.query(query);
    searchSourceBuilder.size(1);
    searchRequest.source(searchSourceBuilder);
    // https://github.com/opensearch-project/sql/issues/1801.
    searchRequest.preference("_primary_first");
    ActionFuture<SearchResponse> searchResponseActionFuture;
    try (ThreadContext.StoredContext ignored =
        client.threadPool().getThreadContext().stashContext()) {
      searchResponseActionFuture = client.search(searchRequest);
    }
    SearchResponse searchResponse = searchResponseActionFuture.actionGet();
    if (searchResponse.status().getStatus() != 200) {
      throw new RuntimeException(
          "Fetching job metadata information failed with status : " + searchResponse.status());
    } else {
      List<AsyncQueryJobMetadata> list = new ArrayList<>();
      for (SearchHit searchHit : searchResponse.getHits().getHits()) {
        String sourceAsString = searchHit.getSourceAsString();
        AsyncQueryJobMetadata asyncQueryJobMetadata;
        try {
          asyncQueryJobMetadata = AsyncQueryJobMetadata.toJobMetadata(sourceAsString);
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
        list.add(asyncQueryJobMetadata);
      }
      return list;
    }
  }
}
