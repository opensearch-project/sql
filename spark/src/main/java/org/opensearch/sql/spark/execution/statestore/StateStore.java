/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.spark.execution.statestore;

import com.google.common.annotations.VisibleForTesting;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Locale;
import java.util.Optional;
import java.util.function.Function;
import java.util.function.Supplier;
import lombok.RequiredArgsConstructor;
import org.apache.commons.io.IOUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.DocWriteResponse;
import org.opensearch.action.admin.indices.create.CreateIndexRequest;
import org.opensearch.action.admin.indices.create.CreateIndexResponse;
import org.opensearch.action.delete.DeleteRequest;
import org.opensearch.action.delete.DeleteResponse;
import org.opensearch.action.get.GetRequest;
import org.opensearch.action.get.GetResponse;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.action.index.IndexResponse;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.action.support.WriteRequest;
import org.opensearch.action.update.UpdateRequest;
import org.opensearch.action.update.UpdateResponse;
import org.opensearch.client.Client;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.action.ActionFuture;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.common.xcontent.LoggingDeprecationHandler;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.index.seqno.SequenceNumbers;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.sql.spark.asyncquery.model.AsyncQueryJobMetadata;
import org.opensearch.sql.spark.dispatcher.model.IndexDMLResult;
import org.opensearch.sql.spark.execution.session.SessionModel;
import org.opensearch.sql.spark.execution.session.SessionState;
import org.opensearch.sql.spark.execution.session.SessionType;
import org.opensearch.sql.spark.execution.statement.StatementModel;
import org.opensearch.sql.spark.execution.statement.StatementState;
import org.opensearch.sql.spark.execution.xcontent.AsyncQueryJobMetadataXContentSerializer;
import org.opensearch.sql.spark.execution.xcontent.FlintIndexStateModelXContentSerializer;
import org.opensearch.sql.spark.execution.xcontent.IndexDMLResultXContentSerializer;
import org.opensearch.sql.spark.execution.xcontent.SessionModelXContentSerializer;
import org.opensearch.sql.spark.execution.xcontent.StatementModelXContentSerializer;
import org.opensearch.sql.spark.execution.xcontent.XContentCommonAttributes;
import org.opensearch.sql.spark.execution.xcontent.XContentSerializer;
import org.opensearch.sql.spark.execution.xcontent.XContentSerializerUtil;
import org.opensearch.sql.spark.flint.FlintIndexState;
import org.opensearch.sql.spark.flint.FlintIndexStateModel;

/**
 * State Store maintain the state of Session and Statement. State State create/update/get doc on
 * index regardless user FGAC permissions.
 */
@RequiredArgsConstructor
public class StateStore {
  public static String SETTINGS_FILE_NAME = "query_execution_request_settings.yml";
  public static String MAPPING_FILE_NAME = "query_execution_request_mapping.yml";
  public static String ALL_DATASOURCE = "*";

  private static final Logger LOG = LogManager.getLogger();

  private final Client client;
  private final ClusterService clusterService;

  @VisibleForTesting
  public <T extends StateModel> T create(T st, CopyBuilder<T> builder, String indexName) {
    try {
      if (!this.clusterService.state().routingTable().hasIndex(indexName)) {
        createIndex(indexName);
      }
      XContentSerializer<T> serializer = getXContentSerializer(st);
      IndexRequest indexRequest =
          new IndexRequest(indexName)
              .id(st.getId())
              .source(serializer.toXContent(st, ToXContent.EMPTY_PARAMS))
              .setIfSeqNo(getSeqNo(st))
              .setIfPrimaryTerm(getPrimaryTerm(st))
              .create(true)
              .setRefreshPolicy(WriteRequest.RefreshPolicy.WAIT_UNTIL);
      try (ThreadContext.StoredContext ignored =
          client.threadPool().getThreadContext().stashContext()) {
        IndexResponse indexResponse = client.index(indexRequest).actionGet();
        if (indexResponse.getResult().equals(DocWriteResponse.Result.CREATED)) {
          LOG.debug("Successfully created doc. id: {}", st.getId());
          return builder.of(
              st,
              XContentSerializerUtil.buildMetadata(
                  indexResponse.getSeqNo(), indexResponse.getPrimaryTerm()));
        } else {
          throw new RuntimeException(
              String.format(
                  Locale.ROOT,
                  "Failed create doc. id: %s, error: %s",
                  st.getId(),
                  indexResponse.getResult().getLowercase()));
        }
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @VisibleForTesting
  public <T extends StateModel> Optional<T> get(
      String sid, FromXContent<T> builder, String indexName) {
    try {
      if (!this.clusterService.state().routingTable().hasIndex(indexName)) {
        createIndex(indexName);
        return Optional.empty();
      }
      GetRequest getRequest = new GetRequest().index(indexName).id(sid).refresh(true);
      try (ThreadContext.StoredContext ignored =
          client.threadPool().getThreadContext().stashContext()) {
        GetResponse getResponse = client.get(getRequest).actionGet();
        if (getResponse.isExists()) {
          XContentParser parser =
              XContentType.JSON
                  .xContent()
                  .createParser(
                      NamedXContentRegistry.EMPTY,
                      LoggingDeprecationHandler.INSTANCE,
                      getResponse.getSourceAsString());
          parser.nextToken();
          return Optional.of(
              builder.fromXContent(parser, getResponse.getSeqNo(), getResponse.getPrimaryTerm()));
        } else {
          return Optional.empty();
        }
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @VisibleForTesting
  public <T extends StateModel, S> T updateState(
      T st, S state, StateCopyBuilder<T, S> builder, String indexName) {
    try {
      T model = builder.of(st, state, st.getMetadata());
      XContentSerializer<T> serializer = getXContentSerializer(st);
      UpdateRequest updateRequest =
          new UpdateRequest()
              .index(indexName)
              .id(model.getId())
              .setIfSeqNo(getSeqNo(model))
              .setIfPrimaryTerm(getPrimaryTerm(model))
              .doc(serializer.toXContent(model, ToXContent.EMPTY_PARAMS))
              .fetchSource(true)
              .setRefreshPolicy(WriteRequest.RefreshPolicy.WAIT_UNTIL);
      try (ThreadContext.StoredContext ignored =
          client.threadPool().getThreadContext().stashContext()) {
        UpdateResponse updateResponse = client.update(updateRequest).actionGet();
        LOG.debug("Successfully update doc. id: {}", st.getId());
        return builder.of(
            model,
            state,
            XContentSerializerUtil.buildMetadata(
                updateResponse.getSeqNo(), updateResponse.getPrimaryTerm()));
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private long getSeqNo(StateModel model) {
    return model.getMetadataItem("seqNo", Long.class).orElse(SequenceNumbers.UNASSIGNED_SEQ_NO);
  }

  private long getPrimaryTerm(StateModel model) {
    return model
        .getMetadataItem("primaryTerm", Long.class)
        .orElse(SequenceNumbers.UNASSIGNED_PRIMARY_TERM);
  }

  /**
   * Delete the index state document with the given ID.
   *
   * @param sid index state doc ID
   * @param indexName index store index name
   * @return true if deleted, otherwise false
   */
  @VisibleForTesting
  public boolean delete(String sid, String indexName) {
    try {
      // No action if the index doesn't exist
      if (!this.clusterService.state().routingTable().hasIndex(indexName)) {
        return true;
      }

      try (ThreadContext.StoredContext ignored =
          client.threadPool().getThreadContext().stashContext()) {
        DeleteRequest deleteRequest = new DeleteRequest(indexName, sid);
        DeleteResponse deleteResponse = client.delete(deleteRequest).actionGet();
        return deleteResponse.getResult() == DocWriteResponse.Result.DELETED;
      }
    } catch (Exception e) {
      throw new RuntimeException(
          String.format("Failed to delete index state doc %s in index %s", sid, indexName), e);
    }
  }

  private void createIndex(String indexName) {
    try {
      CreateIndexRequest createIndexRequest = new CreateIndexRequest(indexName);
      createIndexRequest
          .mapping(loadConfigFromResource(MAPPING_FILE_NAME), XContentType.YAML)
          .settings(loadConfigFromResource(SETTINGS_FILE_NAME), XContentType.YAML);
      ActionFuture<CreateIndexResponse> createIndexResponseActionFuture;
      try (ThreadContext.StoredContext ignored =
          client.threadPool().getThreadContext().stashContext()) {
        createIndexResponseActionFuture = client.admin().indices().create(createIndexRequest);
      }
      CreateIndexResponse createIndexResponse = createIndexResponseActionFuture.actionGet();
      if (createIndexResponse.isAcknowledged()) {
        LOG.info("Index: {} creation Acknowledged", indexName);
      } else {
        throw new RuntimeException("Index creation is not acknowledged.");
      }
    } catch (Throwable e) {
      throw new RuntimeException(
          "Internal server error while creating" + indexName + " index:: " + e.getMessage());
    }
  }

  private long count(String indexName, QueryBuilder query) {
    SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
    searchSourceBuilder.query(query);
    searchSourceBuilder.size(0);

    // https://github.com/opensearch-project/sql/issues/1801.
    SearchRequest searchRequest =
        new SearchRequest()
            .indices(indexName)
            .preference("_primary_first")
            .source(searchSourceBuilder);

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
      return searchResponse.getHits().getTotalHits().value;
    }
  }

  private String loadConfigFromResource(String fileName) throws IOException {
    InputStream fileStream = StateStore.class.getClassLoader().getResourceAsStream(fileName);
    return IOUtils.toString(fileStream, StandardCharsets.UTF_8);
  }

  public static Function<AsyncQueryJobMetadata, AsyncQueryJobMetadata> createJobMetaData(
      StateStore stateStore, String datasourceName) {
    return (jobMetadata) ->
        stateStore.create(
            jobMetadata,
            AsyncQueryJobMetadata::copy,
            OpenSearchStateStoreUtil.getIndexName(datasourceName));
  }

  public static Function<String, Optional<AsyncQueryJobMetadata>> getJobMetaData(
      StateStore stateStore, String datasourceName) {
    AsyncQueryJobMetadataXContentSerializer asyncQueryJobMetadataXContentSerializer =
        new AsyncQueryJobMetadataXContentSerializer();
    return (docId) ->
        stateStore.get(
            docId,
            asyncQueryJobMetadataXContentSerializer::fromXContent,
            OpenSearchStateStoreUtil.getIndexName(datasourceName));
  }

  public static Supplier<Long> activeSessionsCount(StateStore stateStore, String datasourceName) {
    return () ->
        stateStore.count(
            OpenSearchStateStoreUtil.getIndexName(datasourceName),
            QueryBuilders.boolQuery()
                .must(
                    QueryBuilders.termQuery(
                        XContentCommonAttributes.TYPE,
                        SessionModelXContentSerializer.SESSION_DOC_TYPE))
                .must(
                    QueryBuilders.termQuery(
                        SessionModelXContentSerializer.SESSION_TYPE,
                        SessionType.INTERACTIVE.getSessionType()))
                .must(
                    QueryBuilders.termQuery(
                        XContentCommonAttributes.STATE, SessionState.RUNNING.getSessionState())));
  }

  public static Supplier<Long> activeRefreshJobCount(StateStore stateStore, String datasourceName) {
    return () ->
        stateStore.count(
            OpenSearchStateStoreUtil.getIndexName(datasourceName),
            QueryBuilders.boolQuery()
                .must(
                    QueryBuilders.termQuery(
                        XContentCommonAttributes.TYPE,
                        FlintIndexStateModelXContentSerializer.FLINT_INDEX_DOC_TYPE))
                .must(
                    QueryBuilders.termQuery(
                        XContentCommonAttributes.STATE, FlintIndexState.REFRESHING.getState())));
  }

  public static Supplier<Long> activeStatementsCount(StateStore stateStore, String datasourceName) {
    return () ->
        stateStore.count(
            OpenSearchStateStoreUtil.getIndexName(datasourceName),
            QueryBuilders.boolQuery()
                .must(
                    QueryBuilders.termQuery(
                        XContentCommonAttributes.TYPE,
                        StatementModelXContentSerializer.STATEMENT_DOC_TYPE))
                .should(
                    QueryBuilders.termsQuery(
                        XContentCommonAttributes.STATE,
                        StatementState.RUNNING.getState(),
                        StatementState.WAITING.getState())));
  }

  @SuppressWarnings("unchecked")
  private <T extends StateModel> XContentSerializer<T> getXContentSerializer(T st) {
    if (st instanceof StatementModel) {
      return (XContentSerializer<T>) new StatementModelXContentSerializer();
    } else if (st instanceof SessionModel) {
      return (XContentSerializer<T>) new SessionModelXContentSerializer();
    } else if (st instanceof FlintIndexStateModel) {
      return (XContentSerializer<T>) new FlintIndexStateModelXContentSerializer();
    } else if (st instanceof AsyncQueryJobMetadata) {
      return (XContentSerializer<T>) new AsyncQueryJobMetadataXContentSerializer();
    } else if (st instanceof IndexDMLResult) {
      return (XContentSerializer<T>) new IndexDMLResultXContentSerializer();
    } else {
      throw new IllegalArgumentException(
          "Unsupported StateModel subclass: " + st.getClass().getSimpleName());
    }
  }
}
