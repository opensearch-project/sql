/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.spark.execution.statestore;

import static org.opensearch.sql.spark.data.constants.SparkConstants.SPARK_REQUEST_BUFFER_INDEX_NAME;
import static org.opensearch.sql.spark.execution.statestore.StateModel.STATE;

import com.google.common.annotations.VisibleForTesting;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Locale;
import java.util.Optional;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Supplier;
import lombok.RequiredArgsConstructor;
import org.apache.commons.io.IOUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.DocWriteResponse;
import org.opensearch.action.admin.indices.create.CreateIndexRequest;
import org.opensearch.action.admin.indices.create.CreateIndexResponse;
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
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.sql.spark.asyncquery.model.AsyncQueryJobMetadata;
import org.opensearch.sql.spark.dispatcher.model.IndexDMLResult;
import org.opensearch.sql.spark.execution.session.SessionModel;
import org.opensearch.sql.spark.execution.session.SessionState;
import org.opensearch.sql.spark.execution.session.SessionType;
import org.opensearch.sql.spark.execution.statement.StatementModel;
import org.opensearch.sql.spark.execution.statement.StatementState;
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
  public static Function<String, String> DATASOURCE_TO_REQUEST_INDEX =
      datasourceName ->
          String.format(
              "%s_%s", SPARK_REQUEST_BUFFER_INDEX_NAME, datasourceName.toLowerCase(Locale.ROOT));
  public static String ALL_DATASOURCE = "*";

  private static final Logger LOG = LogManager.getLogger();

  private final Client client;
  private final ClusterService clusterService;

  @VisibleForTesting
  public <T extends StateModel> T create(
      T st, StateModel.CopyBuilder<T> builder, String indexName) {
    try {
      if (!this.clusterService.state().routingTable().hasIndex(indexName)) {
        createIndex(indexName);
      }
      IndexRequest indexRequest =
          new IndexRequest(indexName)
              .id(st.getId())
              .source(st.toXContent(XContentFactory.jsonBuilder(), ToXContent.EMPTY_PARAMS))
              .setIfSeqNo(st.getSeqNo())
              .setIfPrimaryTerm(st.getPrimaryTerm())
              .create(true)
              .setRefreshPolicy(WriteRequest.RefreshPolicy.WAIT_UNTIL);
      try (ThreadContext.StoredContext ignored =
          client.threadPool().getThreadContext().stashContext()) {
        IndexResponse indexResponse = client.index(indexRequest).actionGet();
        if (indexResponse.getResult().equals(DocWriteResponse.Result.CREATED)) {
          LOG.debug("Successfully created doc. id: {}", st.getId());
          return builder.of(st, indexResponse.getSeqNo(), indexResponse.getPrimaryTerm());
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
      String sid, StateModel.FromXContent<T> builder, String indexName) {
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
      T st, S state, StateModel.StateCopyBuilder<T, S> builder, String indexName) {
    try {
      T model = builder.of(st, state, st.getSeqNo(), st.getPrimaryTerm());
      UpdateRequest updateRequest =
          new UpdateRequest()
              .index(indexName)
              .id(model.getId())
              .setIfSeqNo(model.getSeqNo())
              .setIfPrimaryTerm(model.getPrimaryTerm())
              .doc(model.toXContent(XContentFactory.jsonBuilder(), ToXContent.EMPTY_PARAMS))
              .fetchSource(true)
              .setRefreshPolicy(WriteRequest.RefreshPolicy.WAIT_UNTIL);
      try (ThreadContext.StoredContext ignored =
          client.threadPool().getThreadContext().stashContext()) {
        UpdateResponse updateResponse = client.update(updateRequest).actionGet();
        LOG.debug("Successfully update doc. id: {}", st.getId());
        return builder.of(model, state, updateResponse.getSeqNo(), updateResponse.getPrimaryTerm());
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
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

  /** Helper Functions */
  public static Function<StatementModel, StatementModel> createStatement(
      StateStore stateStore, String datasourceName) {
    return (st) ->
        stateStore.create(
            st, StatementModel::copy, DATASOURCE_TO_REQUEST_INDEX.apply(datasourceName));
  }

  public static Function<String, Optional<StatementModel>> getStatement(
      StateStore stateStore, String datasourceName) {
    return (docId) ->
        stateStore.get(
            docId, StatementModel::fromXContent, DATASOURCE_TO_REQUEST_INDEX.apply(datasourceName));
  }

  public static BiFunction<StatementModel, StatementState, StatementModel> updateStatementState(
      StateStore stateStore, String datasourceName) {
    return (old, state) ->
        stateStore.updateState(
            old,
            state,
            StatementModel::copyWithState,
            DATASOURCE_TO_REQUEST_INDEX.apply(datasourceName));
  }

  public static Function<SessionModel, SessionModel> createSession(
      StateStore stateStore, String datasourceName) {
    return (session) ->
        stateStore.create(
            session, SessionModel::of, DATASOURCE_TO_REQUEST_INDEX.apply(datasourceName));
  }

  public static Function<String, Optional<SessionModel>> getSession(
      StateStore stateStore, String datasourceName) {
    return (docId) ->
        stateStore.get(
            docId, SessionModel::fromXContent, DATASOURCE_TO_REQUEST_INDEX.apply(datasourceName));
  }

  public static BiFunction<SessionModel, SessionState, SessionModel> updateSessionState(
      StateStore stateStore, String datasourceName) {
    return (old, state) ->
        stateStore.updateState(
            old,
            state,
            SessionModel::copyWithState,
            DATASOURCE_TO_REQUEST_INDEX.apply(datasourceName));
  }

  public static Function<AsyncQueryJobMetadata, AsyncQueryJobMetadata> createJobMetaData(
      StateStore stateStore, String datasourceName) {
    return (jobMetadata) ->
        stateStore.create(
            jobMetadata,
            AsyncQueryJobMetadata::copy,
            DATASOURCE_TO_REQUEST_INDEX.apply(datasourceName));
  }

  public static Function<String, Optional<AsyncQueryJobMetadata>> getJobMetaData(
      StateStore stateStore, String datasourceName) {
    return (docId) ->
        stateStore.get(
            docId,
            AsyncQueryJobMetadata::fromXContent,
            DATASOURCE_TO_REQUEST_INDEX.apply(datasourceName));
  }

  public static Supplier<Long> activeSessionsCount(StateStore stateStore, String datasourceName) {
    return () ->
        stateStore.count(
            DATASOURCE_TO_REQUEST_INDEX.apply(datasourceName),
            QueryBuilders.boolQuery()
                .must(QueryBuilders.termQuery(SessionModel.TYPE, SessionModel.SESSION_DOC_TYPE))
                .must(
                    QueryBuilders.termQuery(
                        SessionModel.SESSION_TYPE, SessionType.INTERACTIVE.getSessionType()))
                .must(
                    QueryBuilders.termQuery(
                        SessionModel.SESSION_STATE, SessionState.RUNNING.getSessionState())));
  }

  public static BiFunction<FlintIndexStateModel, FlintIndexState, FlintIndexStateModel>
      updateFlintIndexState(StateStore stateStore, String datasourceName) {
    return (old, state) ->
        stateStore.updateState(
            old,
            state,
            FlintIndexStateModel::copyWithState,
            DATASOURCE_TO_REQUEST_INDEX.apply(datasourceName));
  }

  public static Function<String, Optional<FlintIndexStateModel>> getFlintIndexState(
      StateStore stateStore, String datasourceName) {
    return (docId) ->
        stateStore.get(
            docId,
            FlintIndexStateModel::fromXContent,
            DATASOURCE_TO_REQUEST_INDEX.apply(datasourceName));
  }

  public static Function<FlintIndexStateModel, FlintIndexStateModel> createFlintIndexState(
      StateStore stateStore, String datasourceName) {
    return (st) ->
        stateStore.create(
            st, FlintIndexStateModel::copy, DATASOURCE_TO_REQUEST_INDEX.apply(datasourceName));
  }

  public static Function<IndexDMLResult, IndexDMLResult> createIndexDMLResult(
      StateStore stateStore, String indexName) {
    return (result) -> stateStore.create(result, IndexDMLResult::copy, indexName);
  }

  public static Supplier<Long> activeRefreshJobCount(StateStore stateStore, String datasourceName) {
    return () ->
        stateStore.count(
            DATASOURCE_TO_REQUEST_INDEX.apply(datasourceName),
            QueryBuilders.boolQuery()
                .must(
                    QueryBuilders.termQuery(
                        SessionModel.TYPE, FlintIndexStateModel.FLINT_INDEX_DOC_TYPE))
                .must(QueryBuilders.termQuery(STATE, FlintIndexState.REFRESHING.getState())));
  }

  public static Supplier<Long> activeStatementsCount(StateStore stateStore, String datasourceName) {
    return () ->
        stateStore.count(
            DATASOURCE_TO_REQUEST_INDEX.apply(datasourceName),
            QueryBuilders.boolQuery()
                .must(
                    QueryBuilders.termQuery(StatementModel.TYPE, StatementModel.STATEMENT_DOC_TYPE))
                .should(
                    QueryBuilders.termsQuery(
                        StatementModel.STATEMENT_STATE,
                        StatementState.RUNNING.getState(),
                        StatementState.WAITING.getState())));
  }
}
