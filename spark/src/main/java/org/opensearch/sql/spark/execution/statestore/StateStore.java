/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.spark.execution.statestore;

import java.io.IOException;
import java.util.Locale;
import java.util.Optional;
import java.util.function.BiFunction;
import java.util.function.Function;
import lombok.RequiredArgsConstructor;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.DocWriteResponse;
import org.opensearch.action.get.GetRequest;
import org.opensearch.action.get.GetResponse;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.action.index.IndexResponse;
import org.opensearch.action.support.WriteRequest;
import org.opensearch.action.update.UpdateRequest;
import org.opensearch.action.update.UpdateResponse;
import org.opensearch.client.Client;
import org.opensearch.common.xcontent.LoggingDeprecationHandler;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.sql.spark.execution.session.SessionModel;
import org.opensearch.sql.spark.execution.session.SessionState;
import org.opensearch.sql.spark.execution.statement.StatementModel;
import org.opensearch.sql.spark.execution.statement.StatementState;

@RequiredArgsConstructor
public class StateStore {
  private static final Logger LOG = LogManager.getLogger();

  private final String indexName;
  private final Client client;

  protected <T extends StateModel> T create(T st, StateModel.CopyBuilder<T> builder) {
    try {
      IndexRequest indexRequest =
          new IndexRequest(indexName)
              .id(st.getId())
              .source(st.toXContent(XContentFactory.jsonBuilder(), ToXContent.EMPTY_PARAMS))
              .setIfSeqNo(st.getSeqNo())
              .setIfPrimaryTerm(st.getPrimaryTerm())
              .create(true)
              .setRefreshPolicy(WriteRequest.RefreshPolicy.WAIT_UNTIL);
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
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  protected <T extends StateModel> Optional<T> get(String sid, StateModel.FromXContent<T> builder) {
    try {
      GetRequest getRequest = new GetRequest().index(indexName).id(sid);
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
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  protected <T extends StateModel, S> T updateState(
      T st, S state, StateModel.StateCopyBuilder<T, S> builder) {
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
      UpdateResponse updateResponse = client.update(updateRequest).actionGet();
      if (updateResponse.getResult().equals(DocWriteResponse.Result.UPDATED)) {
        LOG.debug("Successfully update doc. id: {}", st.getId());
        return builder.of(model, state, updateResponse.getSeqNo(), updateResponse.getPrimaryTerm());
      } else {
        throw new RuntimeException(
            String.format(
                Locale.ROOT,
                "Failed update doc. id: %s, error: %s",
                st.getId(),
                updateResponse.getResult().getLowercase()));
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  /** Helper Functions */
  public static Function<StatementModel, StatementModel> createStatement(StateStore stateStore) {
    return (st) -> stateStore.create(st, StatementModel::copy);
  }

  public static Function<String, Optional<StatementModel>> getStatement(StateStore stateStore) {
    return (docId) -> stateStore.get(docId, StatementModel::fromXContent);
  }

  public static BiFunction<StatementModel, StatementState, StatementModel> updateStatementState(
      StateStore stateStore) {
    return (old, state) -> stateStore.updateState(old, state, StatementModel::copyWithState);
  }

  public static Function<SessionModel, SessionModel> createSession(StateStore stateStore) {
    return (session) -> stateStore.create(session, SessionModel::of);
  }

  public static Function<String, Optional<SessionModel>> getSession(StateStore stateStore) {
    return (docId) -> stateStore.get(docId, SessionModel::fromXContent);
  }

  public static BiFunction<SessionModel, SessionState, SessionModel> updateSessionState(
      StateStore stateStore) {
    return (old, state) -> stateStore.updateState(old, state, SessionModel::copyWithState);
  }
}
