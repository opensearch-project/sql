/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.spark.execution.statestore;

import java.io.IOException;
import java.util.Locale;
import java.util.Optional;
import lombok.RequiredArgsConstructor;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.DocWriteResponse;
import org.opensearch.action.get.GetRequest;
import org.opensearch.action.get.GetResponse;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.action.index.IndexResponse;
import org.opensearch.action.support.WriteRequest;
import org.opensearch.client.Client;
import org.opensearch.common.xcontent.LoggingDeprecationHandler;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.sql.spark.execution.session.SessionId;
import org.opensearch.sql.spark.execution.session.SessionModel;

@RequiredArgsConstructor
public class SessionStateStore {
  private static final Logger LOG = LogManager.getLogger();

  private final String indexName;
  private final Client client;

  public SessionModel create(SessionModel session) {
    try {
      IndexRequest indexRequest =
          new IndexRequest(indexName)
              .id(session.getSessionId().getSessionId())
              .source(session.toXContent(XContentFactory.jsonBuilder(), ToXContent.EMPTY_PARAMS))
              .setIfSeqNo(session.getSeqNo())
              .setIfPrimaryTerm(session.getPrimaryTerm())
              .create(true)
              .setRefreshPolicy(WriteRequest.RefreshPolicy.WAIT_UNTIL);
      IndexResponse indexResponse = client.index(indexRequest).actionGet();
      if (indexResponse.getResult().equals(DocWriteResponse.Result.CREATED)) {
        LOG.debug("Successfully created doc. id: {}", session.getSessionId());
        return SessionModel.of(session, indexResponse.getSeqNo(), indexResponse.getPrimaryTerm());
      } else {
        throw new RuntimeException(
            String.format(
                Locale.ROOT,
                "Failed create doc. id: %s, error: %s",
                session.getSessionId(),
                indexResponse.getResult().getLowercase()));
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  public Optional<SessionModel> get(SessionId sid) {
    try {
      GetRequest getRequest = new GetRequest().index(indexName).id(sid.getSessionId());
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
            SessionModel.fromXContent(
                parser, getResponse.getSeqNo(), getResponse.getPrimaryTerm()));
      } else {
        return Optional.empty();
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
}
