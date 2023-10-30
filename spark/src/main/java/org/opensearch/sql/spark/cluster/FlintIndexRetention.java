/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.spark.cluster;

import static org.opensearch.sql.spark.execution.session.SessionModel.LAST_UPDATE_TIME;
import static org.opensearch.sql.spark.execution.statement.StatementModel.SUBMIT_TIME;

import java.time.Clock;
import java.time.Duration;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.common.CheckedConsumer;
import org.opensearch.common.time.FormatNames;
import org.opensearch.core.action.ActionListener;
import org.opensearch.index.IndexNotFoundException;
import org.opensearch.index.query.QueryBuilders;

public class FlintIndexRetention implements Runnable {
  private static final Logger LOG = LogManager.getLogger(FlintIndexRetention.class);

  static final String SESSION_INDEX_NOT_EXIST_MSG = "Checkpoint index does not exist.";

  static final String RESULT_INDEX_NOT_EXIST_MSG = "Result index does not exist.";

  // timestamp field in result index
  static final String UPDATE_TIME_FIELD = "updateTime";

  private final Duration defaultSessionTtl;
  private final Duration defaultResultTtl;
  private final Clock clock;
  private final IndexCleanup indexCleanup;
  private final String sessionIndexNameRegex;
  private final String resultIndexNameRegex;

  public FlintIndexRetention(
      Duration defaultSessionTtl,
      Duration defaultResultTtl,
      Clock clock,
      IndexCleanup indexCleanup,
      String sessionIndexNameRegex,
      String resultIndexNameRegex) {
    this.defaultSessionTtl = defaultSessionTtl;
    this.defaultResultTtl = defaultResultTtl;
    this.clock = clock;
    this.indexCleanup = indexCleanup;
    this.sessionIndexNameRegex = sessionIndexNameRegex;
    this.resultIndexNameRegex = resultIndexNameRegex;
  }

  @Override
  public void run() {
    purgeSessionIndex();
  }

  private void purgeSessionIndex() {
    purgeIndex(
        sessionIndexNameRegex,
        defaultSessionTtl,
        LAST_UPDATE_TIME,
        this::handleSessionPurgeResponse,
        this::handleSessionPurgeError);
  }

  private void handleSessionPurgeResponse(Long response) {
    purgeStatementIndex();
  }

  private void handleSessionPurgeError(Exception exception) {
    handlePurgeError(SESSION_INDEX_NOT_EXIST_MSG, "session index", exception);
    purgeStatementIndex();
  }

  private void purgeStatementIndex() {
    purgeIndex(
        sessionIndexNameRegex,
        defaultSessionTtl,
        SUBMIT_TIME,
        this::handleStatementPurgeResponse,
        this::handleStatementPurgeError);
  }

  private void handleStatementPurgeResponse(Long response) {
    purgeResultIndex();
  }

  private void handleStatementPurgeError(Exception exception) {
    handlePurgeError(SESSION_INDEX_NOT_EXIST_MSG, "session index", exception);
    purgeResultIndex();
  }

  private void purgeResultIndex() {
    purgeIndex(
        resultIndexNameRegex,
        defaultResultTtl,
        UPDATE_TIME_FIELD,
        this::handleResultPurgeResponse,
        this::handleResultPurgeError);
  }

  private void handleResultPurgeResponse(Long response) {
    LOG.debug("purge result index done");
  }

  private void handleResultPurgeError(Exception exception) {
    handlePurgeError(RESULT_INDEX_NOT_EXIST_MSG, "result index", exception);
  }

  private void handlePurgeError(String notExistMsg, String indexType, Exception exception) {
    if (exception instanceof IndexNotFoundException) {
      LOG.debug(notExistMsg);
    } else {
      LOG.error("delete docs by query fails for " + indexType, exception);
    }
  }

  private void purgeIndex(
      String indexName,
      Duration ttl,
      String timeStampField,
      CheckedConsumer<Long, Exception> successHandler,
      CheckedConsumer<Exception, Exception> errorHandler) {
    indexCleanup.deleteDocsByQuery(
        indexName,
        QueryBuilders.boolQuery()
            .filter(
                QueryBuilders.rangeQuery(timeStampField)
                    .lte(clock.millis() - ttl.toMillis())
                    .format(FormatNames.EPOCH_MILLIS.getSnakeCaseName())),
        ActionListener.wrap(
            response -> {
              try {
                successHandler.accept(response);
              } catch (Exception e) {
                LOG.error("Error handling response for index " + indexName, e);
              }
            },
            ex -> {
              try {
                errorHandler.accept(ex);
              } catch (Exception e) {
                LOG.error("Error handling error for index " + indexName, e);
              }
            }));
  }
}
