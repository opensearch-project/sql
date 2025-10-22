/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.sql;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import org.json.JSONArray;
import org.json.JSONObject;
import org.junit.Test;
import org.opensearch.sql.legacy.SQLIntegTestCase;
import org.opensearch.sql.legacy.TestsConstants;

/** Verifies SQL aggregations remain stable under concurrent execution. */
public class SQLConcurrencyIT extends SQLIntegTestCase {

  private static final int THREAD_POOL_SIZE = 256;
  private static final long QUERY_TIMEOUT_SECONDS = 60;

  @Override
  public void init() throws IOException {
    loadIndex(Index.ACCOUNT);
  }

  @Test
  public void aggregationsHandleConcurrentSqlQueries() throws Exception {
    String countQuery =
        String.format("SELECT COUNT(age) AS cnt FROM %s", TestsConstants.TEST_INDEX_ACCOUNT);
    String sumQuery =
        String.format(
            "SELECT SUM(balance) AS total_sales FROM %s", TestsConstants.TEST_INDEX_ACCOUNT);

    long expectedCount = extractNumber(executeJdbcRequest(countQuery)).longValue();
    double expectedSum = extractNumber(executeJdbcRequest(sumQuery)).doubleValue();

    runConcurrentRound(countQuery, expectedCount, sumQuery, expectedSum);
  }

  private void runConcurrentRound(
      String countQuery, long expectedCount, String sumQuery, double expectedSum) throws Exception {
    ExecutorService executor = Executors.newFixedThreadPool(THREAD_POOL_SIZE);
    List<Callable<Void>> tasks = new ArrayList<>();

    for (int i = 0; i < THREAD_POOL_SIZE; i++) {
      tasks.add(countTask(countQuery, expectedCount));
      tasks.add(sumTask(sumQuery, expectedSum));
    }

    Collections.shuffle(tasks, ThreadLocalRandom.current());

    List<Future<Void>> futures = new ArrayList<>();
    try {
      for (Callable<Void> task : tasks) {
        futures.add(executor.submit(task));
      }
      waitForTasks(futures);
    } finally {
      executor.shutdown();
      if (!executor.awaitTermination(QUERY_TIMEOUT_SECONDS, TimeUnit.SECONDS)) {
        executor.shutdownNow();
      }
    }
  }

  private Callable<Void> countTask(String query, long expected) {
    return () -> {
      long actual = extractNumber(executeJdbcRequest(query)).longValue();
      assertEquals("Unexpected COUNT result", expected, actual);
      return null;
    };
  }

  private Callable<Void> sumTask(String query, double expected) {
    return () -> {
      double actual = extractNumber(executeJdbcRequest(query)).doubleValue();
      assertEquals("Unexpected SUM result", expected, actual, 1e-6);
      return null;
    };
  }

  private void waitForTasks(List<Future<Void>> tasks) throws Exception {
    for (Future<Void> task : tasks) {
      task.get(QUERY_TIMEOUT_SECONDS, TimeUnit.SECONDS);
    }
    tasks.clear();
  }

  private Number extractNumber(JSONObject response) {
    JSONArray rows = response.getJSONArray("datarows");
    assertEquals("Expected a single row", 1, rows.length());
    JSONArray row = rows.getJSONArray(0);
    assertEquals("Expected a single column", 1, row.length());
    Object value = row.get(0);
    assertTrue("Expected numeric result but got " + value, value instanceof Number);
    return (Number) value;
  }
}
