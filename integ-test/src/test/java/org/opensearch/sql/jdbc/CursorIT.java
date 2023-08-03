/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.jdbc;

import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_ACCOUNT;
import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_BANK;
import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_CALCS;
import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_ONLINE;
import static org.opensearch.sql.legacy.plugin.RestSqlAction.QUERY_API_ENDPOINT;
import static org.opensearch.sql.util.TestUtils.getResponseBody;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakScope;
import java.io.File;
import java.net.URL;
import java.net.URLClassLoader;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.List;
import javax.annotation.Nullable;
import lombok.SneakyThrows;
import org.json.JSONObject;
import org.junit.AfterClass;
import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator;
import org.junit.jupiter.api.Test;
import org.opensearch.client.Request;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.Response;
import org.opensearch.sql.legacy.SQLIntegTestCase;

@DisplayNameGeneration(DisplayNameGenerator.ReplaceUnderscores.class)
@ThreadLeakScope(ThreadLeakScope.Scope.NONE)
public class CursorIT extends SQLIntegTestCase {

  private static Connection connection;
  private boolean initialized = false;

  @BeforeEach
  @SneakyThrows
  public void init() {
    if (!initialized) {
      initClient();
      resetQuerySizeLimit();
      loadIndex(Index.BANK);
      loadIndex(Index.CALCS);
      loadIndex(Index.ONLINE);
      loadIndex(Index.ACCOUNT);
      initialized = true;
    }
  }

  @BeforeAll
  @BeforeClass
  @SneakyThrows
  public static void initConnection() {
    var driverFile = System.getProperty("jdbcFile");
    if (driverFile != null) {
      URLClassLoader loader = new URLClassLoader(
              new URL[]{new File(driverFile).toURI().toURL()},
              ClassLoader.getSystemClassLoader()
      );
      Driver driver = (Driver) Class.forName("org.opensearch.jdbc.Driver", true, loader)
          .getDeclaredConstructor().newInstance();
      connection = driver.connect(getConnectionString(), null);
    } else {
      connection = DriverManager.getConnection(getConnectionString());
    }
  }

  @AfterAll
  @AfterClass
  @SneakyThrows
  public static void closeConnection() {
    // TODO should we close Statement and ResultSet?
    if (connection != null) {
      connection.close();
      connection = null;
    }
  }

  @Test
  @SneakyThrows
  public void check_driver_version() {
    var version = System.getProperty("jdbcDriverVersion");
    Assume.assumeTrue("Parameter `jdbcDriverVersion` is not given, test platform uses default driver version",
        version != null);
    assertEquals(version, connection.getMetaData().getDriverVersion());
  }

  @Test
  @SneakyThrows
  public void select_all_no_cursor() {
    Statement stmt = connection.createStatement();

    for (var table : List.of(TEST_INDEX_CALCS, TEST_INDEX_ONLINE, TEST_INDEX_BANK, TEST_INDEX_ACCOUNT)) {
      var query = String.format("SELECT * FROM %s", table);
      ResultSet rs = stmt.executeQuery(query);
      int rows = 0;
      for (; rs.next(); rows++) ;

      var restResponse = executeRestQuery(query, null);
      assertEquals(rows, restResponse.getInt("total"));
    }
  }

  @Test
  @SneakyThrows
  public void select_count_all_no_cursor() {
    Statement stmt = connection.createStatement();

    for (var table : List.of(TEST_INDEX_CALCS, TEST_INDEX_ONLINE, TEST_INDEX_BANK, TEST_INDEX_ACCOUNT)) {
      var query = String.format("SELECT COUNT(*) FROM %s", table);
      ResultSet rs = stmt.executeQuery(query);
      int rows = 0;
      for (; rs.next(); rows++) ;

      var restResponse = executeRestQuery(query, null);
      assertEquals(rows, restResponse.getInt("total"));
    }
  }

  @Test
  @SneakyThrows
  public void select_all_small_table_big_cursor() {
    Statement stmt = connection.createStatement();

    for (var table : List.of(TEST_INDEX_CALCS, TEST_INDEX_BANK)) {
      var query = String.format("SELECT COUNT(*) FROM %s", table);
      stmt.setFetchSize(200);
      ResultSet rs = stmt.executeQuery(query);
      int rows = 0;
      for (; rs.next(); rows++) ;

      var restResponse = executeRestQuery(query, null);
      assertEquals(rows, restResponse.getInt("total"));
    }
  }

  @Test
  @SneakyThrows
  public void select_all_small_table_small_cursor() {
    Statement stmt = connection.createStatement();

    for (var table : List.of(TEST_INDEX_CALCS, TEST_INDEX_BANK)) {
      var query = String.format("SELECT * FROM %s", table);
      stmt.setFetchSize(3);
      ResultSet rs = stmt.executeQuery(query);
      int rows = 0;
      for (; rs.next(); rows++) ;

      var restResponse = executeRestQuery(query, null);
      assertEquals(rows, restResponse.getInt("total"));
    }
  }

  @Test
  @SneakyThrows
  public void select_all_big_table_small_cursor() {
    Statement stmt = connection.createStatement();

    for (var table : List.of(TEST_INDEX_ONLINE, TEST_INDEX_ACCOUNT)) {
      var query = String.format("SELECT * FROM %s", table);
      stmt.setFetchSize(10);
      ResultSet rs = stmt.executeQuery(query);
      int rows = 0;
      for (; rs.next(); rows++) ;

      var restResponse = executeRestQuery(query, null);
      assertEquals(rows, restResponse.getInt("total"));
    }
  }

  @Test
  @SneakyThrows
  public void select_all_big_table_big_cursor() {
    Statement stmt = connection.createStatement();

    for (var table : List.of(TEST_INDEX_ONLINE, TEST_INDEX_ACCOUNT)) {
      var query = String.format("SELECT * FROM %s", table);
      stmt.setFetchSize(500);
      ResultSet rs = stmt.executeQuery(query);
      int rows = 0;
      for (; rs.next(); rows++) ;

      var restResponse = executeRestQuery(query, null);
      assertEquals(rows, restResponse.getInt("total"));
    }
  }

  /**
   * Use OpenSearch cluster initialized by OpenSearch Gradle task.
   */
  private static String getConnectionString() {
    // string like "[::1]:46751,127.0.0.1:34403"
    var clusterUrls = System.getProperty("tests.rest.cluster").split(",");
    return String.format("jdbc:opensearch://%s", clusterUrls[clusterUrls.length - 1]);
  }

  @SneakyThrows
  protected JSONObject executeRestQuery(String query, @Nullable Integer fetch_size) {
    Request request = new Request("POST", QUERY_API_ENDPOINT);
    if (fetch_size != null) {
      request.setJsonEntity(String.format("{ \"query\": \"%s\", \"fetch_size\": %d }", query, fetch_size));
    } else {
      request.setJsonEntity(String.format("{ \"query\": \"%s\" }", query));
    }

    RequestOptions.Builder restOptionsBuilder = RequestOptions.DEFAULT.toBuilder();
    restOptionsBuilder.addHeader("Content-Type", "application/json");
    request.setOptions(restOptionsBuilder);

    Response response = client().performRequest(request);
    return new JSONObject(getResponseBody(response));
  }
}
