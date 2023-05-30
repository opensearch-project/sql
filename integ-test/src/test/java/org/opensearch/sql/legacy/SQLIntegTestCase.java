/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.legacy;

import com.google.common.base.Strings;
import com.google.gson.Gson;
import org.apache.commons.lang3.StringUtils;
import org.json.JSONArray;
import org.json.JSONObject;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.opensearch.client.Request;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.Response;
import org.opensearch.client.RestClient;
import org.opensearch.sql.common.setting.Settings;

import javax.management.MBeanServerInvocationHandler;
import javax.management.ObjectName;
import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXServiceURL;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Locale;
import org.opensearch.sql.datasource.model.DataSourceMetadata;

import static com.google.common.base.Strings.isNullOrEmpty;
import static org.opensearch.sql.legacy.TestUtils.createIndexByRestClient;
import static org.opensearch.sql.legacy.TestUtils.getAccountIndexMapping;
import static org.opensearch.sql.legacy.TestUtils.getBankIndexMapping;
import static org.opensearch.sql.legacy.TestUtils.getBankWithNullValuesIndexMapping;
import static org.opensearch.sql.legacy.TestUtils.getDataTypeNonnumericIndexMapping;
import static org.opensearch.sql.legacy.TestUtils.getDataTypeNumericIndexMapping;
import static org.opensearch.sql.legacy.TestUtils.getDateIndexMapping;
import static org.opensearch.sql.legacy.TestUtils.getDateTimeIndexMapping;
import static org.opensearch.sql.legacy.TestUtils.getDeepNestedIndexMapping;
import static org.opensearch.sql.legacy.TestUtils.getDogIndexMapping;
import static org.opensearch.sql.legacy.TestUtils.getDogs2IndexMapping;
import static org.opensearch.sql.legacy.TestUtils.getDogs3IndexMapping;
import static org.opensearch.sql.legacy.TestUtils.getEmployeeNestedTypeIndexMapping;
import static org.opensearch.sql.legacy.TestUtils.getGameOfThronesIndexMapping;
import static org.opensearch.sql.legacy.TestUtils.getJoinTypeIndexMapping;
import static org.opensearch.sql.legacy.TestUtils.getLocationIndexMapping;
import static org.opensearch.sql.legacy.TestUtils.getMappingFile;
import static org.opensearch.sql.legacy.TestUtils.getNestedSimpleIndexMapping;
import static org.opensearch.sql.legacy.TestUtils.getNestedTypeIndexMapping;
import static org.opensearch.sql.legacy.TestUtils.getOdbcIndexMapping;
import static org.opensearch.sql.legacy.TestUtils.getOrderIndexMapping;
import static org.opensearch.sql.legacy.TestUtils.getPeople2IndexMapping;
import static org.opensearch.sql.legacy.TestUtils.getPhraseIndexMapping;
import static org.opensearch.sql.legacy.TestUtils.getResponseBody;
import static org.opensearch.sql.legacy.TestUtils.getStringIndexMapping;
import static org.opensearch.sql.legacy.TestUtils.getUnexpandedObjectIndexMapping;
import static org.opensearch.sql.legacy.TestUtils.getWeblogsIndexMapping;
import static org.opensearch.sql.legacy.TestUtils.isIndexExist;
import static org.opensearch.sql.legacy.TestUtils.loadDataByRestClient;
import static org.opensearch.sql.legacy.plugin.RestSqlAction.CURSOR_CLOSE_ENDPOINT;
import static org.opensearch.sql.legacy.plugin.RestSqlAction.EXPLAIN_API_ENDPOINT;
import static org.opensearch.sql.legacy.plugin.RestSqlAction.QUERY_API_ENDPOINT;

/**
 * OpenSearch Rest integration test base for SQL testing
 */
public abstract class SQLIntegTestCase extends OpenSearchSQLRestTestCase {

  public static final String PERSISTENT = "persistent";
  public static final String TRANSIENT = "transient";
  public static final Integer DEFAULT_QUERY_SIZE_LIMIT =
      Integer.parseInt(System.getProperty("defaultQuerySizeLimit", "200"));
  public static final Integer DEFAULT_MAX_RESULT_WINDOW =
      Integer.parseInt(System.getProperty("defaultMaxResultWindow", "10000"));

  public boolean shouldResetQuerySizeLimit() {
    return true;
  }

  @Before
  public void setUpIndices() throws Exception {
    if (client() == null) {
      initClient();
    }

    if (shouldResetQuerySizeLimit()) {
      resetQuerySizeLimit();
    }
    init();
  }

  @Override
  protected boolean preserveClusterUponCompletion() {
    return true; // Preserve test index, template and settings between test cases
  }

  /**
   * We need to be able to dump the jacoco coverage before cluster is shut down.
   * The new internal testing framework removed some of the gradle tasks we were listening to
   * to choose a good time to do it. This will dump the executionData to file after each test.
   * TODO: This is also currently just overwriting integTest.exec with the updated execData without
   * resetting after writing each time. This can be improved to either write an exec file per test
   * or by letting jacoco append to the file
   */
  public interface IProxy {
    byte[] getExecutionData(boolean reset);

    void dump(boolean reset);

    void reset();
  }

  @AfterClass
  public static void dumpCoverage() {
    // jacoco.dir is set in sqlplugin-coverage.gradle, if it doesn't exist we don't
    // want to collect coverage so we can return early
    String jacocoBuildPath = System.getProperty("jacoco.dir");
    if (Strings.isNullOrEmpty(jacocoBuildPath)) {
      return;
    }

    String serverUrl = "service:jmx:rmi:///jndi/rmi://127.0.0.1:7777/jmxrmi";
    try (JMXConnector connector = JMXConnectorFactory.connect(new JMXServiceURL(serverUrl))) {
      IProxy proxy = MBeanServerInvocationHandler.newProxyInstance(
          connector.getMBeanServerConnection(), new ObjectName("org.jacoco:type=Runtime"),
          IProxy.class,
          false);

      Path path = Paths.get(jacocoBuildPath + "/integTest.exec");
      Files.write(path, proxy.getExecutionData(false));
    } catch (Exception ex) {
      throw new RuntimeException("Failed to dump coverage", ex);
    }
  }

  /**
   * As JUnit JavaDoc says:
   * "The @AfterClass methods declared in superclasses will be run after those of the current class."
   * So this method is supposed to run before closeClients() in parent class.
   */
  @AfterClass
  public static void cleanUpIndices() throws IOException {
    if (System.getProperty("tests.rest.bwcsuite") == null) {
      wipeAllOpenSearchIndices();
      wipeAllClusterSettings();
    }
  }

  protected void setQuerySizeLimit(Integer limit) throws IOException {
    updateClusterSettings(
        new ClusterSetting("transient", Settings.Key.QUERY_SIZE_LIMIT.getKeyValue(), limit.toString()));
  }

  protected void resetQuerySizeLimit() throws IOException {
    updateClusterSettings(
        new ClusterSetting("transient", Settings.Key.QUERY_SIZE_LIMIT.getKeyValue(), DEFAULT_QUERY_SIZE_LIMIT
            .toString()));
  }

  protected static void wipeAllClusterSettings() throws IOException {
    updateClusterSettings(new ClusterSetting("persistent", "*", null));
    updateClusterSettings(new ClusterSetting("transient", "*", null));
    if (remoteClient() != null) {
      updateClusterSettings(new ClusterSetting("persistent", "*", null), remoteClient());
      updateClusterSettings(new ClusterSetting("transient", "*", null), remoteClient());
    }
  }

  protected void setMaxResultWindow(String indexName, Integer window) throws IOException {
    updateIndexSettings(indexName, "{ \"index\": { \"max_result_window\":" + window + " } }");
  }

  protected void resetMaxResultWindow(String indexName) throws IOException {
    updateIndexSettings(indexName,
        "{ \"index\": { \"max_result_window\": " + DEFAULT_MAX_RESULT_WINDOW + " } }");
  }

  /**
   * Provide for each test to load test index, data and other setup work
   */
  protected void init() throws Exception {
  }

  /**
   * Make it thread-safe in case tests are running in parallel but does not guarantee
   * if test like DeleteIT that mutates cluster running in parallel.
   */
  protected synchronized void loadIndex(Index index, RestClient client) throws IOException {
    String indexName = index.getName();
    String mapping = index.getMapping();
    String dataSet = index.getDataSet();

    if (!isIndexExist(client, indexName)) {
      createIndexByRestClient(client, indexName, mapping);
      loadDataByRestClient(client, indexName, dataSet);
    }
  }

  protected synchronized void loadIndex(Index index) throws IOException {
    loadIndex(index, client());
  }

  protected Request getSqlRequest(String request, boolean explain) {
    return getSqlRequest(request, explain, "json");
  }

  protected Request getSqlRequest(String request, boolean explain, String requestType) {
    String queryEndpoint = String.format("%s?format=%s", QUERY_API_ENDPOINT, requestType);
    Request sqlRequest = new Request("POST", explain ? EXPLAIN_API_ENDPOINT : queryEndpoint);
    sqlRequest.setJsonEntity(request);
    RequestOptions.Builder restOptionsBuilder = RequestOptions.DEFAULT.toBuilder();
    restOptionsBuilder.addHeader("Content-Type", "application/json");
    sqlRequest.setOptions(restOptionsBuilder);

    return sqlRequest;
  }

  protected Request getSqlCursorCloseRequest(String cursorRequest) {
    String queryEndpoint = String.format("%s?format=%s", CURSOR_CLOSE_ENDPOINT, "jdbc");
    Request sqlRequest = new Request("POST", queryEndpoint);
    sqlRequest.setJsonEntity(cursorRequest);
    RequestOptions.Builder restOptionsBuilder = RequestOptions.DEFAULT.toBuilder();
    restOptionsBuilder.addHeader("Content-Type", "application/json");
    sqlRequest.setOptions(restOptionsBuilder);

    return sqlRequest;
  }

  protected String executeQuery(String query, String requestType) {
    try {
      String endpoint = "/_plugins/_sql?format=" + requestType;
      String requestBody = makeRequest(query);

      Request sqlRequest = new Request("POST", endpoint);
      sqlRequest.setJsonEntity(requestBody);

      Response response = client().performRequest(sqlRequest);
      Assert.assertEquals(200, response.getStatusLine().getStatusCode());
      String responseString = getResponseBody(response, true);

      return responseString;
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  protected JSONObject executeJdbcRequest(String query) {
    return new JSONObject(executeQuery(query, "jdbc"));
  }

  protected String executeFetchQuery(String query, int fetchSize, String requestType)
      throws IOException {
    String endpoint = "/_plugins/_sql?format=" + requestType;
    String requestBody = makeRequest(query, fetchSize);

    Request sqlRequest = new Request("POST", endpoint);
    sqlRequest.setJsonEntity(requestBody);

    Response response = client().performRequest(sqlRequest);
    String responseString = getResponseBody(response, true);
    return responseString;
  }

  protected JSONObject executeQueryTemplate(String queryTemplate, String index, int fetchSize)
      throws IOException {
    var query = String.format(queryTemplate, index);
    return new JSONObject(executeFetchQuery(query, fetchSize, "jdbc"));
  }

  protected JSONObject executeQueryTemplate(String queryTemplate, String index) throws IOException {
    var query = String.format(queryTemplate, index);
    return executeQueryTemplate(queryTemplate, index, 4);
  }

  protected String executeFetchLessQuery(String query, String requestType) throws IOException {

    String endpoint = "/_plugins/_sql?format=" + requestType;
    String requestBody = makeFetchLessRequest(query);

    Request sqlRequest = new Request("POST", endpoint);
    sqlRequest.setJsonEntity(requestBody);

    Response response = client().performRequest(sqlRequest);
    String responseString = getResponseBody(response, true);
    return responseString;
  }

  protected Request buildGetEndpointRequest(final String sqlQuery) {

    final String utf8CharsetName = StandardCharsets.UTF_8.name();
    String urlEncodedQuery = "";

    try {
      urlEncodedQuery = URLEncoder.encode(sqlQuery, utf8CharsetName);
    } catch (UnsupportedEncodingException e) {
      // Will never reach here since UTF-8 is always supported
      Assert.fail(utf8CharsetName + " not available");
    }

    final String requestUrl = String.format(Locale.ROOT, "%s?sql=%s&format=%s", QUERY_API_ENDPOINT,
        urlEncodedQuery, "json");
    return new Request("GET", requestUrl);
  }

  protected JSONObject executeQuery(final String sqlQuery) throws IOException {

    final String requestBody = makeRequest(sqlQuery);
    return executeRequest(requestBody);
  }

  protected String explainQuery(final String sqlQuery) throws IOException {

    final String requestBody = makeRequest(sqlQuery);
    return executeExplainRequest(requestBody);
  }

  protected String executeQueryWithStringOutput(final String sqlQuery) throws IOException {

    final String requestString = makeRequest(sqlQuery);
    return executeRequest(requestString, false);
  }

  protected JSONObject executeRequest(final String requestBody) throws IOException {

    return new JSONObject(executeRequest(requestBody, false));
  }

  protected String executeExplainRequest(final String requestBody) throws IOException {

    return executeRequest(requestBody, true);
  }

  private String executeRequest(final String requestBody, final boolean isExplainQuery)
      throws IOException {

    Request sqlRequest = getSqlRequest(requestBody, isExplainQuery);
    return executeRequest(sqlRequest);
  }

  protected static String executeRequest(final Request request, RestClient client) throws IOException {
    Response response = client.performRequest(request);
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());
    return getResponseBody(response);
  }

  protected static String executeRequest(final Request request) throws IOException {
    return executeRequest(request, client());
  }

  protected JSONObject executeQueryWithGetRequest(final String sqlQuery) throws IOException {

    final Request request = buildGetEndpointRequest(sqlQuery);
    final String result = executeRequest(request);
    return new JSONObject(result);
  }

  protected JSONObject executeCursorQuery(final String cursor) throws IOException {
    final String requestBody = makeCursorRequest(cursor);
    Request sqlRequest = getSqlRequest(requestBody, false, "jdbc");
    return new JSONObject(executeRequest(sqlRequest));
  }

  protected JSONObject executeCursorCloseQuery(final String cursor) throws IOException {
    final String requestBody = makeCursorRequest(cursor);
    Request sqlRequest = getSqlCursorCloseRequest(requestBody);
    return new JSONObject(executeRequest(sqlRequest));
  }

  protected static JSONObject updateClusterSettings(ClusterSetting setting, RestClient client) throws IOException {
    Request request = new Request("PUT", "/_cluster/settings");
    String persistentSetting = String.format(Locale.ROOT,
        "{\"%s\": {\"%s\": %s}}", setting.type, setting.name, setting.value);
    request.setJsonEntity(persistentSetting);
    RequestOptions.Builder restOptionsBuilder = RequestOptions.DEFAULT.toBuilder();
    restOptionsBuilder.addHeader("Content-Type", "application/json");
    request.setOptions(restOptionsBuilder);
    return new JSONObject(executeRequest(request, client));
  }

  protected static JSONObject updateClusterSettings(ClusterSetting setting) throws IOException {
    return updateClusterSettings(setting, client());
  }

  protected static JSONObject getAllClusterSettings() throws IOException {
    Request request = new Request("GET", "/_cluster/settings?flat_settings&include_defaults");
    RequestOptions.Builder restOptionsBuilder = RequestOptions.DEFAULT.toBuilder();
    restOptionsBuilder.addHeader("Content-Type", "application/json");
    request.setOptions(restOptionsBuilder);
    return new JSONObject(executeRequest(request));
  }

  protected static class ClusterSetting {
    private final String type;
    private final String name;
    private final String value;

    public ClusterSetting(String type, String name, String value) {
      this.type = type;
      this.name = name;
      this.value = (value == null) ? "null" : ("\"" + value + "\"");
    }

    ClusterSetting nullify() {
      return new ClusterSetting(type, name, null);
    }

    @Override
    public String toString() {
      return "ClusterSetting{" +
          "type='" + type + '\'' +
          ", path='" + name + '\'' +
          ", value='" + value + '\'' +
          '}';
    }
  }

  protected static JSONObject updateIndexSettings(String indexName, String setting)
      throws IOException {
    Request request = new Request("PUT", "/" + indexName + "/_settings");
    if (!isNullOrEmpty(setting)) {
      request.setJsonEntity(setting);
    }
    RequestOptions.Builder restOptionsBuilder = RequestOptions.DEFAULT.toBuilder();
    restOptionsBuilder.addHeader("Content-Type", "application/json");
    request.setOptions(restOptionsBuilder);
    return new JSONObject(executeRequest(request));
  }

  protected String makeRequest(String query) {
    return makeRequest(query, 0);
  }

  protected String makeRequest(String query, int fetch_size) {
    return String.format("{\n" +
        "  \"fetch_size\": \"%s\",\n" +
        "  \"query\": \"%s\"\n" +
        "}", fetch_size, query);
  }

  protected String makeFetchLessRequest(String query) {
    return String.format("{\n" +
        "  \"query\": \"%s\"\n" +
        "}", query);
  }

  protected String makeCursorRequest(String cursor) {
    return String.format("{\"cursor\":\"%s\"}", cursor);
  }

  protected JSONArray getHits(JSONObject response) {
    Assert.assertTrue(response.getJSONObject("hits").has("hits"));

    return response.getJSONObject("hits").getJSONArray("hits");
  }

  protected int getTotalHits(JSONObject response) {
    Assert.assertTrue(response.getJSONObject("hits").has("total"));
    Assert.assertTrue(response.getJSONObject("hits").getJSONObject("total").has("value"));

    return response.getJSONObject("hits").getJSONObject("total").getInt("value");
  }

  protected JSONObject getSource(JSONObject hit) {
    return hit.getJSONObject("_source");
  }

  protected static Request getCreateDataSourceRequest(DataSourceMetadata dataSourceMetadata) {
    Request request = new Request("POST", "/_plugins/_query/_datasources");
    request.setJsonEntity(new Gson().toJson(dataSourceMetadata));
    RequestOptions.Builder restOptionsBuilder = RequestOptions.DEFAULT.toBuilder();
    restOptionsBuilder.addHeader("Content-Type", "application/json");
    request.setOptions(restOptionsBuilder);
    return request;
  }

  protected static Request getUpdateDataSourceRequest(DataSourceMetadata dataSourceMetadata) {
    Request request = new Request("PUT", "/_plugins/_query/_datasources");
    request.setJsonEntity(new Gson().toJson(dataSourceMetadata));
    RequestOptions.Builder restOptionsBuilder = RequestOptions.DEFAULT.toBuilder();
    restOptionsBuilder.addHeader("Content-Type", "application/json");
    request.setOptions(restOptionsBuilder);
    return request;
  }

  protected static Request getFetchDataSourceRequest(String name) {
    Request request = new Request("GET", "/_plugins/_query/_datasources" + "/" + name);
    if (StringUtils.isEmpty(name)) {
      request = new Request("GET", "/_plugins/_query/_datasources");
    }
    RequestOptions.Builder restOptionsBuilder = RequestOptions.DEFAULT.toBuilder();
    restOptionsBuilder.addHeader("Content-Type", "application/json");
    request.setOptions(restOptionsBuilder);
    return request;
  }


  protected static Request getDeleteDataSourceRequest(String name) {
    Request request = new Request("DELETE", "/_plugins/_query/_datasources" + "/" + name);
    RequestOptions.Builder restOptionsBuilder = RequestOptions.DEFAULT.toBuilder();
    restOptionsBuilder.addHeader("Content-Type", "application/json");
    request.setOptions(restOptionsBuilder);
    return request;
  }

  /**
   * Enum for associating test index with relevant mapping and data.
   */
  public enum Index {
    ONLINE(TestsConstants.TEST_INDEX_ONLINE,
        "online",
        null,
        "src/test/resources/online.json"),
    ACCOUNT(TestsConstants.TEST_INDEX_ACCOUNT,
        "account",
        getAccountIndexMapping(),
        "src/test/resources/accounts.json"),
    PHRASE(TestsConstants.TEST_INDEX_PHRASE,
        "phrase",
        getPhraseIndexMapping(),
        "src/test/resources/phrases.json"),
    DOG(TestsConstants.TEST_INDEX_DOG,
        "dog",
        getDogIndexMapping(),
        "src/test/resources/dogs.json"),
    DOGS2(TestsConstants.TEST_INDEX_DOG2,
        "dog",
        getDogs2IndexMapping(),
        "src/test/resources/dogs2.json"),
    DOGS3(TestsConstants.TEST_INDEX_DOG3,
        "dog",
        getDogs3IndexMapping(),
        "src/test/resources/dogs3.json"),
    DOGSSUBQUERY(TestsConstants.TEST_INDEX_DOGSUBQUERY,
        "dog",
        getDogIndexMapping(),
        "src/test/resources/dogsubquery.json"),
    PEOPLE(TestsConstants.TEST_INDEX_PEOPLE,
        "people",
        null,
        "src/test/resources/peoples.json"),
    PEOPLE2(TestsConstants.TEST_INDEX_PEOPLE2,
        "people",
        getPeople2IndexMapping(),
        "src/test/resources/people2.json"),
    GAME_OF_THRONES(TestsConstants.TEST_INDEX_GAME_OF_THRONES,
        "gotCharacters",
        getGameOfThronesIndexMapping(),
        "src/test/resources/game_of_thrones_complex.json"),
    SYSTEM(TestsConstants.TEST_INDEX_SYSTEM,
        "systems",
        null,
        "src/test/resources/systems.json"),
    ODBC(TestsConstants.TEST_INDEX_ODBC,
        "odbc",
        getOdbcIndexMapping(),
        "src/test/resources/odbc-date-formats.json"),
    LOCATION(TestsConstants.TEST_INDEX_LOCATION,
        "location",
        getLocationIndexMapping(),
        "src/test/resources/locations.json"),
    LOCATION_TWO(TestsConstants.TEST_INDEX_LOCATION2,
        "location2",
        getLocationIndexMapping(),
        "src/test/resources/locations2.json"),
    NESTED(TestsConstants.TEST_INDEX_NESTED_TYPE,
        "nestedType",
        getNestedTypeIndexMapping(),
        "src/test/resources/nested_objects.json"),
    NESTED_WITHOUT_ARRAYS(TestsConstants.TEST_INDEX_NESTED_TYPE_WITHOUT_ARRAYS,
        "nestedTypeWithoutArrays",
        getNestedTypeIndexMapping(),
        "src/test/resources/nested_objects_without_arrays.json"),
    NESTED_WITH_QUOTES(TestsConstants.TEST_INDEX_NESTED_WITH_QUOTES,
        "nestedType",
        getNestedTypeIndexMapping(),
        "src/test/resources/nested_objects_quotes_in_values.json"),
    EMPLOYEE_NESTED(TestsConstants.TEST_INDEX_EMPLOYEE_NESTED,
        "_doc",
        getEmployeeNestedTypeIndexMapping(),
        "src/test/resources/employee_nested.json"),
    JOIN(TestsConstants.TEST_INDEX_JOIN_TYPE,
        "joinType",
        getJoinTypeIndexMapping(),
        "src/test/resources/join_objects.json"),
    UNEXPANDED_OBJECT(TestsConstants.TEST_INDEX_UNEXPANDED_OBJECT,
        "unexpandedObject",
        getUnexpandedObjectIndexMapping(),
        "src/test/resources/unexpanded_objects.json"),
    BANK(TestsConstants.TEST_INDEX_BANK,
        "account",
        getBankIndexMapping(),
        "src/test/resources/bank.json"),
    BANK_TWO(TestsConstants.TEST_INDEX_BANK_TWO,
        "account_two",
        getBankIndexMapping(),
        "src/test/resources/bank_two.json"),
    BANK_WITH_NULL_VALUES(TestsConstants.TEST_INDEX_BANK_WITH_NULL_VALUES,
        "account_null",
        getBankWithNullValuesIndexMapping(),
        "src/test/resources/bank_with_null_values.json"),
    BANK_WITH_STRING_VALUES(TestsConstants.TEST_INDEX_STRINGS,
        "strings",
        getStringIndexMapping(),
        "src/test/resources/strings.json"),
    BANK_CSV_SANITIZE(TestsConstants.TEST_INDEX_BANK_CSV_SANITIZE,
        "account",
        getBankIndexMapping(),
        "src/test/resources/bank_csv_sanitize.json"),
    BANK_RAW_SANITIZE(TestsConstants.TEST_INDEX_BANK_RAW_SANITIZE,
            "account",
            getBankIndexMapping(),
            "src/test/resources/bank_raw_sanitize.json"),
    ORDER(TestsConstants.TEST_INDEX_ORDER,
        "_doc",
        getOrderIndexMapping(),
        "src/test/resources/order.json"),
    WEBLOG(TestsConstants.TEST_INDEX_WEBLOG,
        "weblog",
        getWeblogsIndexMapping(),
        "src/test/resources/weblogs.json"),
    DATE(TestsConstants.TEST_INDEX_DATE,
        "dates",
        getDateIndexMapping(),
        "src/test/resources/dates.json"),
    DATETIME(TestsConstants.TEST_INDEX_DATE_TIME,
        "_doc",
        getDateTimeIndexMapping(),
        "src/test/resources/datetime.json"),
    NESTED_SIMPLE(TestsConstants.TEST_INDEX_NESTED_SIMPLE,
        "_doc",
        getNestedSimpleIndexMapping(),
        "src/test/resources/nested_simple.json"),
    DEEP_NESTED(TestsConstants.TEST_INDEX_DEEP_NESTED,
        "_doc",
        getDeepNestedIndexMapping(),
        "src/test/resources/deep_nested_index_data.json"),
    DATA_TYPE_NUMERIC(TestsConstants.TEST_INDEX_DATATYPE_NUMERIC,
        "_doc",
        getDataTypeNumericIndexMapping(),
        "src/test/resources/datatypes_numeric.json"),
    DATA_TYPE_NONNUMERIC(TestsConstants.TEST_INDEX_DATATYPE_NONNUMERIC,
        "_doc",
        getDataTypeNonnumericIndexMapping(),
        "src/test/resources/datatypes.json"),
    BEER(TestsConstants.TEST_INDEX_BEER,
        "beer",
        null,
        "src/test/resources/beer.stackexchange.json"),
    NULL_MISSING(TestsConstants.TEST_INDEX_NULL_MISSING,
        "null_missing",
        getMappingFile("null_missing_index_mapping.json"),
        "src/test/resources/null_missing.json"),
    CALCS(TestsConstants.TEST_INDEX_CALCS,
        "calcs",
        getMappingFile("calcs_index_mappings.json"),
        "src/test/resources/calcs.json"),
    DATE_FORMATS(TestsConstants.TEST_INDEX_DATE_FORMATS,
        "date_formats",
        getMappingFile("date_formats_index_mapping.json"),
        "src/test/resources/date_formats.json"),
    WILDCARD(TestsConstants.TEST_INDEX_WILDCARD,
        "wildcard",
        getMappingFile("wildcard_index_mappings.json"),
        "src/test/resources/wildcard.json"),
    DATASOURCES(TestsConstants.DATASOURCES,
        "datasource",
        getMappingFile("datasources_index_mappings.json"),
        "src/test/resources/datasources.json"),
    MULTI_NESTED(TestsConstants.TEST_INDEX_MULTI_NESTED_TYPE,
        "multi_nested",
        getMappingFile("multi_nested.json"),
        "src/test/resources/multi_nested_objects.json"),
    NESTED_WITH_NULLS(TestsConstants.TEST_INDEX_NESTED_WITH_NULLS,
        "multi_nested",
        getNestedTypeIndexMapping(),
        "src/test/resources/nested_with_nulls.json");

    private final String name;
    private final String type;
    private final String mapping;
    private final String dataSet;

    Index(String name, String type, String mapping, String dataSet) {
      this.name = name;
      this.type = type;
      this.mapping = mapping;
      this.dataSet = dataSet;
    }

    public String getName() {
      return this.name;
    }

    public String getType() {
      return this.type;
    }

    public String getMapping() {
      return this.mapping;
    }

    public String getDataSet() {
      return this.dataSet;
    }


  }
}
