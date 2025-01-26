/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.legacy;

import static com.google.common.base.Strings.isNullOrEmpty;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Locale;
import java.util.stream.Collectors;
import org.json.JSONObject;
import org.opensearch.action.bulk.BulkRequest;
import org.opensearch.action.bulk.BulkResponse;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.client.Client;
import org.opensearch.client.Request;
import org.opensearch.client.Response;
import org.opensearch.client.RestClient;
import org.opensearch.common.xcontent.XContentType;

public class TestUtils {

  private static final String MAPPING_FILE_PATH = "src/test/resources/indexDefinitions/";

  /**
   * Create test index by REST client.
   *
   * @param client client connection
   * @param indexName test index name
   * @param mapping test index mapping or null if no predefined mapping
   */
  public static void createIndexByRestClient(RestClient client, String indexName, String mapping) {
    Request request = new Request("PUT", "/" + indexName);
    if (!isNullOrEmpty(mapping)) {
      request.setJsonEntity(mapping);
    }
    performRequest(client, request);
  }

  /**
   * https://github.com/elastic/elasticsearch/pull/49959<br>
   * Deprecate creation of dot-prefixed index names except for hidden and system indices. Create
   * hidden index by REST client.
   *
   * @param client client connection
   * @param indexName test index name
   * @param mapping test index mapping or null if no predefined mapping
   */
  public static void createHiddenIndexByRestClient(
      RestClient client, String indexName, String mapping) {
    Request request = new Request("PUT", "/" + indexName);
    JSONObject jsonObject = isNullOrEmpty(mapping) ? new JSONObject() : new JSONObject(mapping);
    jsonObject.put("settings", new JSONObject("{\"index\":{\"hidden\":true}}"));
    request.setJsonEntity(jsonObject.toString());

    performRequest(client, request);
  }

  /**
   * Check if index already exists by OpenSearch index exists API which returns: 200 - specified
   * indices or aliases exist 404 - one or more indices specified or aliases do not exist
   *
   * @param client client connection
   * @param indexName index name
   * @return true for index exist
   */
  public static boolean isIndexExist(RestClient client, String indexName) {
    try {
      Response response = client.performRequest(new Request("HEAD", "/" + indexName));
      return (response.getStatusLine().getStatusCode() == 200);
    } catch (IOException e) {
      throw new IllegalStateException("Failed to perform request", e);
    }
  }

  /**
   * Load test data set by REST client.
   *
   * @param client client connection
   * @param indexName index name
   * @param dataSetFilePath file path of test data set
   * @throws IOException
   */
  public static void loadDataByRestClient(
      RestClient client, String indexName, String dataSetFilePath) throws IOException {
    Path path = Paths.get(getResourceFilePath(dataSetFilePath));
    Request request = new Request("POST", "/" + indexName + "/_bulk?refresh=true");
    request.setJsonEntity(new String(Files.readAllBytes(path)));
    performRequest(client, request);
  }

  /**
   * Perform a request by REST client.
   *
   * @param client client connection
   * @param request request object
   */
  public static Response performRequest(RestClient client, Request request) {
    try {
      Response response = client.performRequest(request);
      int status = response.getStatusLine().getStatusCode();
      if (status >= 400) {
        throw new IllegalStateException("Failed to perform request. Error code: " + status);
      }
      return response;
    } catch (IOException e) {
      throw new IllegalStateException("Failed to perform request", e);
    }
  }

  public static String getAccountIndexMapping() {
    String mappingFile = "account_index_mapping.json";
    return getMappingFile(mappingFile);
  }

  public static String getPhraseIndexMapping() {
    String mappingFile = "phrase_index_mapping.json";
    return getMappingFile(mappingFile);
  }

  public static String getDogIndexMapping() {
    String mappingFile = "dog_index_mapping.json";
    return getMappingFile(mappingFile);
  }

  public static String getDogs2IndexMapping() {
    String mappingFile = "dog2_index_mapping.json";
    return getMappingFile(mappingFile);
  }

  public static String getDogs3IndexMapping() {
    String mappingFile = "dog3_index_mapping.json";
    return getMappingFile(mappingFile);
  }

  public static String getPeople2IndexMapping() {
    String mappingFile = "people2_index_mapping.json";
    return getMappingFile(mappingFile);
  }

  public static String getGameOfThronesIndexMapping() {
    String mappingFile = "game_of_thrones_index_mapping.json";
    return getMappingFile(mappingFile);
  }

  // System

  public static String getOdbcIndexMapping() {
    String mappingFile = "odbc_index_mapping.json";
    return getMappingFile(mappingFile);
  }

  public static String getLocationIndexMapping() {
    String mappingFile = "location_index_mapping.json";
    return getMappingFile(mappingFile);
  }

  public static String getEmployeeNestedTypeIndexMapping() {
    String mappingFile = "employee_nested_type_index_mapping.json";
    return getMappingFile(mappingFile);
  }

  public static String getNestedTypeIndexMapping() {
    String mappingFile = "nested_type_index_mapping.json";
    return getMappingFile(mappingFile);
  }

  public static String getJoinTypeIndexMapping() {
    String mappingFile = "join_type_index_mapping.json";
    return getMappingFile(mappingFile);
  }

  public static String getUnexpandedObjectIndexMapping() {
    String mappingFile = "unexpanded_object_index_mapping.json";
    return getMappingFile(mappingFile);
  }

  public static String getBankIndexMapping() {
    String mappingFile = "bank_index_mapping.json";
    return getMappingFile(mappingFile);
  }

  public static String getBankWithNullValuesIndexMapping() {
    String mappingFile = "bank_with_null_values_index_mapping.json";
    return getMappingFile(mappingFile);
  }

  public static String getStringIndexMapping() {
    String mappingFile = "string_index_mapping.json";
    return getMappingFile(mappingFile);
  }

  public static String getOrderIndexMapping() {
    String mappingFile = "order_index_mapping.json";
    return getMappingFile(mappingFile);
  }

  public static String getWeblogsIndexMapping() {
    String mappingFile = "weblogs_index_mapping.json";
    return getMappingFile(mappingFile);
  }

  public static String getDateIndexMapping() {
    String mappingFile = "date_index_mapping.json";
    return getMappingFile(mappingFile);
  }

  public static String getDateTimeIndexMapping() {
    String mappingFile = "date_time_index_mapping.json";
    return getMappingFile(mappingFile);
  }

  public static String getNestedSimpleIndexMapping() {
    String mappingFile = "nested_simple_index_mapping.json";
    return getMappingFile(mappingFile);
  }

  public static String getDeepNestedIndexMapping() {
    String mappingFile = "deep_nested_index_mapping.json";
    return getMappingFile(mappingFile);
  }

  public static String getDataTypeNumericIndexMapping() {
    String mappingFile = "datatypes_numeric_index_mapping.json";
    return getMappingFile(mappingFile);
  }

  public static String getDataTypeNonnumericIndexMapping() {
    String mappingFile = "datatypes_index_mapping.json";
    return getMappingFile(mappingFile);
  }

  public static String getGeopointIndexMapping() {
    String mappingFile = "geopoint_index_mapping.json";
    return getMappingFile(mappingFile);
  }

  public static String getJsonTestIndexMapping() {
    String mappingFile = "json_test_index_mapping.json";
    return getMappingFile(mappingFile);
  }

  public static String getAliasIndexMapping() {
    String mappingFile = "alias_index_mapping.json";
    return getMappingFile(mappingFile);
  }

  public static void loadBulk(Client client, String jsonPath, String defaultIndex)
      throws Exception {
    System.out.println(String.format("Loading file %s into opensearch cluster", jsonPath));
    String absJsonPath = getResourceFilePath(jsonPath);

    BulkRequest bulkRequest = new BulkRequest();
    try (final InputStream stream = new FileInputStream(absJsonPath);
        final Reader streamReader = new InputStreamReader(stream, StandardCharsets.UTF_8);
        final BufferedReader br = new BufferedReader(streamReader)) {

      while (true) {

        String actionLine = br.readLine();
        if (actionLine == null || actionLine.trim().isEmpty()) {
          break;
        }
        String sourceLine = br.readLine();
        JSONObject actionJson = new JSONObject(actionLine);

        IndexRequest indexRequest = new IndexRequest();
        indexRequest.index(defaultIndex);
        if (actionJson.getJSONObject("index").has("_id")) {
          String docId = actionJson.getJSONObject("index").getString("_id");
          indexRequest.id(docId);
        }
        if (actionJson.getJSONObject("index").has("_routing")) {
          String routing = actionJson.getJSONObject("index").getString("_routing");
          indexRequest.routing(routing);
        }
        indexRequest.source(sourceLine, XContentType.JSON);
        bulkRequest.add(indexRequest);
      }
    }

    BulkResponse bulkResponse = client.bulk(bulkRequest).actionGet();

    if (bulkResponse.hasFailures()) {
      throw new Exception(
          "Failed to load test data into index "
              + defaultIndex
              + ", "
              + bulkResponse.buildFailureMessage());
    }
    System.out.println(bulkResponse.getItems().length + " documents loaded.");
    // ensure the documents are searchable
    client.admin().indices().prepareRefresh(defaultIndex).execute().actionGet();
  }

  public static String getResourceFilePath(String relPath) {
    String projectRoot = System.getProperty("project.root", null);
    if (projectRoot == null) {
      return new File(relPath).getAbsolutePath();
    } else {
      return new File(projectRoot + "/" + relPath).getAbsolutePath();
    }
  }

  public static String getResponseBody(Response response) throws IOException {

    return getResponseBody(response, false);
  }

  public static String getResponseBody(Response response, boolean retainNewLines)
      throws IOException {
    final StringBuilder sb = new StringBuilder();

    try (final InputStream is = response.getEntity().getContent();
        final BufferedReader br =
            new BufferedReader(new InputStreamReader(is, StandardCharsets.UTF_8))) {

      String line;
      while ((line = br.readLine()) != null) {
        sb.append(line);
        if (retainNewLines) {
          sb.append(String.format(Locale.ROOT, "%n"));
        }
      }
    }
    return sb.toString();
  }

  public static String fileToString(
      final String filePathFromProjectRoot, final boolean removeNewLines) throws IOException {

    final String absolutePath = getResourceFilePath(filePathFromProjectRoot);

    try (final InputStream stream = new FileInputStream(absolutePath);
        final Reader streamReader = new InputStreamReader(stream, StandardCharsets.UTF_8);
        final BufferedReader br = new BufferedReader(streamReader)) {

      final StringBuilder stringBuilder = new StringBuilder();
      String line = br.readLine();

      while (line != null) {

        stringBuilder.append(line);
        if (!removeNewLines) {
          stringBuilder.append(String.format(Locale.ROOT, "%n"));
        }
        line = br.readLine();
      }

      return stringBuilder.toString();
    }
  }

  /**
   * Builds all permutations of the given list of Strings
   *
   * @param items list of strings to permute
   * @return list of permutations
   */
  public static List<List<String>> getPermutations(final List<String> items) {

    if (items.size() > 5) {
      throw new IllegalArgumentException("Inefficient test, please refactor");
    }

    final List<List<String>> result = new LinkedList<>();

    if (items.isEmpty() || 1 == items.size()) {

      final List<String> onlyElement = new ArrayList<>();
      if (1 == items.size()) {
        onlyElement.add(items.get(0));
      }
      result.add(onlyElement);
      return result;
    }

    for (int i = 0; i < items.size(); ++i) {

      final List<String> smallerSet = new ArrayList<>();

      if (i != 0) {
        smallerSet.addAll(items.subList(0, i));
      }
      if (i != items.size() - 1) {
        smallerSet.addAll(items.subList(i + 1, items.size()));
      }

      final String currentItem = items.get(i);
      result.addAll(
          getPermutations(smallerSet).stream()
              .map(
                  smallerSetPermutation -> {
                    final List<String> permutation = new ArrayList<>();
                    permutation.add(currentItem);
                    permutation.addAll(smallerSetPermutation);
                    return permutation;
                  })
              .collect(Collectors.toCollection(LinkedList::new)));
    }

    return result;
  }

  public static String getMappingFile(String fileName) {
    try {
      return fileToString(MAPPING_FILE_PATH + fileName, false);
    } catch (IOException e) {
      return null;
    }
  }
}
