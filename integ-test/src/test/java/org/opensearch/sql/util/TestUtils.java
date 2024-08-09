/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.util;

import static com.google.common.base.Strings.isNullOrEmpty;
import static org.junit.Assert.*;
import static org.opensearch.sql.executor.pagination.PlanSerializer.CURSOR_PREFIX;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.stream.Collectors;
import org.json.JSONArray;
import org.json.JSONObject;
import org.junit.Assert;
import org.opensearch.action.bulk.BulkRequest;
import org.opensearch.action.bulk.BulkResponse;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.client.Client;
import org.opensearch.client.Request;
import org.opensearch.client.Response;
import org.opensearch.client.ResponseException;
import org.opensearch.client.RestClient;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.sql.legacy.cursor.CursorType;

public class TestUtils {

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
   * Check if index already exists by OpenSearch index exists API which returns:<br>
   * 200 - specified indices or aliases exist<br>
   * 404 - one or more indices specified or aliases do not exist
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
      if (isRefreshPolicyError(e)) {
        try {
          return retryWithoutRefreshPolicy(request, client);
        } catch (IOException ex) {
          throw new IllegalStateException("Failed to perform request without refresh policy.", ex);
        }
      }
      throw new IllegalStateException("Failed to perform request", e);
    }
  }

  /**
   * Checks if the IOException is due to an unsupported refresh policy.
   *
   * @param e The IOException to check.
   * @return true if the exception is due to a refresh policy error, false otherwise.
   */
  private static boolean isRefreshPolicyError(IOException e) {
    return e instanceof ResponseException
        && ((ResponseException) e).getResponse().getStatusLine().getStatusCode() == 400
        && e.getMessage().contains("true refresh policy is not supported.");
  }

  /**
   * Attempts to perform the request without the refresh policy.
   *
   * @param request The original request.
   * @param client client connection
   * @return The response after retrying the request.
   * @throws IOException If the request fails.
   */
  private static Response retryWithoutRefreshPolicy(Request request, RestClient client)
      throws IOException {
    Request req =
        new Request(request.getMethod(), request.getEndpoint().replaceAll("refresh=true", ""));
    req.setEntity(request.getEntity());
    return client.performRequest(req);
  }

  /**
   * Compares two multiline strings representing rows of addresses to ensure they are equivalent.
   * This method checks if the entire content of the expected and actual strings are the same. If
   * they differ, it breaks down the strings into lines and performs a step-by-step comparison:
   *
   * @param expected The expected string representing rows of data.
   * @param actual The actual string to compare against the expected.
   */
  public static void assertRowsEqual(String expected, String actual) {
    if (expected.equals(actual)) {
      return;
    }

    List<String> expectedLines = List.of(expected.split("\n"));
    List<String> actualLines = List.of(actual.split("\n"));

    if (expectedLines.size() != actualLines.size()) {
      Assert.fail("Line count is different. expected=" + expected + ", actual=" + actual);
    }

    if (!expectedLines.get(0).equals(actualLines.get(0))) {
      Assert.fail("Header is different. expected=" + expected + ", actual=" + actual);
    }

    Set<String> expectedItems = new HashSet<>(expectedLines.subList(1, expectedLines.size()));
    Set<String> actualItems = new HashSet<>(actualLines.subList(1, actualLines.size()));

    assertEquals(expectedItems, actualItems);
  }

  public static String getAccountIndexMapping() {
    return "{  \"mappings\": {"
        + " \"properties\": {\n"
        + "          \"gender\": {\n"
        + "            \"type\": \"text\",\n"
        + "            \"fielddata\": true\n"
        + "          },"
        + "          \"address\": {\n"
        + "            \"type\": \"text\",\n"
        + "            \"fielddata\": true\n"
        + "          },"
        + "          \"firstname\": {\n"
        + "            \"type\": \"text\",\n"
        + "            \"fielddata\": true,\n"
        + "            \"fields\": {\n"
        + "              \"keyword\": {\n"
        + "                \"type\": \"keyword\",\n"
        + "                \"ignore_above\": 256\n"
        + "              }"
        + "            }"
        + "          },"
        + "          \"lastname\": {\n"
        + "            \"type\": \"text\",\n"
        + "            \"fielddata\": true,\n"
        + "            \"fields\": {\n"
        + "              \"keyword\": {\n"
        + "                \"type\": \"keyword\",\n"
        + "                \"ignore_above\": 256\n"
        + "              }"
        + "            }"
        + "          },"
        + "          \"state\": {\n"
        + "            \"type\": \"text\",\n"
        + "            \"fielddata\": true,\n"
        + "            \"fields\": {\n"
        + "              \"keyword\": {\n"
        + "                \"type\": \"keyword\",\n"
        + "                \"ignore_above\": 256\n"
        + "              }"
        + "            }"
        + "          }"
        + "       }"
        + "   }"
        + "}";
  }

  public static String getPhraseIndexMapping() {
    return "{  \"mappings\": {"
        + " \"properties\": {\n"
        + "          \"phrase\": {\n"
        + "            \"type\": \"text\",\n"
        + "            \"store\": true\n"
        + "          }"
        + "       }"
        + "   }"
        + "}";
  }

  public static String getDogIndexMapping() {
    return "{  \"mappings\": {"
        + " \"properties\": {\n"
        + "          \"dog_name\": {\n"
        + "            \"type\": \"text\",\n"
        + "            \"fielddata\": true\n"
        + "          }"
        + "       }"
        + "   }"
        + "}";
  }

  public static String getDogs2IndexMapping() {
    return "{  \"mappings\": {"
        + " \"properties\": {\n"
        + "          \"dog_name\": {\n"
        + "            \"type\": \"text\",\n"
        + "            \"fielddata\": true\n"
        + "          },\n"
        + "          \"holdersName\": {\n"
        + "            \"type\": \"keyword\"\n"
        + "          }"
        + "       }"
        + "   }"
        + "}";
  }

  public static String getDogs3IndexMapping() {
    return "{  \"mappings\": {"
        + " \"properties\": {\n"
        + "          \"holdersName\": {\n"
        + "            \"type\": \"keyword\"\n"
        + "          },\n"
        + "          \"color\": {\n"
        + "            \"type\": \"text\"\n"
        + "          }"
        + "       }"
        + "   }"
        + "}";
  }

  public static String getPeople2IndexMapping() {
    return "{  \"mappings\": {"
        + " \"properties\": {\n"
        + "          \"firstname\": {\n"
        + "            \"type\": \"keyword\"\n"
        + "          }"
        + "       }"
        + "   }"
        + "}";
  }

  public static String getGameOfThronesIndexMapping() {
    return "{  \"mappings\": { "
        + "    \"properties\": {\n"
        + "      \"nickname\": {\n"
        + "        \"type\":\"text\", "
        + "        \"fielddata\":true"
        + "      },\n"
        + "      \"name\": {\n"
        + "        \"properties\": {\n"
        + "          \"firstname\": {\n"
        + "            \"type\": \"text\",\n"
        + "            \"fielddata\": true\n"
        + "          },\n"
        + "          \"lastname\": {\n"
        + "            \"type\": \"text\",\n"
        + "            \"fielddata\": true\n"
        + "          },\n"
        + "          \"ofHerName\": {\n"
        + "            \"type\": \"integer\"\n"
        + "          },\n"
        + "          \"ofHisName\": {\n"
        + "            \"type\": \"integer\"\n"
        + "          }\n"
        + "        }\n"
        + "      },\n"
        + "      \"house\": {\n"
        + "        \"type\": \"text\",\n"
        + "        \"fields\": {\n"
        + "          \"keyword\": {\n"
        + "            \"type\": \"keyword\"\n"
        + "          }\n"
        + "        }\n"
        + "      },\n"
        + "      \"gender\": {\n"
        + "        \"type\": \"text\",\n"
        + "        \"fields\": {\n"
        + "          \"keyword\": {\n"
        + "            \"type\": \"keyword\"\n"
        + "          }\n"
        + "        }\n"
        + "      }"
        + "} } }";
  }

  // System

  public static String getOdbcIndexMapping() {
    return "{\n"
        + "\t\"mappings\" :{\n"
        + "\t\t\"properties\":{\n"
        + "\t\t\t\"odbc_time\":{\n"
        + "\t\t\t\t\"type\":\"date\",\n"
        + "\t\t\t\t\"format\": \"'{ts' ''yyyy-MM-dd HH:mm:ss.SSS'''}'\"\n"
        + "\t\t\t},\n"
        + "\t\t\t\"docCount\":{\n"
        + "\t\t\t\t\"type\":\"text\"\n"
        + "\t\t\t}\n"
        + "\t\t}\n"
        + "\t}\n"
        + "}";
  }

  public static String getLocationIndexMapping() {
    return "{\n"
        + "\t\"mappings\" :{\n"
        + "\t\t\"properties\":{\n"
        + "\t\t\t\"place\":{\n"
        + "\t\t\t\t\"type\":\"geo_shape\"\n"
        +
        // "\t\t\t\t\"tree\": \"quadtree\",\n" + // Field tree and precision are deprecated in
        // OpenSearch
        // "\t\t\t\t\"precision\": \"10km\"\n" +
        "\t\t\t},\n"
        + "\t\t\t\"center\":{\n"
        + "\t\t\t\t\"type\":\"geo_point\"\n"
        + "\t\t\t},\n"
        + "\t\t\t\"description\":{\n"
        + "\t\t\t\t\"type\":\"text\"\n"
        + "\t\t\t}\n"
        + "\t\t}\n"
        + "\t}\n"
        + "}";
  }

  public static String getEmployeeNestedTypeIndexMapping() {
    return "{\n"
        + "  \"mappings\": {\n"
        + "    \"properties\": {\n"
        + "      \"comments\": {\n"
        + "        \"type\": \"nested\",\n"
        + "        \"properties\": {\n"
        + "          \"date\": {\n"
        + "            \"type\": \"date\"\n"
        + "          },\n"
        + "          \"likes\": {\n"
        + "            \"type\": \"long\"\n"
        + "          },\n"
        + "          \"message\": {\n"
        + "            \"type\": \"text\",\n"
        + "            \"fields\": {\n"
        + "              \"keyword\": {\n"
        + "                \"type\": \"keyword\",\n"
        + "                \"ignore_above\": 256\n"
        + "              }\n"
        + "            }\n"
        + "          }\n"
        + "        }\n"
        + "      },\n"
        + "      \"id\": {\n"
        + "        \"type\": \"long\"\n"
        + "      },\n"
        + "      \"name\": {\n"
        + "        \"type\": \"text\",\n"
        + "        \"fields\": {\n"
        + "          \"keyword\": {\n"
        + "            \"type\": \"keyword\",\n"
        + "            \"ignore_above\": 256\n"
        + "          }\n"
        + "        }\n"
        + "      },\n"
        + "      \"projects\": {\n"
        + "        \"type\": \"nested\",\n"
        + "        \"properties\": {\n"
        + "          \"address\": {\n"
        + "            \"type\": \"nested\",\n"
        + "            \"properties\": {\n"
        + "              \"city\": {\n"
        + "                \"type\": \"text\",\n"
        + "                \"fields\": {\n"
        + "                  \"keyword\": {\n"
        + "                    \"type\": \"keyword\",\n"
        + "                    \"ignore_above\": 256\n"
        + "                  }\n"
        + "                }\n"
        + "              },\n"
        + "              \"state\": {\n"
        + "                \"type\": \"text\",\n"
        + "                \"fields\": {\n"
        + "                  \"keyword\": {\n"
        + "                    \"type\": \"keyword\",\n"
        + "                    \"ignore_above\": 256\n"
        + "                  }\n"
        + "                }\n"
        + "              }\n"
        + "            }\n"
        + "          },\n"
        + "          \"name\": {\n"
        + "            \"type\": \"text\",\n"
        + "            \"fields\": {\n"
        + "              \"keyword\": {\n"
        + "                \"type\": \"keyword\"\n"
        + "              }\n"
        + "            },\n"
        + "            \"fielddata\": true\n"
        + "          },\n"
        + "          \"started_year\": {\n"
        + "            \"type\": \"long\"\n"
        + "          }\n"
        + "        }\n"
        + "      },\n"
        + "      \"title\": {\n"
        + "        \"type\": \"text\",\n"
        + "        \"fields\": {\n"
        + "          \"keyword\": {\n"
        + "            \"type\": \"keyword\",\n"
        + "            \"ignore_above\": 256\n"
        + "          }\n"
        + "        }\n"
        + "      }\n"
        + "    }\n"
        + "  }\n"
        + "}\n";
  }

  public static String getNestedTypeIndexMapping() {
    return "{ \"mappings\": {\n"
        + "        \"properties\": {\n"
        + "          \"message\": {\n"
        + "            \"type\": \"nested\",\n"
        + "            \"properties\": {\n"
        + "              \"info\": {\n"
        + "                \"type\": \"keyword\",\n"
        + "                \"index\": \"true\"\n"
        + "              },\n"
        + "              \"author\": {\n"
        + "                \"type\": \"keyword\",\n"
        + "                \"fields\": {\n"
        + "                  \"keyword\": {\n"
        + "                    \"type\": \"keyword\",\n"
        + "                    \"ignore_above\" : 256\n"
        + "                  }\n"
        + "                },\n"
        + "                \"index\": \"true\"\n"
        + "              },\n"
        + "              \"dayOfWeek\": {\n"
        + "                \"type\": \"long\"\n"
        + "              }\n"
        + "            }\n"
        + "          },\n"
        + "          \"comment\": {\n"
        + "            \"type\": \"nested\",\n"
        + "            \"properties\": {\n"
        + "              \"data\": {\n"
        + "                \"type\": \"keyword\",\n"
        + "                \"index\": \"true\"\n"
        + "              },\n"
        + "              \"likes\": {\n"
        + "                \"type\": \"long\"\n"
        + "              }\n"
        + "            }\n"
        + "          },\n"
        + "          \"myNum\": {\n"
        + "            \"type\": \"long\"\n"
        + "          },\n"
        + "          \"someField\": {\n"
        + "                \"type\": \"keyword\",\n"
        + "                \"index\": \"true\"\n"
        + "          }\n"
        + "        }\n"
        + "      }\n"
        + "    }}";
  }

  public static String getJoinTypeIndexMapping() {
    return "{\n"
        + "  \"mappings\": {\n"
        + "    \"properties\": {\n"
        + "      \"join_field\": {\n"
        + "        \"type\": \"join\",\n"
        + "        \"relations\": {\n"
        + "          \"parentType\": \"childrenType\"\n"
        + "        }\n"
        + "      },\n"
        + "      \"parentTile\": {\n"
        + "        \"index\": \"true\",\n"
        + "        \"type\": \"keyword\"\n"
        + "      },\n"
        + "      \"dayOfWeek\": {\n"
        + "        \"type\": \"long\"\n"
        + "      },\n"
        + "      \"author\": {\n"
        + "        \"index\": \"true\",\n"
        + "        \"type\": \"keyword\"\n"
        + "      },\n"
        + "      \"info\": {\n"
        + "        \"index\": \"true\",\n"
        + "        \"type\": \"keyword\"\n"
        + "      }\n"
        + "    }\n"
        + "  }\n"
        + "}";
  }

  public static String getBankIndexMapping() {
    return "{\n"
        + "  \"mappings\": {\n"
        + "    \"properties\": {\n"
        + "      \"account_number\": {\n"
        + "        \"type\": \"long\"\n"
        + "      },\n"
        + "      \"address\": {\n"
        + "        \"type\": \"text\"\n"
        + "      },\n"
        + "      \"age\": {\n"
        + "        \"type\": \"integer\"\n"
        + "      },\n"
        + "      \"balance\": {\n"
        + "        \"type\": \"long\"\n"
        + "      },\n"
        + "      \"birthdate\": {\n"
        + "        \"type\": \"date\"\n"
        + "      },\n"
        + "      \"city\": {\n"
        + "        \"type\": \"keyword\"\n"
        + "      },\n"
        + "      \"email\": {\n"
        + "        \"type\": \"text\"\n"
        + "      },\n"
        + "      \"employer\": {\n"
        + "        \"type\": \"text\"\n"
        + "      },\n"
        + "      \"firstname\": {\n"
        + "        \"type\": \"text\"\n"
        + "      },\n"
        + "      \"gender\": {\n"
        + "        \"type\": \"text\",\n"
        + "        \"fielddata\": true\n"
        + "      },"
        + "      \"lastname\": {\n"
        + "        \"type\": \"keyword\"\n"
        + "      },\n"
        + "      \"male\": {\n"
        + "        \"type\": \"boolean\"\n"
        + "      },\n"
        + "      \"state\": {\n"
        + "        \"type\": \"text\",\n"
        + "        \"fields\": {\n"
        + "          \"keyword\": {\n"
        + "            \"type\": \"keyword\",\n"
        + "            \"ignore_above\": 256\n"
        + "          }\n"
        + "        }\n"
        + "      }\n"
        + "    }\n"
        + "  }\n"
        + "}";
  }

  public static String getBankWithNullValuesIndexMapping() {
    return "{\n"
        + "  \"mappings\": {\n"
        + "    \"properties\": {\n"
        + "      \"account_number\": {\n"
        + "        \"type\": \"long\"\n"
        + "      },\n"
        + "      \"address\": {\n"
        + "        \"type\": \"text\"\n"
        + "      },\n"
        + "      \"age\": {\n"
        + "        \"type\": \"integer\"\n"
        + "      },\n"
        + "      \"balance\": {\n"
        + "        \"type\": \"long\"\n"
        + "      },\n"
        + "      \"gender\": {\n"
        + "        \"type\": \"text\"\n"
        + "      },\n"
        + "      \"firstname\": {\n"
        + "        \"type\": \"text\"\n"
        + "      },\n"
        + "      \"lastname\": {\n"
        + "        \"type\": \"keyword\"\n"
        + "      }\n"
        + "    }\n"
        + "  }\n"
        + "}";
  }

  public static String getOrderIndexMapping() {
    return "{\n"
        + "  \"mappings\": {\n"
        + "    \"properties\": {\n"
        + "      \"id\": {\n"
        + "        \"type\": \"long\"\n"
        + "      },\n"
        + "      \"name\": {\n"
        + "        \"type\": \"text\",\n"
        + "        \"fields\": {\n"
        + "          \"keyword\": {\n"
        + "            \"type\": \"keyword\",\n"
        + "            \"ignore_above\": 256\n"
        + "          }\n"
        + "        }\n"
        + "      }\n"
        + "    }\n"
        + "  }\n"
        + "}";
  }

  public static String getWeblogsIndexMapping() {
    return "{\n"
        + "  \"mappings\": {\n"
        + "    \"properties\": {\n"
        + "      \"host\": {\n"
        + "        \"type\": \"ip\"\n"
        + "      },\n"
        + "      \"method\": {\n"
        + "        \"type\": \"text\"\n"
        + "      },\n"
        + "      \"url\": {\n"
        + "        \"type\": \"text\"\n"
        + "      },\n"
        + "      \"response\": {\n"
        + "        \"type\": \"text\"\n"
        + "      },\n"
        + "      \"bytes\": {\n"
        + "        \"type\": \"text\"\n"
        + "      }\n"
        + "    }\n"
        + "  }\n"
        + "}";
  }

  public static String getDateIndexMapping() {
    return "{  \"mappings\": {"
        + " \"properties\": {\n"
        + "          \"date_keyword\": {\n"
        + "            \"type\": \"keyword\",\n"
        + "            \"ignore_above\": 256\n"
        + "          }"
        + "       }"
        + "   }"
        + "}";
  }

  public static String getDateTimeIndexMapping() {
    return "{"
        + "  \"mappings\": {"
        + "    \"properties\": {"
        + "      \"birthday\": {"
        + "        \"type\": \"date\""
        + "      }"
        + "    }"
        + "  }"
        + "}";
  }

  public static String getNestedSimpleIndexMapping() {
    return "{"
        + "  \"mappings\": {"
        + "    \"properties\": {"
        + "      \"address\": {"
        + "        \"type\": \"nested\","
        + "        \"properties\": {"
        + "          \"city\": {"
        + "            \"type\": \"text\","
        + "            \"fields\": {"
        + "              \"keyword\": {"
        + "                \"type\": \"keyword\","
        + "                \"ignore_above\": 256"
        + "              }"
        + "            }"
        + "          },"
        + "          \"state\": {"
        + "            \"type\": \"text\","
        + "            \"fields\": {"
        + "              \"keyword\": {"
        + "                \"type\": \"keyword\","
        + "                \"ignore_above\": 256"
        + "              }"
        + "            }"
        + "          }"
        + "        }"
        + "      },"
        + "      \"age\": {"
        + "        \"type\": \"long\""
        + "      },"
        + "      \"id\": {"
        + "        \"type\": \"long\""
        + "      },"
        + "      \"name\": {"
        + "        \"type\": \"text\","
        + "        \"fields\": {"
        + "          \"keyword\": {"
        + "            \"type\": \"keyword\","
        + "            \"ignore_above\": 256"
        + "          }"
        + "        }"
        + "      }"
        + "    }"
        + "  }"
        + "}";
  }

  public static void loadBulk(Client client, String jsonPath, String defaultIndex)
      throws Exception {
    System.out.println(String.format("Loading file %s into OpenSearch cluster", jsonPath));
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

  // TODO: this is temporary fix for fixing serverless tests to pass with 2 digit precision value
  public static JSONArray roundOfResponse(JSONArray array) {
    JSONArray responseJSON = new JSONArray();
    array
        .iterator()
        .forEachRemaining(
            o -> {
              JSONArray jsonArray = new JSONArray();
              ((JSONArray) o)
                  .iterator()
                  .forEachRemaining(
                      i -> {
                        if (i instanceof BigDecimal) {
                          jsonArray.put(((BigDecimal) i).setScale(2, RoundingMode.HALF_UP));
                        } else {
                          jsonArray.put(i);
                        }
                      });
              responseJSON.put(jsonArray);
            });
    return responseJSON;
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

  public static void verifyIsV1Cursor(JSONObject response) {
    var legacyCursorPrefixes =
        Arrays.stream(CursorType.values()).map(c -> c.getId() + ":").collect(Collectors.toList());
    verifyCursor(response, legacyCursorPrefixes, "v1");
  }

  public static void verifyIsV2Cursor(JSONObject response) {
    verifyCursor(response, List.of(CURSOR_PREFIX), "v2");
  }

  private static void verifyCursor(
      JSONObject response, List<String> validCursorPrefix, String engineName) {
    assertTrue("'cursor' property does not exist", response.has("cursor"));

    var cursor = response.getString("cursor");
    assertFalse("'cursor' property is empty", cursor.isEmpty());
    assertTrue(
        "The cursor '" + cursor.substring(0, 50) + "...' is not from " + engineName + " engine.",
        validCursorPrefix.stream().anyMatch(cursor::startsWith));
  }

  public static void verifyNoCursor(JSONObject response) {
    assertTrue(!response.has("cursor"));
  }
}
