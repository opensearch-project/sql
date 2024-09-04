/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.datasource;

import static org.opensearch.sql.datasource.model.DataSourceStatus.ACTIVE;
import static org.opensearch.sql.datasource.model.DataSourceStatus.DISABLED;
import static org.opensearch.sql.datasources.utils.XContentParserUtils.ALLOWED_ROLES_FIELD;
import static org.opensearch.sql.datasources.utils.XContentParserUtils.DESCRIPTION_FIELD;
import static org.opensearch.sql.datasources.utils.XContentParserUtils.NAME_FIELD;
import static org.opensearch.sql.datasources.utils.XContentParserUtils.STATUS_FIELD;
import static org.opensearch.sql.legacy.TestUtils.getResponseBody;

import com.google.common.collect.ImmutableMap;
import com.google.gson.JsonObject;
import com.google.gson.reflect.TypeToken;
import java.io.IOException;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import lombok.SneakyThrows;
import lombok.Value;
import org.json.JSONObject;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Test;
import org.opensearch.client.Request;
import org.opensearch.client.Response;
import org.opensearch.client.ResponseException;
import org.opensearch.sql.common.setting.Settings;
import org.opensearch.sql.datasource.model.DataSourceMetadata;
import org.opensearch.sql.datasource.model.DataSourceType;
import org.opensearch.sql.ppl.PPLIntegTestCase;
import org.opensearch.sql.utils.SerializeUtils;

public class DataSourceAPIsIT extends PPLIntegTestCase {

  @After
  public void cleanUp() throws IOException {
    wipeAllClusterSettings();
  }

  @AfterClass
  protected static void deleteDataSourcesCreated() throws IOException {
    Request deleteRequest = getDeleteDataSourceRequest("create_prometheus");
    Response deleteResponse = client().performRequest(deleteRequest);
    Assert.assertEquals(204, deleteResponse.getStatusLine().getStatusCode());

    deleteRequest = getDeleteDataSourceRequest("update_prometheus");
    deleteResponse = client().performRequest(deleteRequest);
    Assert.assertEquals(204, deleteResponse.getStatusLine().getStatusCode());

    deleteRequest = getDeleteDataSourceRequest("get_all_prometheus");
    deleteResponse = client().performRequest(deleteRequest);
    Assert.assertEquals(204, deleteResponse.getStatusLine().getStatusCode());

    deleteRequest = getDeleteDataSourceRequest("Create_Prometheus");
    deleteResponse = client().performRequest(deleteRequest);
    Assert.assertEquals(204, deleteResponse.getStatusLine().getStatusCode());

    deleteRequest = getDeleteDataSourceRequest("duplicate_prometheus");
    deleteResponse = client().performRequest(deleteRequest);
    Assert.assertEquals(204, deleteResponse.getStatusLine().getStatusCode());

    deleteRequest = getDeleteDataSourceRequest("patch_prometheus");
    deleteResponse = client().performRequest(deleteRequest);
    Assert.assertEquals(204, deleteResponse.getStatusLine().getStatusCode());
  }

  @SneakyThrows
  @Test
  public void createDataSourceAPITest() {
    // create datasource
    DataSourceMetadata createDSM =
        new DataSourceMetadata.Builder()
            .setName("create_prometheus")
            .setDescription("Prometheus Creation for Integ test")
            .setConnector(DataSourceType.PROMETHEUS)
            .setProperties(
                ImmutableMap.of(
                    "prometheus.uri",
                    "https://localhost:9090",
                    "prometheus.auth.type",
                    "basicauth",
                    "prometheus.auth.username",
                    "username",
                    "prometheus.auth.password",
                    "password"))
            .build();
    Request createRequest = getCreateDataSourceRequest(createDSM);
    Response response = client().performRequest(createRequest);
    Assert.assertEquals(201, response.getStatusLine().getStatusCode());
    String createResponseString = getResponseBody(response);
    Assert.assertEquals("\"Created DataSource with name create_prometheus\"", createResponseString);
    // Datasource is not immediately created. so introducing a sleep of 2s.
    Thread.sleep(2000);

    // get datasource to validate the creation.
    Request getRequest = getFetchDataSourceRequest("create_prometheus");
    Response getResponse = client().performRequest(getRequest);
    Assert.assertEquals(200, getResponse.getStatusLine().getStatusCode());
    String getResponseString = getResponseBody(getResponse);
    DataSourceMetadata dataSourceMetadata =
        SerializeUtils.buildGson().fromJson(getResponseString, DataSourceMetadata.class);
    Assert.assertEquals(
        "https://localhost:9090", dataSourceMetadata.getProperties().get("prometheus.uri"));
    Assert.assertEquals(
        "basicauth", dataSourceMetadata.getProperties().get("prometheus.auth.type"));
    Assert.assertNull(dataSourceMetadata.getProperties().get("prometheus.auth.username"));
    Assert.assertNull(dataSourceMetadata.getProperties().get("prometheus.auth.password"));
    Assert.assertEquals(ACTIVE, dataSourceMetadata.getStatus());
    Assert.assertEquals("Prometheus Creation for Integ test", dataSourceMetadata.getDescription());
  }

  @SneakyThrows
  @Test
  public void updateDataSourceAPITest() {
    // create datasource
    DataSourceMetadata createDSM =
        new DataSourceMetadata.Builder()
            .setName("update_prometheus")
            .setConnector(DataSourceType.PROMETHEUS)
            .setProperties(ImmutableMap.of("prometheus.uri", "https://localhost:9090"))
            .build();
    Request createRequest = getCreateDataSourceRequest(createDSM);
    client().performRequest(createRequest);
    // Datasource is not immediately created. so introducing a sleep of 2s.
    Thread.sleep(2000);

    // update datasource
    DataSourceMetadata updateDSM =
        new DataSourceMetadata.Builder()
            .setName("update_prometheus")
            .setConnector(DataSourceType.PROMETHEUS)
            .setProperties(ImmutableMap.of("prometheus.uri", "https://randomtest.com:9090"))
            .build();
    Request updateRequest = getUpdateDataSourceRequest(updateDSM);
    Response updateResponse = client().performRequest(updateRequest);
    Assert.assertEquals(200, updateResponse.getStatusLine().getStatusCode());
    String updateResponseString = getResponseBody(updateResponse);
    Assert.assertEquals("\"Updated DataSource with name update_prometheus\"", updateResponseString);

    // Datasource is not immediately updated. so introducing a sleep of 2s.
    Thread.sleep(2000);

    // get datasource to validate the modification.
    // get datasource
    Request getRequest = getFetchDataSourceRequest("update_prometheus");
    Response getResponse = client().performRequest(getRequest);
    Assert.assertEquals(200, getResponse.getStatusLine().getStatusCode());
    String getResponseString = getResponseBody(getResponse);
    DataSourceMetadata dataSourceMetadata =
        SerializeUtils.buildGson().fromJson(getResponseString, DataSourceMetadata.class);
    Assert.assertEquals(
        "https://randomtest.com:9090", dataSourceMetadata.getProperties().get("prometheus.uri"));
    Assert.assertEquals("", dataSourceMetadata.getDescription());

    // patch datasource
    Map<String, Object> updateDS =
        new HashMap<>(Map.of(NAME_FIELD, "update_prometheus", DESCRIPTION_FIELD, "test"));
    Request patchRequest = getPatchDataSourceRequest(updateDS);
    Response patchResponse = client().performRequest(patchRequest);
    Assert.assertEquals(200, patchResponse.getStatusLine().getStatusCode());
    String patchResponseString = getResponseBody(patchResponse);
    Assert.assertEquals("\"Updated DataSource with name update_prometheus\"", patchResponseString);

    // Datasource is not immediately updated. so introducing a sleep of 2s.
    Thread.sleep(2000);

    // get datasource to validate the modification.
    // get datasource
    Request getRequestAfterPatch = getFetchDataSourceRequest("update_prometheus");
    Response getResponseAfterPatch = client().performRequest(getRequestAfterPatch);
    Assert.assertEquals(200, getResponseAfterPatch.getStatusLine().getStatusCode());
    String getResponseStringAfterPatch = getResponseBody(getResponseAfterPatch);
    DataSourceMetadata dataSourceMetadataAfterPatch =
        SerializeUtils.buildGson().fromJson(getResponseStringAfterPatch, DataSourceMetadata.class);
    Assert.assertEquals(
        "https://randomtest.com:9090",
        dataSourceMetadataAfterPatch.getProperties().get("prometheus.uri"));
    Assert.assertEquals("test", dataSourceMetadataAfterPatch.getDescription());
  }

  @SneakyThrows
  @Test
  public void deleteDataSourceTest() {

    // create datasource for deletion
    DataSourceMetadata createDSM =
        new DataSourceMetadata.Builder()
            .setName("delete_prometheus")
            .setConnector(DataSourceType.PROMETHEUS)
            .setProperties(ImmutableMap.of("prometheus.uri", "https://localhost:9090"))
            .build();
    Request createRequest = getCreateDataSourceRequest(createDSM);
    client().performRequest(createRequest);
    // Datasource is not immediately created. so introducing a sleep of 2s.
    Thread.sleep(2000);

    // delete datasource
    Request deleteRequest = getDeleteDataSourceRequest("delete_prometheus");
    Response deleteResponse = client().performRequest(deleteRequest);
    Assert.assertEquals(204, deleteResponse.getStatusLine().getStatusCode());

    // Datasource is not immediately deleted. so introducing a sleep of 2s.
    Thread.sleep(2000);

    // get datasources to verify the deletion
    final Request prometheusGetRequest = getFetchDataSourceRequest("delete_prometheus");
    ResponseException prometheusGetResponseException =
        Assert.assertThrows(
            ResponseException.class, () -> client().performRequest(prometheusGetRequest));
    Assert.assertEquals(
        404, prometheusGetResponseException.getResponse().getStatusLine().getStatusCode());
    String prometheusGetResponseString =
        getResponseBody(prometheusGetResponseException.getResponse());
    JsonObject errorMessage =
        SerializeUtils.buildGson().fromJson(prometheusGetResponseString, JsonObject.class);
    Assert.assertEquals(
        "DataSource with name delete_prometheus doesn't exist.",
        errorMessage.get("error").getAsJsonObject().get("details").getAsString());
  }

  @SneakyThrows
  @Test
  public void getAllDataSourceTest() {
    // create datasource for deletion
    DataSourceMetadata createDSM =
        new DataSourceMetadata.Builder()
            .setName("get_all_prometheus")
            .setConnector(DataSourceType.PROMETHEUS)
            .setProperties(ImmutableMap.of("prometheus.uri", "https://localhost:9090"))
            .build();
    Request createRequest = getCreateDataSourceRequest(createDSM);
    client().performRequest(createRequest);
    // Datasource is not immediately created. so introducing a sleep of 2s.
    Thread.sleep(2000);

    Request getRequest = getFetchDataSourceRequest(null);
    Response getResponse = client().performRequest(getRequest);
    Assert.assertEquals(200, getResponse.getStatusLine().getStatusCode());
    String getResponseString = getResponseBody(getResponse);
    Type listType = new TypeToken<ArrayList<DataSourceMetadata>>() {}.getType();
    List<DataSourceMetadata> dataSourceMetadataList =
        SerializeUtils.buildGson().fromJson(getResponseString, listType);
    Assert.assertTrue(
        dataSourceMetadataList.stream().anyMatch(ds -> ds.getName().equals("get_all_prometheus")));
  }

  /** https://github.com/opensearch-project/sql/issues/2196 */
  @SneakyThrows
  @Test
  public void issue2196() {
    // create datasource
    DataSourceMetadata createDSM =
        new DataSourceMetadata.Builder()
            .setName("Create_Prometheus")
            .setDescription("Prometheus Creation for Integ test")
            .setConnector(DataSourceType.PROMETHEUS)
            .setProperties(
                ImmutableMap.of(
                    "prometheus.uri",
                    "https://localhost:9090",
                    "prometheus.auth.type",
                    "basicauth",
                    "prometheus.auth.username",
                    "username",
                    "prometheus.auth.password",
                    "password"))
            .build();
    Request createRequest = getCreateDataSourceRequest(createDSM);
    Response response = client().performRequest(createRequest);
    Assert.assertEquals(201, response.getStatusLine().getStatusCode());
    String createResponseString = getResponseBody(response);
    Assert.assertEquals("\"Created DataSource with name Create_Prometheus\"", createResponseString);
    // Datasource is not immediately created. so introducing a sleep of 2s.
    Thread.sleep(2000);

    // get datasource to validate the creation.
    Request getRequest = getFetchDataSourceRequest("Create_Prometheus");
    Response getResponse = client().performRequest(getRequest);
    Assert.assertEquals(200, getResponse.getStatusLine().getStatusCode());
    String getResponseString = getResponseBody(getResponse);
    DataSourceMetadata dataSourceMetadata =
        SerializeUtils.buildGson().fromJson(getResponseString, DataSourceMetadata.class);
    Assert.assertEquals(
        "https://localhost:9090", dataSourceMetadata.getProperties().get("prometheus.uri"));
    Assert.assertEquals(
        "basicauth", dataSourceMetadata.getProperties().get("prometheus.auth.type"));
    Assert.assertNull(dataSourceMetadata.getProperties().get("prometheus.auth.username"));
    Assert.assertNull(dataSourceMetadata.getProperties().get("prometheus.auth.password"));
    Assert.assertEquals("Prometheus Creation for Integ test", dataSourceMetadata.getDescription());
  }

  @Test
  public void datasourceLimitTest() throws InterruptedException, IOException {
    DataSourceMetadata d1 = mockDataSourceMetadata("duplicate_prometheus");
    Request createRequest = getCreateDataSourceRequest(d1);
    Response response = client().performRequest(createRequest);
    Assert.assertEquals(201, response.getStatusLine().getStatusCode());
    // Datasource is not immediately created. so introducing a sleep of 2s.
    Thread.sleep(2000);

    updateClusterSettings(new ClusterSetting(TRANSIENT, "plugins.query.datasources.limit", "1"));

    DataSourceMetadata d2 = mockDataSourceMetadata("d2");
    ResponseException exception =
        Assert.assertThrows(
            ResponseException.class, () -> client().performRequest(getCreateDataSourceRequest(d2)));
    Assert.assertEquals(400, exception.getResponse().getStatusLine().getStatusCode());
    String prometheusGetResponseString = getResponseBody(exception.getResponse());
    JsonObject errorMessage =
        SerializeUtils.buildGson().fromJson(prometheusGetResponseString, JsonObject.class);
    Assert.assertEquals(
        "domain concurrent datasources can not exceed 1",
        errorMessage.get("error").getAsJsonObject().get("details").getAsString());
  }

  @SneakyThrows
  @Test
  public void patchDataSourceAPITest() {
    // create datasource
    DataSourceMetadata createDSM =
        new DataSourceMetadata.Builder()
            .setName("patch_prometheus")
            .setDescription("Prometheus Creation for Integ test")
            .setConnector(DataSourceType.PROMETHEUS)
            .setProperties(
                ImmutableMap.of(
                    "prometheus.uri",
                    "https://localhost:9090",
                    "prometheus.auth.type",
                    "basicauth",
                    "prometheus.auth.username",
                    "username",
                    "prometheus.auth.password",
                    "password"))
            .setAllowedRoles(List.of("role1", "role2"))
            .build();
    Request createRequest = getCreateDataSourceRequest(createDSM);
    Response response = client().performRequest(createRequest);
    Assert.assertEquals(201, response.getStatusLine().getStatusCode());
    String createResponseString = getResponseBody(response);
    Assert.assertEquals("\"Created DataSource with name patch_prometheus\"", createResponseString);
    // Datasource is not immediately created. so introducing a sleep of 2s.
    Thread.sleep(2000);

    // patch datasource
    Map<String, Object> updateDS =
        new HashMap<>(
            Map.of(
                NAME_FIELD,
                "patch_prometheus",
                DESCRIPTION_FIELD,
                "test",
                STATUS_FIELD,
                "disabled",
                ALLOWED_ROLES_FIELD,
                List.of("role3", "role4")));

    Request patchRequest = getPatchDataSourceRequest(updateDS);
    Response patchResponse = client().performRequest(patchRequest);
    Assert.assertEquals(200, patchResponse.getStatusLine().getStatusCode());
    String patchResponseString = getResponseBody(patchResponse);
    Assert.assertEquals("\"Updated DataSource with name patch_prometheus\"", patchResponseString);

    // Datasource is not immediately updated. so introducing a sleep of 2s.
    Thread.sleep(2000);

    // get datasource to validate the creation.
    Request getRequest = getFetchDataSourceRequest("patch_prometheus");
    Response getResponse = client().performRequest(getRequest);
    Assert.assertEquals(200, getResponse.getStatusLine().getStatusCode());
    String getResponseString = getResponseBody(getResponse);
    DataSourceMetadata dataSourceMetadata =
        SerializeUtils.buildGson().fromJson(getResponseString, DataSourceMetadata.class);
    Assert.assertEquals(
        "https://localhost:9090", dataSourceMetadata.getProperties().get("prometheus.uri"));
    Assert.assertEquals(
        "basicauth", dataSourceMetadata.getProperties().get("prometheus.auth.type"));
    Assert.assertNull(dataSourceMetadata.getProperties().get("prometheus.auth.username"));
    Assert.assertNull(dataSourceMetadata.getProperties().get("prometheus.auth.password"));
    Assert.assertEquals(DISABLED, dataSourceMetadata.getStatus());
    Assert.assertEquals(List.of("role3", "role4"), dataSourceMetadata.getAllowedRoles());
    Assert.assertEquals("test", dataSourceMetadata.getDescription());
  }

  @Test
  public void testDataSourcesEnabledSettingIsTrueByDefault() {
    Assert.assertTrue(getDataSourceEnabledSetting("defaults"));
  }

  @Test
  public void testDataSourcesEnabledSettingCanBeSetToTransientFalse() {
    setDataSourcesEnabled("transient", false);
    Assert.assertFalse(getDataSourceEnabledSetting("transient"));
  }

  @Test
  public void testDataSourcesEnabledSettingCanBeSetToTransientTrue() {
    setDataSourcesEnabled("transient", true);
    Assert.assertTrue(getDataSourceEnabledSetting("transient"));
  }

  @Test
  public void testDataSourcesEnabledSettingCanBeSetToPersistentFalse() {
    setDataSourcesEnabled("persistent", false);
    Assert.assertFalse(getDataSourceEnabledSetting("persistent"));
  }

  @Test
  public void testDataSourcesEnabledSettingCanBeSetToPersistentTrue() {
    setDataSourcesEnabled("persistent", true);
    Assert.assertTrue(getDataSourceEnabledSetting("persistent"));
  }

  @Test
  public void testDataSourcesEnabledSetToFalseRejectsApiOperations() {
    setDataSourcesEnabled("transient", false);
    validateAllDataSourceApisWithEnabledSetting(false);
  }

  @Test
  public void testDataSourcesEnabledSetToTrueAllowsApiOperations() {
    setDataSourcesEnabled("transient", true);
    validateAllDataSourceApisWithEnabledSetting(true);
  }

  @SneakyThrows
  private void validateAllDataSourceApisWithEnabledSetting(boolean dataSourcesEnabled) {

    @Value
    class TestCase {
      Request request;
      int expectedResponseCodeOnSuccess;
      String expectResponseToContainOnSuccess;
    }

    TestCase[] testCases =
        new TestCase[] {
          // create
          new TestCase(
              getCreateDataSourceRequest(mockDataSourceMetadata("dummy")),
              201,
              "Created DataSource"),
          // read
          new TestCase(getFetchDataSourceRequest("dummy"), 200, "dummy"),
          // update
          new TestCase(
              getUpdateDataSourceRequest(mockDataSourceMetadata("dummy")),
              200,
              "Updated DataSource"),
          // list
          new TestCase(getFetchDataSourceRequest(null), 200, "dummy"),
          // delete
          new TestCase(getDeleteDataSourceRequest("dummy"), 204, null)
        };

    for (TestCase testCase : testCases) {

      // data source APIs are eventually consistent. sleep delay is added for consistency
      // see createDataSourceAPITest above.
      Thread.sleep(2_000);

      final int expectedResponseCode =
          dataSourcesEnabled ? testCase.getExpectedResponseCodeOnSuccess() : 400;

      final String expectedResponseBodyToContain =
          dataSourcesEnabled
              ? testCase.getExpectResponseToContainOnSuccess()
              : "plugins.query.datasources.enabled setting is false";

      Response response;

      try {
        response = client().performRequest(testCase.getRequest());
      } catch (ResponseException e) {
        response = e.getResponse();
      }

      Assert.assertEquals(
          String.format(
              "Test for " + testCase + " failed. Expected response code of %s, but got %s",
              expectedResponseCode,
              response.getStatusLine().getStatusCode()),
          expectedResponseCode,
          response.getStatusLine().getStatusCode());

      if (expectedResponseBodyToContain != null) {

        String responseBody = getResponseBody(response);

        Assert.assertTrue(
            String.format(
                "Test for " + testCase + " failed. '%s' failed to contain '%s'",
                responseBody,
                expectedResponseBodyToContain),
            responseBody.contains(expectedResponseBodyToContain));
      }
    }
  }

  @SneakyThrows
  private boolean getDataSourceEnabledSetting(String... clusterSettingsTypeKeys) {

    final String settingKey = Settings.Key.DATASOURCES_ENABLED.getKeyValue();

    JSONObject settings = getAllClusterSettings();

    return Arrays.stream(clusterSettingsTypeKeys)
        .map(settings::getJSONObject)
        .filter(obj -> obj.has(settingKey))
        .map(obj -> obj.getBoolean(settingKey))
        .findFirst()
        .orElseThrow();
  }

  public DataSourceMetadata mockDataSourceMetadata(String name) {
    return new DataSourceMetadata.Builder()
        .setName(name)
        .setDescription("Prometheus Creation for Integ test")
        .setConnector(DataSourceType.PROMETHEUS)
        .setProperties(
            ImmutableMap.of(
                "prometheus.uri",
                "https://localhost:9090",
                "prometheus.auth.type",
                "basicauth",
                "prometheus.auth.username",
                "username",
                "prometheus.auth.password",
                "password"))
        .build();
  }
}
