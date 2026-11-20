/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.security;

import static org.opensearch.sql.spark.execution.statestore.StateStore.MAPPING_FILE_NAME;
import static org.opensearch.sql.spark.execution.statestore.StateStore.SETTINGS_FILE_NAME;

import java.io.IOException;
import java.io.InputStream;
import java.util.Map;
import org.json.JSONObject;
import org.junit.After;
import org.junit.Test;
import org.opensearch.client.Request;
import org.opensearch.client.Response;
import org.opensearch.client.ResponseException;
import org.opensearch.common.xcontent.XContentHelper;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.sql.legacy.TestUtils;
import org.opensearch.sql.spark.execution.statestore.OpenSearchStateStoreUtil;
import org.opensearch.sql.spark.execution.statestore.StateStore;
import org.opensearch.sql.util.ClusterPlugins;

/**
 * Integration tests for {@link #superAdminClient()}. The session index of a datasource is a system
 * index of the plugin, so the basic auth admin user of {@link #client()} cannot write to it, and a
 * test that needs session documents has to seed them with the admin certificate.
 */
public class SuperAdminClientSecurityIT extends SecurityTestBase {

  private static final String SESSION_INDEX =
      OpenSearchStateStoreUtil.getIndexName("superadminclientit");

  private static final String SESSION_DOC = "{\"type\":\"session\",\"state\":\"running\"}";

  private boolean sessionIndexCreated;

  @Override
  protected void init() throws Exception {
    ClusterPlugins.requirePluginOrAssume(
        client(),
        ClusterPlugins.SECURITY_PLUGIN,
        "opensearch-security plugin not installed on test cluster; skipping admin certificate"
            + " tests");
    super.init();
    deleteSessionIndex();
    createSessionIndex();
    sessionIndexCreated = true;
  }

  @After
  public void cleanUpSessionIndex() throws IOException {
    // Skipped tests never reach init(), and the client needs the secured task's properties.
    if (sessionIndexCreated) {
      deleteSessionIndex();
    }
  }

  @Test
  public void superAdminClientWritesAndReadsSessionIndex() throws IOException {
    Response put = superAdminClient().performRequest(putSessionDoc("seeded"));
    assertEquals(201, put.getStatusLine().getStatusCode());

    JSONObject doc = getSessionDoc("seeded");
    assertTrue(doc.getBoolean("found"));
    assertEquals("running", doc.getJSONObject("_source").getString("state"));
  }

  @Test
  public void basicAuthAdminCannotWriteSessionIndex() throws IOException {
    ResponseException e =
        assertThrows(
            ResponseException.class, () -> client().performRequest(putSessionDoc("denied")));
    assertEquals(403, e.getResponse().getStatusLine().getStatusCode());
    assertEquals(
        "security_exception",
        new JSONObject(TestUtils.getResponseBody(e.getResponse()))
            .getJSONObject("error")
            .getString("type"));

    assertFalse(getSessionDoc("denied").getBoolean("found"));
  }

  /** Creates the index with the settings and mapping the plugin's {@link StateStore} uses. */
  private void createSessionIndex() throws IOException {
    JSONObject body =
        new JSONObject()
            .put("settings", new JSONObject(loadYaml(SETTINGS_FILE_NAME)))
            .put("mappings", new JSONObject(loadYaml(MAPPING_FILE_NAME)));
    Request request = new Request("PUT", "/" + SESSION_INDEX);
    request.setJsonEntity(body.toString());
    superAdminClient().performRequest(request);
  }

  private void deleteSessionIndex() throws IOException {
    Request request = new Request("DELETE", "/" + SESSION_INDEX);
    request.addParameter("ignore_unavailable", "true");
    superAdminClient().performRequest(request);
  }

  private static Map<String, Object> loadYaml(String resource) throws IOException {
    try (InputStream in = StateStore.class.getClassLoader().getResourceAsStream(resource)) {
      assertNotNull("Missing plugin resource " + resource, in);
      return XContentHelper.convertToMap(XContentType.YAML.xContent(), in, true);
    }
  }

  private static Request putSessionDoc(String id) {
    Request request = new Request("PUT", "/" + SESSION_INDEX + "/_doc/" + id);
    request.addParameter("refresh", "true");
    request.setJsonEntity(SESSION_DOC);
    return request;
  }

  private JSONObject getSessionDoc(String id) throws IOException {
    Request request = new Request("GET", "/" + SESSION_INDEX + "/_doc/" + id);
    try {
      return new JSONObject(TestUtils.getResponseBody(superAdminClient().performRequest(request)));
    } catch (ResponseException e) {
      // Only a missing document is an answer; a missing index has no "found" field.
      if (e.getResponse().getStatusLine().getStatusCode() != 404) {
        throw e;
      }
      JSONObject body = new JSONObject(TestUtils.getResponseBody(e.getResponse()));
      if (!body.has("found")) {
        throw e;
      }
      return body;
    }
  }
}
