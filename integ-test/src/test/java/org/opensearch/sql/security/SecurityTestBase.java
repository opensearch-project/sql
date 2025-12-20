/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.security;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import org.json.JSONObject;
import org.opensearch.client.Request;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.Response;
import org.opensearch.sql.ppl.PPLIntegTestCase;

/**
 * Base class for security-related integration tests. Provides common utilities for creating users,
 * roles, and executing queries with authentication.
 */
public abstract class SecurityTestBase extends PPLIntegTestCase {

  protected static final String STRONG_PASSWORD = "correcthorsebatterystaple";

  /**
   * Creates a role with access to a specific index pattern and standard permissions.
   *
   * @param roleName the name of the role
   * @param indexPattern the index pattern to grant access to
   */
  protected void createRoleWithIndexAccess(String roleName, String indexPattern)
      throws IOException {
    createRoleWithPermissions(
        roleName,
        indexPattern,
        new String[] {"cluster:admin/opensearch/ppl"},
        new String[] {
          "indices:data/read/search*",
          "indices:admin/mappings/get",
          "indices:monitor/settings/get",
          "indices:data/read/point_in_time/create",
          "indices:data/read/point_in_time/delete"
        });
  }

  /**
   * Creates a role with specific cluster and index permissions.
   *
   * @param roleName the name of the role
   * @param indexPattern the index pattern to grant access to
   * @param clusterPermissions array of cluster-level permissions
   * @param indexPermissions array of index-level permissions
   */
  protected void createRoleWithPermissions(
      String roleName, String indexPattern, String[] clusterPermissions, String[] indexPermissions)
      throws IOException {

    Request request = new Request("PUT", "/_plugins/_security/api/roles/" + roleName);

    StringBuilder clusterPermsJson = new StringBuilder();
    for (int i = 0; i < clusterPermissions.length; i++) {
      clusterPermsJson.append("\"").append(clusterPermissions[i]).append("\"");
      if (i < clusterPermissions.length - 1) clusterPermsJson.append(",");
    }

    StringBuilder indexPermsJson = new StringBuilder();
    for (int i = 0; i < indexPermissions.length; i++) {
      indexPermsJson.append("\"").append(indexPermissions[i]).append("\"");
      if (i < indexPermissions.length - 1) indexPermsJson.append(",");
    }

    request.setJsonEntity(
        String.format(
            Locale.ROOT,
            """
            {
              "cluster_permissions": [%s],
              "index_permissions": [{
                "index_patterns": ["%s"],
                "allowed_actions": [%s]
              }]
            }
            """,
            clusterPermsJson,
            indexPattern,
            indexPermsJson));

    RequestOptions.Builder restOptionsBuilder = RequestOptions.DEFAULT.toBuilder();
    restOptionsBuilder.addHeader("Content-Type", "application/json");
    request.setOptions(restOptionsBuilder);

    Response response = client().performRequest(request);
    assertTrue(
        response.getStatusLine().getStatusCode() == 200
            || response.getStatusLine().getStatusCode() == 201);
  }

  /**
   * Creates a role with document-level security (DLS) filtering.
   *
   * @param roleName the name of the role
   * @param indexPattern the index pattern to grant access to
   * @param dlsQuery the document-level security query in escaped JSON string format
   */
  protected void createRoleWithDLS(String roleName, String indexPattern, String dlsQuery)
      throws IOException {
    Request request = new Request("PUT", "/_plugins/_security/api/roles/" + roleName);
    request.setJsonEntity(
        String.format(
            Locale.ROOT,
            """
            {
              "cluster_permissions": [
                "cluster:admin/opensearch/ppl"
              ],
              "index_permissions": [{
                "index_patterns": [
                  "%s"
                ],
                "allowed_actions": [
                  "indices:data/read/search*",
                  "indices:admin/mappings/get",
                  "indices:monitor/settings/get",
                  "indices:data/read/point_in_time/create",
                  "indices:data/read/point_in_time/delete"
                ],
                "dls": "%s"
              }]
            }
            """,
            indexPattern,
            dlsQuery));

    RequestOptions.Builder restOptionsBuilder = RequestOptions.DEFAULT.toBuilder();
    restOptionsBuilder.addHeader("Content-Type", "application/json");
    request.setOptions(restOptionsBuilder);

    Response response = client().performRequest(request);
    assertTrue(
        response.getStatusLine().getStatusCode() == 200
            || response.getStatusLine().getStatusCode() == 201);
  }

  /**
   * Creates a role with field-level security (FLS) restrictions.
   *
   * @param roleName the name of the role
   * @param indexPattern the index pattern to grant access to
   * @param allowedFields array of field names that the role can access
   */
  protected void createRoleWithFLS(String roleName, String indexPattern, String[] allowedFields)
      throws IOException {
    StringBuilder fieldsJson = new StringBuilder();
    for (int i = 0; i < allowedFields.length; i++) {
      if (i > 0) fieldsJson.append(", ");
      fieldsJson.append("\"").append(allowedFields[i]).append("\"");
    }

    Request request = new Request("PUT", "/_plugins/_security/api/roles/" + roleName);
    request.setJsonEntity(
        String.format(
            Locale.ROOT,
            """
            {
              "cluster_permissions": [
                "cluster:admin/opensearch/ppl"
              ],
              "index_permissions": [{
                "index_patterns": [
                  "%s"
                ],
                "allowed_actions": [
                  "indices:data/read/search*",
                  "indices:admin/mappings/get",
                  "indices:monitor/settings/get",
                  "indices:data/read/point_in_time/create",
                  "indices:data/read/point_in_time/delete"
                ],
                "fls": [%s]
              }]
            }
            """,
            indexPattern,
            fieldsJson));

    RequestOptions.Builder restOptionsBuilder = RequestOptions.DEFAULT.toBuilder();
    restOptionsBuilder.addHeader("Content-Type", "application/json");
    request.setOptions(restOptionsBuilder);

    Response response = client().performRequest(request);
    assertTrue(
        response.getStatusLine().getStatusCode() == 200
            || response.getStatusLine().getStatusCode() == 201);
  }

  /**
   * Creates a user and maps them to a role.
   *
   * @param username the username
   * @param roleName the role to map the user to
   */
  protected void createUser(String username, String roleName) throws IOException {
    // Create user with password
    Request userRequest = new Request("PUT", "/_plugins/_security/api/internalusers/" + username);
    userRequest.setJsonEntity(
        String.format(
            Locale.ROOT,
            """
            {
              "password": "%s",
              "backend_roles": [],
              "attributes": {}
            }
            """,
            STRONG_PASSWORD));

    RequestOptions.Builder restOptionsBuilder = RequestOptions.DEFAULT.toBuilder();
    restOptionsBuilder.addHeader("Content-Type", "application/json");
    userRequest.setOptions(restOptionsBuilder);

    Response userResponse = client().performRequest(userRequest);
    assertTrue(
        userResponse.getStatusLine().getStatusCode() == 200
            || userResponse.getStatusLine().getStatusCode() == 201);

    // Map user to role
    Request mappingRequest = new Request("PUT", "/_plugins/_security/api/rolesmapping/" + roleName);
    mappingRequest.setJsonEntity(
        String.format(
            Locale.ROOT,
            """
            {
              "backend_roles": [],
              "hosts": [],
              "users": ["%s"]
            }
            """,
            username));

    mappingRequest.setOptions(restOptionsBuilder);

    Response mappingResponse = client().performRequest(mappingRequest);
    assertTrue(
        mappingResponse.getStatusLine().getStatusCode() == 200
            || mappingResponse.getStatusLine().getStatusCode() == 201);
  }

  /**
   * Executes a PPL query as a specific user with basic authentication.
   *
   * @param query the PPL query to execute
   * @param username the username to authenticate as
   * @return the JSON response from the query
   */
  protected JSONObject executeQueryAsUser(String query, String username) throws IOException {
    Request request = new Request("POST", "/_plugins/_ppl");
    request.setJsonEntity(
        String.format(
            Locale.ROOT,
            """
            {
              "query": "%s"
            }
            """,
            query));

    RequestOptions.Builder restOptionsBuilder = RequestOptions.DEFAULT.toBuilder();
    restOptionsBuilder.addHeader("Content-Type", "application/json");
    restOptionsBuilder.addHeader("Authorization", createBasicAuthHeader(username, STRONG_PASSWORD));
    request.setOptions(restOptionsBuilder);

    Response response = client().performRequest(request);
    assertEquals(200, response.getStatusLine().getStatusCode());
    return new JSONObject(org.opensearch.sql.legacy.TestUtils.getResponseBody(response, true));
  }

  /**
   * Creates a Basic authentication header value.
   *
   * @param username the username
   * @param password the password
   * @return the Basic auth header value
   */
  protected String createBasicAuthHeader(String username, String password) {
    return "Basic "
        + java.util.Base64.getEncoder().encodeToString((username + ":" + password).getBytes());
  }

  /**
   * Helper to build bulk insert request body for multiple documents.
   *
   * @param indexName the index to insert into
   * @param documents list of document maps (field name -> value)
   * @return the bulk request body as a string
   */
  protected String buildBulkInsertRequest(String indexName, List<Map<String, Object>> documents) {
    StringBuilder bulk = new StringBuilder();
    for (Map<String, Object> doc : documents) {
      bulk.append(String.format(Locale.ROOT, "{ \"index\": { \"_index\": \"%s\" } }\n", indexName));
      bulk.append(new JSONObject(doc).toString());
      bulk.append("\n");
    }
    return bulk.toString();
  }

  /**
   * Performs a bulk insert operation with automatic refresh.
   *
   * @param bulkRequestBody the bulk request body (NDJSON format)
   * @return the response from the bulk operation
   */
  protected Response performBulkInsert(String bulkRequestBody) throws IOException {
    Request request = new Request("POST", "/_bulk");
    request.addParameter("refresh", "true");
    request.setJsonEntity(bulkRequestBody);

    RequestOptions.Builder restOptionsBuilder = RequestOptions.DEFAULT.toBuilder();
    restOptionsBuilder.addHeader("Content-Type", "application/x-ndjson");
    request.setOptions(restOptionsBuilder);

    Response response = client().performRequest(request);
    assertEquals(200, response.getStatusLine().getStatusCode());
    return response;
  }

  /**
   * Creates an index with a simple mapping.
   *
   * @param indexName the name of the index
   * @param mappingJson the mapping definition as JSON string
   */
  protected void createIndexWithMapping(String indexName, String mappingJson) throws IOException {
    Request request = new Request("PUT", "/" + indexName);
    request.setJsonEntity(mappingJson);
    Response response = client().performRequest(request);
    assertEquals(200, response.getStatusLine().getStatusCode());
  }

  /** Simple builder for creating bulk insert documents. */
  protected static class BulkDocumentBuilder {
    private final List<Map<String, Object>> documents = new ArrayList<>();

    public BulkDocumentBuilder addDocument(Map<String, Object> doc) {
      documents.add(doc);
      return this;
    }

    public List<Map<String, Object>> build() {
      return documents;
    }
  }
}
