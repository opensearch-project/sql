/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.security;

import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_BANK;
import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_DOG;
import static org.opensearch.sql.util.MatcherUtils.columnName;
import static org.opensearch.sql.util.MatcherUtils.rows;
import static org.opensearch.sql.util.MatcherUtils.verifyColumn;
import static org.opensearch.sql.util.MatcherUtils.verifyDataRows;

import java.io.IOException;
import java.util.Locale;
import org.json.JSONObject;
import org.junit.Test;
import org.opensearch.client.Request;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.Response;
import org.opensearch.client.ResponseException;
import org.opensearch.sql.ppl.PPLIntegTestCase;

/**
 * Integration tests for PPL permissions issue fix. Tests that PPL queries work correctly when users
 * have permissions for specific indices by ensuring the search request is properly scoped to the
 * requested index.
 *
 * <p>This test verifies the fix for the issue where the security plugin was evaluating permissions
 * for all indices instead of just the requested index when no indices were specified in the
 * SearchRequest.
 */
public class PPLPermissionsIT extends PPLIntegTestCase {

  private static final String BANK_USER = "bank_user";
  private static final String BANK_ROLE = "bank_role";
  private static final String DOG_USER = "dog_user";
  private static final String DOG_ROLE = "dog_role";
  private static final String STRONG_PASSWORD = "StrongPassword123!";

  // Users for testing missing permissions
  private static final String NO_PPL_USER = "no_ppl_user";
  private static final String NO_PPL_ROLE = "no_ppl_role";
  private static final String NO_SEARCH_USER = "no_search_user";
  private static final String NO_SEARCH_ROLE = "no_search_role";
  private static final String NO_MAPPING_USER = "no_mapping_user";
  private static final String NO_MAPPING_ROLE = "no_mapping_role";
  private static final String NO_SETTINGS_USER = "no_settings_user";
  private static final String NO_SETTINGS_ROLE = "no_settings_role";

  // User with minimal permissions for plugin-based PIT testing
  private static final String MINIMAL_USER = "minimal_user";
  private static final String MINIMAL_ROLE = "minimal_role";

  // User without PIT permissions to test PIT requirement
  private static final String NO_PIT_USER = "no_pit_user";
  private static final String NO_PIT_ROLE = "no_pit_role";

  private boolean initialized = false;

  @Override
  protected void init() throws Exception {
    super.init();
    createSecurityRolesAndUsers();
    loadIndex(Index.BANK);
    loadIndex(Index.DOG);
    // Enable Calcite engine to test PIT behavior with Calcite
    enableCalcite();
    // TODO Handle permission failure in v3
    allowCalciteFallback();
  }

  /**
   * Creates security roles and users with minimal permissions for testing. Each user only has
   * access to their specific index.
   */
  private void createSecurityRolesAndUsers() throws IOException {
    if (!initialized) {
      // Create role for bank index access
      createRole(BANK_ROLE, TEST_INDEX_BANK);

      // Create role for dog index access
      createRole(DOG_ROLE, TEST_INDEX_DOG);

      // Create users and map them to roles
      createUser(BANK_USER, BANK_ROLE);
      createUser(DOG_USER, DOG_ROLE);

      // Create roles for testing missing permissions
      createRoleWithMissingPermissions();

      // Create user with minimal permissions for plugin-based PIT testing
      createMinimalUserForPitTesting();

      // Create user without PIT permissions to test PIT requirement
      createNoPitUserForTesting();
      initialized = true;
    }
  }

  private void createRole(String roleName, String indexPattern) throws IOException {
    Request request = new Request("PUT", "/_plugins/_security/api/roles/" + roleName);
    request.setJsonEntity(
        String.format(
            Locale.ROOT,
            "{"
                + "\"cluster_permissions\": ["
                + "\"cluster:admin/opensearch/ppl\""
                + "],"
                + "\"index_permissions\": [{"
                + "\"index_patterns\": ["
                + "\"%s\""
                + "],"
                + "\"allowed_actions\": ["
                + "\"indices:data/read/search*\","
                + "\"indices:admin/mappings/get\","
                + "\"indices:monitor/settings/get\","
                + "\"indices:data/read/point_in_time/create\","
                + "\"indices:data/read/point_in_time/delete\""
                + "]"
                + "}]"
                + "}",
            indexPattern));

    RequestOptions.Builder restOptionsBuilder = RequestOptions.DEFAULT.toBuilder();
    restOptionsBuilder.addHeader("Content-Type", "application/json");
    request.setOptions(restOptionsBuilder);

    Response response = client().performRequest(request);
    // Role creation returns 201 (Created) for new roles or 200 (OK) for updates
    assertTrue(
        response.getStatusLine().getStatusCode() == 200
            || response.getStatusLine().getStatusCode() == 201);
  }

  private void createUser(String username, String roleName) throws IOException {
    // Create user with password
    Request userRequest = new Request("PUT", "/_plugins/_security/api/internalusers/" + username);
    userRequest.setJsonEntity(
        String.format(
            Locale.ROOT,
            "{"
                + "\"password\": \"%s\","
                + "\"backend_roles\": [],"
                + "\"attributes\": {}"
                + "}",
            STRONG_PASSWORD));

    RequestOptions.Builder restOptionsBuilder = RequestOptions.DEFAULT.toBuilder();
    restOptionsBuilder.addHeader("Content-Type", "application/json");
    userRequest.setOptions(restOptionsBuilder);

    Response userResponse = client().performRequest(userRequest);
    // User creation returns 201 (Created) for new users or 200 (OK) for updates
    assertTrue(
        userResponse.getStatusLine().getStatusCode() == 200
            || userResponse.getStatusLine().getStatusCode() == 201);

    // Map user to role
    Request mappingRequest = new Request("PUT", "/_plugins/_security/api/rolesmapping/" + roleName);
    mappingRequest.setJsonEntity(
        String.format(
            Locale.ROOT,
            "{"
                + "\"backend_roles\": [],"
                + "\"hosts\": [],"
                + "\"users\": [\"%s\"]"
                + "}",
            username));

    mappingRequest.setOptions(restOptionsBuilder);

    Response mappingResponse = client().performRequest(mappingRequest);
    // Role mapping returns 201 (Created) for new mappings or 200 (OK) for updates
    assertTrue(
        mappingResponse.getStatusLine().getStatusCode() == 200
            || mappingResponse.getStatusLine().getStatusCode() == 201);
  }

  /** Creates roles with missing permissions for negative testing. */
  private void createRoleWithMissingPermissions() throws IOException {
    // Role missing PPL cluster permission
    createRoleWithSpecificPermissions(
        NO_PPL_ROLE,
        TEST_INDEX_BANK,
        new String[] {}, // No cluster permissions
        new String[] {
          "indices:data/read/search*",
          "indices:admin/mappings/get",
          "indices:monitor/settings/get",
          "indices:data/read/point_in_time/create",
          "indices:data/read/point_in_time/delete"
        });
    createUser(NO_PPL_USER, NO_PPL_ROLE);

    // Role missing search permissions
    createRoleWithSpecificPermissions(
        NO_SEARCH_ROLE,
        TEST_INDEX_BANK,
        new String[] {"cluster:admin/opensearch/ppl"},
        new String[] {
          "indices:admin/mappings/get",
          "indices:monitor/settings/get",
          "indices:data/read/point_in_time/create",
          "indices:data/read/point_in_time/delete"
        });
    createUser(NO_SEARCH_USER, NO_SEARCH_ROLE);

    // Role missing mapping permissions
    createRoleWithSpecificPermissions(
        NO_MAPPING_ROLE,
        TEST_INDEX_BANK,
        new String[] {"cluster:admin/opensearch/ppl"},
        new String[] {
          "indices:data/read/search*",
          "indices:monitor/settings/get",
          "indices:data/read/point_in_time/create",
          "indices:data/read/point_in_time/delete"
        });
    createUser(NO_MAPPING_USER, NO_MAPPING_ROLE);

    // Role missing settings permissions
    createRoleWithSpecificPermissions(
        NO_SETTINGS_ROLE,
        TEST_INDEX_BANK,
        new String[] {"cluster:admin/opensearch/ppl"},
        new String[] {
          "indices:data/read/search*",
          "indices:admin/mappings/get",
          "indices:data/read/point_in_time/create",
          "indices:data/read/point_in_time/delete"
        });
    createUser(NO_SETTINGS_USER, NO_SETTINGS_ROLE);
  }

  /** Creates a role with specific permissions for testing. */
  private void createRoleWithSpecificPermissions(
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
            "{"
                + "\"cluster_permissions\": [%s],"
                + "\"index_permissions\": [{"
                + "\"index_patterns\": [\"%s\"],"
                + "\"allowed_actions\": [%s]"
                + "}]"
                + "}",
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

  /** Creates a user with minimal permissions for testing plugin-based PIT functionality. */
  private void createMinimalUserForPitTesting() throws IOException {
    // Create role with minimal permissions needed for plugin-based PIT testing
    // This role has all required permissions (PPL, search, mapping, settings, and PIT)
    createRoleWithSpecificPermissions(
        MINIMAL_ROLE,
        TEST_INDEX_BANK,
        new String[] {"cluster:admin/opensearch/ppl"}, // PPL permission
        new String[] {
          "indices:data/read/search*", // Search permissions
          "indices:admin/mappings/get", // Mapping permissions
          "indices:monitor/settings/get", // Settings permissions
          "indices:data/read/point_in_time/create", // PIT create permissions
          "indices:data/read/point_in_time/delete" // PIT delete permissions
        });
    createUser(MINIMAL_USER, MINIMAL_ROLE);
  }

  /** Creates a user without PIT permissions to test PIT requirement. */
  private void createNoPitUserForTesting() throws IOException {
    // Create role with all permissions EXCEPT PIT create/delete permissions
    // This role has PPL, search, mapping, settings permissions but NO PIT permissions
    createRoleWithSpecificPermissions(
        NO_PIT_ROLE,
        TEST_INDEX_BANK,
        new String[] {"cluster:admin/opensearch/ppl"}, // PPL permission
        new String[] {
          "indices:data/read/search*", // Search permissions
          "indices:admin/mappings/get", // Mapping permissions
          "indices:monitor/settings/get" // Settings permissions
          // Note: NO PIT permissions (indices:data/read/point_in_time/create,
          // indices:data/read/point_in_time/delete)
        });
    createUser(NO_PIT_USER, NO_PIT_ROLE);
  }

  /** Executes a PPL query as a specific user with basic authentication. */
  private JSONObject executeQueryAsUser(String query, String username) throws IOException {
    Request request = new Request("POST", "/_plugins/_ppl");
    request.setJsonEntity(String.format(Locale.ROOT, "{\n" + "  \"query\": \"%s\"\n" + "}", query));

    RequestOptions.Builder restOptionsBuilder = RequestOptions.DEFAULT.toBuilder();
    restOptionsBuilder.addHeader("Content-Type", "application/json");
    restOptionsBuilder.addHeader(
        "Authorization",
        "Basic "
            + java.util.Base64.getEncoder()
                .encodeToString((username + ":" + STRONG_PASSWORD).getBytes()));
    request.setOptions(restOptionsBuilder);

    Response response = client().performRequest(request);
    assertEquals(200, response.getStatusLine().getStatusCode());
    return new JSONObject(org.opensearch.sql.legacy.TestUtils.getResponseBody(response, true));
  }

  @Test
  public void testUserWithBankPermissionCanAccessBankIndex() throws IOException {
    // Test that bank_user can access bank index - this should work with the fix
    JSONObject result =
        executeQueryAsUser(
            String.format("search source=%s | fields firstname", TEST_INDEX_BANK), BANK_USER);
    verifyColumn(result, columnName("firstname"));

    // Verify we get expected data from the bank index
    JSONObject resultWithFilter =
        executeQueryAsUser(
            String.format(
                "search source=%s firstname='Hattie' | fields firstname", TEST_INDEX_BANK),
            BANK_USER);
    verifyDataRows(resultWithFilter, rows("Hattie"));
  }

  @Test
  public void testUserWithDogPermissionCanAccessDogIndex() throws IOException {
    // Test that dog_user can access dog index - this should work with the fix
    JSONObject result =
        executeQueryAsUser(
            String.format("search source=%s | fields dog_name", TEST_INDEX_DOG), DOG_USER);
    verifyColumn(result, columnName("dog_name"));
  }

  @Test
  public void testUserWithBankPermissionCannotAccessDogIndex() throws IOException {
    // Test that bank_user cannot access dog index - this should fail
    try {
      executeQueryAsUser(
          String.format("search source=%s | fields dog_name", TEST_INDEX_DOG), BANK_USER);
      fail("Expected security exception when accessing unauthorized index");
    } catch (ResponseException e) {
      // This is expected - user should not be able to access the dog index
      String responseBody =
          org.opensearch.sql.legacy.TestUtils.getResponseBody(e.getResponse(), false);
      assertTrue(
          "Response should contain permission error message",
          responseBody.contains("no permissions") || responseBody.contains("Forbidden"));
    }
  }

  @Test
  public void testUserWithDogPermissionCannotAccessBankIndex() throws IOException {
    // Test that dog_user cannot access bank index - this should fail
    try {
      executeQueryAsUser(
          String.format("search source=%s | fields firstname", TEST_INDEX_BANK), DOG_USER);
      fail("Expected security exception when accessing unauthorized index");
    } catch (ResponseException e) {
      // This is expected - user should not be able to access the dog index
      String responseBody =
          org.opensearch.sql.legacy.TestUtils.getResponseBody(e.getResponse(), false);
      assertTrue(
          "Response should contain permission error message",
          responseBody.contains("no permissions") || responseBody.contains("Forbidden"));
    }
  }

  @Test
  public void testBankUserWithMultipleFieldsFromBankIndex() throws IOException {
    // Test that bank_user can query multiple fields from bank index
    JSONObject result =
        executeQueryAsUser(
            String.format("search source=%s | fields firstname, lastname, age", TEST_INDEX_BANK),
            BANK_USER);
    verifyColumn(result, columnName("firstname"), columnName("lastname"), columnName("age"));
  }

  @Test
  public void testBankUserWithWhereClauseOnBankIndex() throws IOException {
    // Test PPL search with WHERE clause on bank index by bank_user
    JSONObject result =
        executeQueryAsUser(
            String.format(
                "search source=%s | where age > 30 | fields firstname, age", TEST_INDEX_BANK),
            BANK_USER);
    verifyColumn(result, columnName("firstname"), columnName("age"));
  }

  @Test
  public void testBankUserWithStatsOnBankIndex() throws IOException {
    // Test PPL search with stats aggregation on bank index by bank_user
    JSONObject result =
        executeQueryAsUser(
            String.format("search source=%s | stats count() by gender", TEST_INDEX_BANK),
            BANK_USER);
    verifyColumn(result, columnName("gender"), columnName("count()"));
  }

  @Test
  public void testBankUserWithSortOnBankIndex() throws IOException {
    // Test PPL search with sort on bank index by bank_user
    JSONObject result =
        executeQueryAsUser(
            String.format(
                "search source=%s | sort age | fields firstname, age | head 5", TEST_INDEX_BANK),
            BANK_USER);
    verifyColumn(result, columnName("firstname"), columnName("age"));
  }

  @Test
  public void testBankUserCanDescribeBankIndex() throws IOException {
    // Test PPL describe command on bank index by bank_user
    JSONObject result =
        executeQueryAsUser(String.format("describe %s", TEST_INDEX_BANK), BANK_USER);
    verifyColumn(
        result,
        columnName("TABLE_CAT"),
        columnName("TABLE_SCHEM"),
        columnName("TABLE_NAME"),
        columnName("COLUMN_NAME"),
        columnName("DATA_TYPE"),
        columnName("TYPE_NAME"),
        columnName("COLUMN_SIZE"),
        columnName("BUFFER_LENGTH"),
        columnName("DECIMAL_DIGITS"),
        columnName("NUM_PREC_RADIX"),
        columnName("NULLABLE"),
        columnName("REMARKS"),
        columnName("COLUMN_DEF"),
        columnName("SQL_DATA_TYPE"),
        columnName("SQL_DATETIME_SUB"),
        columnName("CHAR_OCTET_LENGTH"),
        columnName("ORDINAL_POSITION"),
        columnName("IS_NULLABLE"),
        columnName("SCOPE_CATALOG"),
        columnName("SCOPE_SCHEMA"),
        columnName("SCOPE_TABLE"),
        columnName("SOURCE_DATA_TYPE"),
        columnName("IS_AUTOINCREMENT"),
        columnName("IS_GENERATEDCOLUMN"));
  }

  @Test
  public void testBankUserWithComplexQuery() throws IOException {
    // Test a more complex PPL query to ensure the fix works with various operations
    JSONObject result =
        executeQueryAsUser(
            String.format(
                "search source=%s | where age > 25 AND gender = 'M' | stats avg(age) as avg_age,"
                    + " count() as total_count by state | sort total_count | head 3",
                TEST_INDEX_BANK),
            BANK_USER);
    verifyColumn(result, columnName("state"), columnName("avg_age"), columnName("total_count"));
  }

  @Test
  public void testBankUserWithRenameCommand() throws IOException {
    // Test PPL search with rename command by bank_user
    JSONObject result =
        executeQueryAsUser(
            String.format(
                "search source=%s | rename firstname as first_name | fields first_name",
                TEST_INDEX_BANK),
            BANK_USER);
    verifyColumn(result, columnName("first_name"));
  }

  @Test
  public void testBankUserWithEvalCommand() throws IOException {
    // Test PPL search with eval command by bank_user
    JSONObject result =
        executeQueryAsUser(
            String.format(
                "search source=%s | eval full_name = concat(firstname, ' ', lastname) | fields"
                    + " full_name | head 5",
                TEST_INDEX_BANK),
            BANK_USER);
    verifyColumn(result, columnName("full_name"));
  }

  // Negative test cases for missing permissions

  @Test
  public void testUserWithoutPPLPermissionCannotExecutePPLQuery() throws IOException {
    // Test that user without PPL cluster permission gets 403 error
    try {
      executeQueryAsUser(
          String.format("search source=%s | fields firstname", TEST_INDEX_BANK), NO_PPL_USER);
      fail("Expected security exception for user without PPL permission");
    } catch (ResponseException e) {
      assertEquals(403, e.getResponse().getStatusLine().getStatusCode());
      String responseBody =
          org.opensearch.sql.legacy.TestUtils.getResponseBody(e.getResponse(), false);
      assertTrue(
          "Response should contain permission error message",
          responseBody.contains("no permissions")
              || responseBody.contains("Forbidden")
              || responseBody.contains("cluster:admin/opensearch/ppl"));
    }
  }

  @Test
  public void testUserWithoutSearchPermissionCannotSearchIndex() throws IOException {
    // Test that user without search permission gets 403 error
    try {
      executeQueryAsUser(
          String.format("search source=%s | fields firstname", TEST_INDEX_BANK), NO_SEARCH_USER);
      fail("Expected security exception for user without search permission");
    } catch (ResponseException e) {
      assertEquals(403, e.getResponse().getStatusLine().getStatusCode());
      String responseBody =
          org.opensearch.sql.legacy.TestUtils.getResponseBody(e.getResponse(), false);
      assertTrue(
          "Response should contain search permission error message",
          responseBody.contains("no permissions")
              || responseBody.contains("Forbidden")
              || responseBody.contains("indices:data/read/search"));
    }
  }

  @Test
  public void testUserWithoutMappingPermissionCannotGetFieldMappings() throws IOException {
    // Test that user without mapping permission gets 403 error
    try {
      executeQueryAsUser(String.format("describe %s", TEST_INDEX_BANK), NO_MAPPING_USER);
      fail("Expected security exception for user without mapping permission");
    } catch (ResponseException e) {
      assertEquals(403, e.getResponse().getStatusLine().getStatusCode());
      String responseBody =
          org.opensearch.sql.legacy.TestUtils.getResponseBody(e.getResponse(), false);
      assertTrue(
          "Response should contain mapping permission error message",
          responseBody.contains("no permissions")
              || responseBody.contains("Forbidden")
              || responseBody.contains("indices:admin/mappings/get"));
    }
  }

  @Test
  public void testUserWithoutSettingsPermissionCannotGetSettings() throws IOException {
    // Test that user without settings permission gets 403 error
    try {
      executeQueryAsUser(
          String.format("search source=%s | fields firstname", TEST_INDEX_BANK), NO_SETTINGS_USER);
      fail("Expected security exception for user without settings permission");
    } catch (ResponseException e) {
      assertEquals(403, e.getResponse().getStatusLine().getStatusCode());
      String responseBody =
          org.opensearch.sql.legacy.TestUtils.getResponseBody(e.getResponse(), false);
      assertTrue(
          "Response should contain settings permission error message",
          responseBody.contains("no permissions")
              || responseBody.contains("Forbidden")
              || responseBody.contains("indices:monitor/settings/get"));
    }
  }

  @Test
  public void testPluginBasedPITWorksWithMinimalUserPermissions() throws IOException {
    // Test that users with minimal permissions (including PIT create/delete) can search
    // with queries that require PIT functionality
    // Using MINIMAL_USER who has all required permissions including PIT permissions

    // 1. Query with filter - should work with plugin-based PIT
    JSONObject result1 =
        executeQueryAsUser(
            String.format(
                "search source=%s | where age > 25 | fields firstname, age", TEST_INDEX_BANK),
            MINIMAL_USER);
    verifyColumn(result1, columnName("firstname"), columnName("age"));

    // 2. Query with aggregation and filter - should work with plugin-based PIT
    JSONObject result2 =
        executeQueryAsUser(
            String.format(
                "search source=%s | where gender = 'M' | stats count() by state", TEST_INDEX_BANK),
            MINIMAL_USER);
    verifyColumn(result2, columnName("state"), columnName("count()"));

    // 3. Query with sort and limit (pagination-like) - should work with plugin-based PIT
    JSONObject result3 =
        executeQueryAsUser(
            String.format(
                "search source=%s | sort age | fields firstname | head 100", TEST_INDEX_BANK),
            MINIMAL_USER);
    verifyColumn(result3, columnName("firstname"));

    // 4. Complex query with multiple operations - should work with plugin-based PIT
    JSONObject result4 =
        executeQueryAsUser(
            String.format(
                "search source=%s | where age > 30 | stats avg(age) as avg_age by gender | sort"
                    + " avg_age",
                TEST_INDEX_BANK),
            MINIMAL_USER);
    verifyColumn(result4, columnName("gender"), columnName("avg_age"));
  }

  @Test
  public void testMultiplePermissionsMissingShowsRelevantError() throws IOException {
    // Test that when multiple permissions are missing, the error is still clear
    try {
      executeQueryAsUser(String.format("describe %s", TEST_INDEX_BANK), NO_PPL_USER);
      fail("Expected security exception for user with multiple missing permissions");
    } catch (ResponseException e) {
      assertEquals(403, e.getResponse().getStatusLine().getStatusCode());
      String responseBody =
          org.opensearch.sql.legacy.TestUtils.getResponseBody(e.getResponse(), false);
      assertTrue(
          "Response should contain clear error message even with multiple missing permissions",
          responseBody.contains("no permissions")
              || responseBody.contains("Forbidden")
              || responseBody.contains("access denied"));
    }
  }

  // Tests for PIT permission requirements

  @Test
  public void testUserWithoutPITPermissionCannotExecuteQueriesThatRequirePIT() throws IOException {
    // Test queries that trigger PIT creation when user lacks PIT permissions

    // 1. Query with deep pagination that exceeds maxResultWindow should fail
    try {
      executeQueryAsUser(
          String.format("search source=%s | head 10 from 10000", TEST_INDEX_BANK), NO_PIT_USER);
      fail("Expected security exception for user without PIT permission on deep pagination query");
    } catch (ResponseException e) {
      assertEquals(403, e.getResponse().getStatusLine().getStatusCode());
      String responseBody =
          org.opensearch.sql.legacy.TestUtils.getResponseBody(e.getResponse(), false);
      assertTrue(
          "Response should contain PIT permission error message",
          responseBody.contains("no permissions")
              || responseBody.contains("Forbidden")
              || responseBody.contains("point_in_time")
              || responseBody.contains("indices:data/read/point_in_time/create"));
    }
  }

  @Test
  public void testUserWithoutPITPermissionCanExecuteSmallQueries() throws IOException {
    // Test that users without PIT permissions can still execute small queries that don't trigger
    // PIT

    // Small query that doesn't exceed maxResultWindow - should work
    JSONObject result =
        executeQueryAsUser(
            String.format("search source=%s | head 10 | fields firstname", TEST_INDEX_BANK),
            NO_PIT_USER);
    verifyColumn(result, columnName("firstname"));

    // Aggregation query - should work (aggregations don't use PIT)
    JSONObject aggResult =
        executeQueryAsUser(
            String.format("search source=%s | stats count() by gender", TEST_INDEX_BANK),
            NO_PIT_USER);
    verifyColumn(aggResult, columnName("gender"), columnName("count()"));
  }

  @Test
  public void testUserWithPITPermissionCanExecuteLargeQueries() throws IOException {
    // Verify that users with PIT permissions can execute large queries

    // Query with deep pagination that exceeds maxResultWindow - should work with MINIMAL_USER who
    // has PIT permissions
    JSONObject result =
        executeQueryAsUser(
            String.format(
                "search source=%s | head 10 from 10000 | fields firstname", TEST_INDEX_BANK),
            MINIMAL_USER);
    verifyColumn(result, columnName("firstname"));
  }

  @Test
  public void testPITPermissionErrorMessageIsInformative() throws IOException {
    // Test that error messages clearly indicate PIT permission issues

    try {
      executeQueryAsUser(
          String.format("search source=%s | sort firstname | head 5 from 9999", TEST_INDEX_BANK),
          NO_PIT_USER);
      fail("Expected security exception for user without PIT permission");
    } catch (ResponseException e) {
      assertEquals(403, e.getResponse().getStatusLine().getStatusCode());
      String responseBody =
          org.opensearch.sql.legacy.TestUtils.getResponseBody(e.getResponse(), false);

      assertTrue(
          "Response should contain clear PIT permission error message. Actual response: "
              + responseBody,
          responseBody.contains("no permissions")
              || responseBody.contains("Forbidden")
              || responseBody.contains("access denied")
              || responseBody.contains("point_in_time")
              || responseBody.contains("indices:data/read/point_in_time/create"));
    }
  }

  @Test
  public void testUserWithoutPITPermissionCannotExecutePageSizeQueries() throws IOException {
    // Test that queries with pageSize set (which always trigger PIT) fail without PIT permissions
    // Note: This test assumes there's a way to trigger page size queries, which may require
    // specific query patterns or configurations in the test environment

    // Alternative: Test other patterns that might trigger PIT
    try {
      executeQueryAsUser(
          String.format("search source=%s | head 2 from 9999", TEST_INDEX_BANK), NO_PIT_USER);
      fail("Expected security exception for user without PIT permission on paginated query");
    } catch (ResponseException e) {
      assertEquals(403, e.getResponse().getStatusLine().getStatusCode());
      String responseBody =
          org.opensearch.sql.legacy.TestUtils.getResponseBody(e.getResponse(), false);

      assertTrue(
          "Response should contain PIT permission error message. Actual response: " + responseBody,
          responseBody.contains("no permissions")
              || responseBody.contains("Forbidden")
              || responseBody.contains("access denied")
              || responseBody.contains("point_in_time")
              || responseBody.contains("indices:data/read/point_in_time/create"));
    }
  }
}
