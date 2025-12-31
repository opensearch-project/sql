/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.security;

import static org.opensearch.sql.util.MatcherUtils.columnName;
import static org.opensearch.sql.util.MatcherUtils.verifyColumn;

import java.io.IOException;
import java.util.Locale;
import lombok.SneakyThrows;
import org.jetbrains.annotations.NotNull;
import org.json.JSONObject;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestInstance.Lifecycle;
import org.opensearch.client.Request;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.Response;
import org.opensearch.client.ResponseException;

/**
 * Integration tests for Fine-Grained Access Control (FGAC) across indices.
 *
 * <p>These tests verify all three levels of access control: 1. Index-level: Can users access the
 * index? 2. Column-level (Field-level): Can users see specific fields? 3. Row-level
 * (Document-level): Can users see specific documents?
 */
@TestInstance(Lifecycle.PER_CLASS)
public class FGACIndexScanningIT extends SecurityTestBase {
  private static final String PUBLIC_USER = "public_user";
  private static final String PUBLIC_ROLE = "public_role";
  private static final String LIMITED_USER = "limited_user";
  private static final String LIMITED_ROLE = "limited_role";
  private static final String SENSITIVE_USER = "sensitive_user";
  private static final String SENSITIVE_ROLE = "sensitive_role";
  private static final String MANAGER_USER = "manager_user";
  private static final String MANAGER_ROLE = "manager_role";
  private static final String HR_USER = "hr_user";
  private static final String HR_ROLE = "hr_role";
  private static final String[] RECORDS_INDEX_COLUMNS = {
    "name", "department", "salary", "email", "employee_id"
  };

  // Indices for testing
  private static final String PUBLIC_LOGS = "public_logs_fgac";
  private static final String SENSITIVE_LOGS = "sensitive_logs_fgac";
  private static final String SECURE_LOGS = "secure_logs_fgac";
  private static final String EMPLOYEE_RECORDS = "employee_records_fgac";

  private static final int LARGE_DATASET_SIZE = 2000;

  @SneakyThrows
  @BeforeAll
  public void initialize() {
    setUpIndices();
    setupTestIndices();
    createSecurityRolesAndUsers();
  }

  @Override
  protected void init() throws Exception {
    super.init();
    enableCalcite();
    allowCalciteFallback();
  }

  private void setupTestIndices() throws IOException {
    createPublicLogsIndex();
    createSensitiveLogsIndex();
    createEmployeeRecordsIndex();
    createSecureLogsIndex();
  }

  /** Creates public_logs index with 2000+ documents. */
  private void createPublicLogsIndex() throws IOException {
    Request request = new Request("PUT", "/" + PUBLIC_LOGS);
    request.setJsonEntity(
        """
        {
          "settings": {
            "number_of_shards": 1,
            "number_of_replicas": 0
          },
          "mappings": {
            "properties": {
              "message": { "type": "text" },
              "level": { "type": "keyword" },
              "timestamp": { "type": "date" }
            }
          }
        }
        """);
    client().performRequest(request);

    bulkInsertDocs(PUBLIC_LOGS, "public");
  }

  /** Creates sensitive_logs index with 2000+ documents. */
  private void createSensitiveLogsIndex() throws IOException {
    Request request = new Request("PUT", "/" + SENSITIVE_LOGS);
    request.setJsonEntity(
        """
        {
          "settings": {
            "number_of_shards": 1,
            "number_of_replicas": 0
          },
          "mappings": {
            "properties": {
              "message": { "type": "text" },
              "level": { "type": "keyword" },
              "timestamp": { "type": "date" }
            }
          }
        }
        """);
    client().performRequest(request);

    bulkInsertDocs(SENSITIVE_LOGS, "sensitive");
  }

  /**
   * Creates employee_records index with sensitive fields for field-level security testing. Contains
   * fields: employee_id, name, department, salary, ssn
   */
  private void createEmployeeRecordsIndex() throws IOException {
    Request request = new Request("PUT", "/" + EMPLOYEE_RECORDS);
    request.setJsonEntity(
        """
        {
          "settings": {
            "number_of_shards": 1,
            "number_of_replicas": 0
          },
          "mappings": {
            "properties": {
              "employee_id": { "type": "keyword" },
              "name": { "type": "text" },
              "department": { "type": "keyword" },
              "salary": { "type": "integer" },
              "ssn": { "type": "keyword" },
              "email": { "type": "keyword" }
            }
          }
        }
        """);
    client().performRequest(request);

    bulkInsertEmployeeRecords();
  }

  /**
   * Creates secure_logs index with mixed security levels. This index contains documents with
   * different security_level values to test row-level filtering.
   */
  private void createSecureLogsIndex() throws IOException {
    Request request = new Request("PUT", "/" + SECURE_LOGS);
    request.setJsonEntity(
        """
        {
          "settings": {
            "number_of_shards": 1,
            "number_of_replicas": 0
          },
          "mappings": {
            "properties": {
              "message": { "type": "text" },
              "security_level": { "type": "keyword" },
              "timestamp": { "type": "date" }
            }
          }
        }
        """);
    client().performRequest(request);

    // Insert documents with mixed security levels
    // 1000 public, 500 internal, 500 confidential
    bulkInsertDocsWithSecurityLevel();
  }

  /** Bulk inserts documents to trigger background scanning. */
  private void bulkInsertDocs(String indexName, String prefix) throws IOException {
    StringBuilder bulk = new StringBuilder();
    for (int i = 0; i < FGACIndexScanningIT.LARGE_DATASET_SIZE; i++) {
      bulk.append(
          String.format(
              Locale.ROOT,
              """
              { "index": { "_index": "%s" } }
              { "message": "%s message %d", "level": "info", "timestamp": "2025-01-01T00:00:00Z" }
              """,
              indexName,
              prefix,
              i));
    }

    Request request = new Request("POST", "/_bulk");
    request.addParameter("refresh", "true");
    request.setJsonEntity(bulk.toString());

    RequestOptions.Builder restOptionsBuilder = RequestOptions.DEFAULT.toBuilder();
    restOptionsBuilder.addHeader("Content-Type", "application/x-ndjson");
    request.setOptions(restOptionsBuilder);

    Response response = client().performRequest(request);
    assertEquals(200, response.getStatusLine().getStatusCode());
  }

  /** Bulk inserts employee records with sensitive fields for FLS testing. */
  private void bulkInsertEmployeeRecords() throws IOException {
    String bulk = getBulkEmployeeIndexRequest();

    Request request = new Request("POST", "/_bulk");
    request.addParameter("refresh", "true");
    request.setJsonEntity(bulk);

    RequestOptions.Builder restOptionsBuilder = RequestOptions.DEFAULT.toBuilder();
    restOptionsBuilder.addHeader("Content-Type", "application/x-ndjson");
    request.setOptions(restOptionsBuilder);

    Response response = client().performRequest(request);
    assertEquals(200, response.getStatusLine().getStatusCode());
  }

  @NotNull
  private static String getBulkEmployeeIndexRequest() {
    StringBuilder bulk = new StringBuilder();
    String[] departments = {"engineering", "finance", "hr", "sales", "marketing"};

    for (int i = 0; i < LARGE_DATASET_SIZE; i++) {
      String dept = departments[i % departments.length];
      bulk.append(
          String.format(
              Locale.ROOT,
              """
              { "index": { "_index": "%s" } }
              { "employee_id": "EMP%04d", "name": "Employee %d", "department": "%s", "salary": %d, "ssn": "XXX-XX-%04d", "email": "emp%d@company.com" }
              """,
              FGACIndexScanningIT.EMPLOYEE_RECORDS,
              i,
              i,
              dept,
              50000 + (i * 1000),
              i,
              i));
    }
    return bulk.toString();
  }

  /** Bulk inserts documents with different security levels for row-level testing. */
  private void bulkInsertDocsWithSecurityLevel() throws IOException {
    StringBuilder bulk = new StringBuilder();

    // 1000 public documents
    for (int i = 0; i < 1000; i++) {
      bulk.append(
          String.format(
              Locale.ROOT,
              """
              { "index": { "_index": "%s" } }
              { "message": "public message %d", "security_level": "public", "timestamp": "2025-01-01T00:00:00Z" }
              """,
              FGACIndexScanningIT.SECURE_LOGS,
              i));
    }

    // 500 internal documents
    for (int i = 1000; i < 1500; i++) {
      bulk.append(
          String.format(
              Locale.ROOT,
              """
              { "index": { "_index": "%s" } }
              { "message": "internal message %d", "security_level": "internal", "timestamp": "2025-01-01T00:00:00Z" }
              """,
              FGACIndexScanningIT.SECURE_LOGS,
              i));
    }

    // 500 confidential documents
    for (int i = 1500; i < 2000; i++) {
      bulk.append(
          String.format(
              Locale.ROOT,
              """
              { "index": { "_index": "%s" } }
              { "message": "confidential message %d", "security_level": "confidential", "timestamp": "2025-01-01T00:00:00Z" }
              """,
              FGACIndexScanningIT.SECURE_LOGS,
              i));
    }

    Request request = new Request("POST", "/_bulk");
    request.addParameter("refresh", "true");
    request.setJsonEntity(bulk.toString());

    RequestOptions.Builder restOptionsBuilder = RequestOptions.DEFAULT.toBuilder();
    restOptionsBuilder.addHeader("Content-Type", "application/x-ndjson");
    request.setOptions(restOptionsBuilder);

    Response response = client().performRequest(request);
    assertEquals(200, response.getStatusLine().getStatusCode());
  }

  /** Creates security roles and users for testing. */
  private void createSecurityRolesAndUsers() throws IOException {
    // Role for public_user: can only access PUBLIC_LOGS
    createRoleWithIndexAccess(PUBLIC_ROLE, PUBLIC_LOGS);
    createUser(PUBLIC_USER, PUBLIC_ROLE);

    // Role for sensitive_user: can only access SENSITIVE_LOGS
    createRoleWithIndexAccess(SENSITIVE_ROLE, SENSITIVE_LOGS);
    createUser(SENSITIVE_USER, SENSITIVE_ROLE);

    // Role for limited_user: can access SECURE_LOGS but with document-level filtering
    // Only allow documents with security_level="public"
    createRoleWithDocumentLevelSecurity();
    createUser(LIMITED_USER, LIMITED_ROLE);

    // Roles for Scenario 2: Field-level security
    // manager_user: can see name, department, salary, email BUT NOT ssn
    createRoleWithFieldLevelSecurity();
    createUser(MANAGER_USER, MANAGER_ROLE);

    // hr_user: can see ALL fields including ssn
    createRoleWithIndexAccess(HR_ROLE, EMPLOYEE_RECORDS);
    createUser(HR_USER, HR_ROLE);
  }

  /**
   * Creates a role with document-level security (DLS) - only documents matching the query are
   * visible.
   */
  private void createRoleWithDocumentLevelSecurity() throws IOException {
    createRoleWithDLS(
        LIMITED_ROLE, SECURE_LOGS, "{\\\"match\\\":{\\\"security_level\\\":\\\"public\\\"}}");
  }

  /** Creates a role with field-level security (FLS) - only specific fields are accessible. */
  private void createRoleWithFieldLevelSecurity() throws IOException {
    createRoleWithFLS(MANAGER_ROLE, EMPLOYEE_RECORDS, RECORDS_INDEX_COLUMNS);
  }

  @Test
  public void testPublicUserCanAccessPublicLogs() throws IOException {
    // public_user can access public_logs (large dataset triggers background scanning)
    JSONObject result =
        executeQueryAsUser(
            String.format("search source=%s | fields message | head 10", PUBLIC_LOGS), PUBLIC_USER);
    verifyColumn(result, columnName("message"));
  }

  @Test
  public void testPublicUserCannotAccessSensitiveLogs() throws IOException {
    // public_user cannot access sensitive_logs (should fail at planning stage)
    try {
      executeQueryAsUser(
          String.format("search source=%s | fields message", SENSITIVE_LOGS), PUBLIC_USER);
      fail("Expected security exception when public_user accesses sensitive_logs");
    } catch (ResponseException e) {
      String responseBody =
          org.opensearch.sql.legacy.TestUtils.getResponseBody(e.getResponse(), false);
      assertTrue(
          "Response should contain permission error",
          responseBody.contains("no permissions") || responseBody.contains("Forbidden"));
    }
  }

  @Test
  public void testSensitiveUserCanAccessSensitiveLogs() throws IOException {
    // sensitive_user can access sensitive_logs
    JSONObject result =
        executeQueryAsUser(
            String.format("search source=%s | fields message | head 10", SENSITIVE_LOGS),
            SENSITIVE_USER);
    verifyColumn(result, columnName("message"));
  }

  @Test
  public void testSensitiveUserCannotAccessPublicLogs() throws IOException {
    // sensitive_user cannot access public_logs
    try {
      executeQueryAsUser(
          String.format("search source=%s | fields message", PUBLIC_LOGS), SENSITIVE_USER);
      fail("Expected security exception when sensitive_user accesses public_logs");
    } catch (ResponseException e) {
      String responseBody =
          org.opensearch.sql.legacy.TestUtils.getResponseBody(e.getResponse(), false);
      assertTrue(
          "Response should contain permission error",
          responseBody.contains("no permissions") || responseBody.contains("Forbidden"));
    }
  }

  @Test
  public void testHrUserCanSeeAllFieldsIncludingSensitiveData() throws IOException {
    // hr_user can see ALL fields including sensitive ssn
    String queryAllFields =
        String.format(
            "search source=%s | fields name, department, salary, ssn | head 10", EMPLOYEE_RECORDS);
    JSONObject hrResult = executeQueryAsUser(queryAllFields, HR_USER);

    var hrSchema = hrResult.getJSONArray("schema");
    boolean hrHasName = false, hrHasSalary = false, hrHasSSN = false, hrHasDepartment = false;

    for (int i = 0; i < hrSchema.length(); i++) {
      String fieldName = hrSchema.getJSONObject(i).getString("name");
      if ("name".equals(fieldName)) hrHasName = true;
      if ("salary".equals(fieldName)) hrHasSalary = true;
      if ("ssn".equals(fieldName)) hrHasSSN = true;
      if ("department".equals(fieldName)) hrHasDepartment = true;
    }

    assertTrue("hr_user should see 'name' field", hrHasName);
    assertTrue("hr_user should see 'salary' field", hrHasSalary);
    assertTrue("hr_user should see 'ssn' field (sensitive)", hrHasSSN);
    assertTrue("hr_user should see 'department' field", hrHasDepartment);
  }

  @Test
  public void testManagerUserCannotSeeSensitiveFields() throws IOException {
    // manager_user can see most fields but NOT ssn
    String queryAllowedFields =
        String.format(
            "search source=%s | fields name, department, salary | head 10", EMPLOYEE_RECORDS);
    JSONObject managerResult = executeQueryAsUser(queryAllowedFields, MANAGER_USER);

    var managerSchema = managerResult.getJSONArray("schema");
    boolean managerHasName = false,
        managerHasSalary = false,
        managerHasSSN = false,
        managerHasDepartment = false;

    for (int i = 0; i < managerSchema.length(); i++) {
      String fieldName = managerSchema.getJSONObject(i).getString("name");
      if ("name".equals(fieldName)) managerHasName = true;
      if ("salary".equals(fieldName)) managerHasSalary = true;
      if ("ssn".equals(fieldName)) managerHasSSN = true;
      if ("department".equals(fieldName)) managerHasDepartment = true;
    }

    assertTrue("manager_user should see 'name' field", managerHasName);
    assertTrue("manager_user should see 'salary' field", managerHasSalary);
    assertTrue("manager_user should see 'department' field", managerHasDepartment);
    assertFalse(
        "SECURITY VIOLATION: manager_user should NOT see 'ssn' field. "
            + "Field-level security should hide this sensitive field.",
        managerHasSSN);
  }

  @Test
  public void testManagerUserCannotQueryRestrictedField() throws IOException {
    // Verify manager_user cannot even reference ssn in query (field is invisible)
    try {
      String queryWithSSN =
          String.format("search source=%s | fields ssn | head 10", EMPLOYEE_RECORDS);
      executeQueryAsUser(queryWithSSN, MANAGER_USER);
      fail(
          "SECURITY VIOLATION: manager_user should NOT be able to query 'ssn' field. "
              + "Query should fail because field is invisible to this user.");
    } catch (ResponseException e) {
      String responseBody =
          org.opensearch.sql.legacy.TestUtils.getResponseBody(e.getResponse(), false);
      assertTrue(
          "Error should indicate field not found",
          responseBody.contains("Field [ssn] not found") || responseBody.contains("ssn"));
    }
  }

  @Test
  public void testFieldLevelSecurityEnforcedWithLargeDataset() throws IOException {
    // Verify with large result set that FLS is still enforced
    String queryLargeDataset =
        String.format(
            "search source=%s | fields name, salary, department | stats count()", EMPLOYEE_RECORDS);
    JSONObject managerLargeResult = executeQueryAsUser(queryLargeDataset, MANAGER_USER);

    // Even with large dataset, manager should not see ssn
    var largeSchema = managerLargeResult.getJSONArray("schema");
    boolean hasSSNInLarge = false;
    for (int i = 0; i < largeSchema.length(); i++) {
      if ("ssn".equals(largeSchema.getJSONObject(i).getString("name"))) {
        hasSSNInLarge = true;
        break;
      }
    }

    assertFalse(
        "SECURITY VIOLATION: manager_user should NOT see 'ssn' even with large dataset (2000+"
            + " rows). Field-level security must be enforced.",
        hasSSNInLarge);
  }

  @Test
  public void testRowLevelSecurityV2() throws IOException {
    // Test V2 (legacy) engine explicitly
    disableCalcite();

    // limited_user should only see "public" documents

    // Execute query as limited_user
    String query =
        String.format(
            "search source=%s | fields security_level, message | stats count() by security_level",
            SECURE_LOGS);
    JSONObject result = executeQueryAsUser(query, LIMITED_USER);

    // Extract the datarows for validation
    var datarows = result.getJSONArray("datarows");

    // Count total documents visible
    int totalDocs = 0;
    boolean sawConfidential = false;
    boolean sawInternal = false;
    int publicDocs = 0;

    for (int i = 0; i < datarows.length(); i++) {
      var row = datarows.getJSONArray(i);
      int count = row.getInt(0);
      String securityLevel = row.getString(1);
      totalDocs += count;

      if ("confidential".equals(securityLevel)) {
        sawConfidential = true;
      } else if ("internal".equals(securityLevel)) {
        sawInternal = true;
      } else if ("public".equals(securityLevel)) {
        publicDocs = count;
      }
    }

    assertFalse(
        "[V2] SECURITY VIOLATION: limited_user should NOT see 'confidential' documents. "
            + "This indicates ThreadContext is not being copied to async worker threads in V2, "
            + "causing queries to run with admin permissions and bypass row-level security.",
        sawConfidential);

    assertFalse(
        "[V2] SECURITY VIOLATION: limited_user should NOT see 'internal' documents. "
            + "This indicates ThreadContext is not being copied to async worker threads in V2, "
            + "causing queries to run with admin permissions and bypass row-level security.",
        sawInternal);

    assertEquals(
        "[V2] limited_user should ONLY see 'public' documents (~1000). "
            + "Seeing more indicates row-level security is being bypassed in V2.",
        1000,
        publicDocs);

    assertEquals(
        "[V2] Total visible documents should be ~1000 (only public). "
            + "Seeing 2000 documents indicates row-level security is completely bypassed in V2.",
        1000,
        totalDocs);
  }

  @Test
  public void testRowLevelSecurity() throws IOException {
    // Test V3 (Calcite) engine - Calcite is enabled in init()
    // limited_user should only see "public" documents

    // Execute query as limited_user
    String query =
        String.format(
            "search source=%s | fields security_level, message | stats count() by security_level",
            SECURE_LOGS);
    JSONObject result = executeQueryAsUser(query, LIMITED_USER);

    // Extract the datarows for validation
    var datarows = result.getJSONArray("datarows");

    // limited_user should ONLY see "public" documents
    // Note: Without DLS configured in Security Plugin, all documents are visible
    // Once DLS is configured with a rule like: { "match": { "security_level": "public" } }
    // Then with the ThreadContext fix, this test should pass

    // Count total documents visible
    int totalDocs = 0;
    boolean sawConfidential = false;
    boolean sawInternal = false;
    int publicDocs = 0;

    for (int i = 0; i < datarows.length(); i++) {
      var row = datarows.getJSONArray(i);
      int count = row.getInt(0);
      String securityLevel = row.getString(1);
      totalDocs += count;

      if ("confidential".equals(securityLevel)) {
        sawConfidential = true;
      } else if ("internal".equals(securityLevel)) {
        sawInternal = true;
      } else if ("public".equals(securityLevel)) {
        publicDocs = count;
      }
    }

    assertFalse(
        "[V3] SECURITY VIOLATION: limited_user should NOT see 'confidential' documents. "
            + "This indicates ThreadContext is not being properly copied to search threads in V3, "
            + "causing queries to run with admin permissions and bypass row-level security.",
        sawConfidential);

    assertFalse(
        "[V3] SECURITY VIOLATION: limited_user should NOT see 'internal' documents. "
            + "This indicates ThreadContext is not being properly copied to search threads in V3, "
            + "causing queries to run with admin permissions and bypass row-level security.",
        sawInternal);

    assertEquals(
        "[V3] limited_user should ONLY see 'public' documents (~1000). "
            + "Seeing more indicates row-level security is being bypassed in V3.",
        1000,
        publicDocs);

    assertEquals(
        "[V3] Total visible documents should be ~1000 (only public). "
            + "Seeing 2000 documents indicates row-level security is completely bypassed in V3.",
        1000,
        totalDocs);
  }
}
