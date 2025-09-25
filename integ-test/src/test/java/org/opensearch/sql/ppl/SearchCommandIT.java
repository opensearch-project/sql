/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ppl;

import static org.opensearch.sql.legacy.TestsConstants.*;
import static org.opensearch.sql.util.MatcherUtils.columnName;
import static org.opensearch.sql.util.MatcherUtils.rows;
import static org.opensearch.sql.util.MatcherUtils.schema;
import static org.opensearch.sql.util.MatcherUtils.verifyColumn;
import static org.opensearch.sql.util.MatcherUtils.verifyDataRows;
import static org.opensearch.sql.util.MatcherUtils.verifyNumOfRows;
import static org.opensearch.sql.util.MatcherUtils.verifySchema;

import java.io.IOException;
import org.json.JSONObject;
import org.junit.jupiter.api.Test;
import org.opensearch.client.Request;
import org.opensearch.client.ResponseException;

public class SearchCommandIT extends PPLIntegTestCase {

  @Override
  public void init() throws Exception {
    super.init();
    loadIndex(Index.BANK);
    loadIndex(Index.DOG);
    loadIndex(Index.OTELLOGS);
    loadIndex(Index.TIME_TEST_DATA);
  }

  @Test
  public void testSearchAllFields() throws IOException {
    JSONObject result = executeQuery(String.format("search source=%s", TEST_INDEX_DOG));
    verifyColumn(result, columnName("dog_name"), columnName("holdersName"), columnName("age"));
  }

  @Test
  public void testSearchCommandWithoutSearchKeyword() throws IOException {
    assertEquals(
        executeQueryToString(String.format("search source=%s", TEST_INDEX_BANK)),
        executeQueryToString(String.format("source=%s", TEST_INDEX_BANK)));
  }

  @Test
  public void testSearchCommandWithSpecialIndexName() throws IOException {
    executeRequest(new Request("PUT", "/logs-2021.01.11"));
    verifyDataRows(executeQuery("search source=logs-2021.01.11"));

    executeRequest(new Request("PUT", "/logs-7.10.0-2021.01.11"));
    verifyDataRows(executeQuery("search source=logs-7.10.0-2021.01.11"));
  }

  @Test
  public void testSearchCommandWithLogicalExpression() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "search source=%s firstname='Hattie' | fields firstname", TEST_INDEX_BANK));
    verifyDataRows(result, rows("Hattie"));
  }

  @Test
  public void searchCommandWithoutSourceShouldFailToParse() throws IOException {
    try {
      executeQuery("search firstname='Hattie'");
      fail();
    } catch (ResponseException e) {
      assertTrue(e.getMessage().contains("RuntimeException"));
      assertTrue(e.getMessage().contains(SYNTAX_EX_MSG_FRAGMENT));
    }
  }

  @Test
  public void testSearchWithFieldEquals() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "search source=%s `resource.attributes.service.name`=\\\"cart-service\\\" | fields"
                    + " body ",
                TEST_INDEX_OTEL_LOGS));
    verifyDataRows(
        result,
        rows("User e1ce63e6-8501-11f0-930d-c2fcbdc05f14 adding 4 of product HQTGWGPNH4 to cart"));
  }

  @Test
  public void testSearchWithFieldNotEquals() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "search source=%s severityText!=\\\"INFO\\\" | sort time | fields severityText |"
                    + " head 5",
                TEST_INDEX_OTEL_LOGS));
    verifyDataRows(
        result, rows("ERROR"), rows("WARN"), rows("DEBUG"), rows("FATAL"), rows("TRACE"));
  }

  @Test
  public void testSearchWithNumericComparison() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "search source=%s severityNumber>15 AND severityNumber<=20 | sort time | fields"
                    + " severityNumber, severityText",
                TEST_INDEX_OTEL_LOGS));
    verifyDataRows(
        result,
        rows(17, "ERROR"),
        rows(17, "ERROR"),
        rows(18, "ERROR2"),
        rows(19, "ERROR3"),
        rows(20, "ERROR4"),
        rows(16, "WARN4"));
  }

  // ===== Boolean Operator Tests =====

  @Test
  public void testSearchWithOROperator() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "search source=%s severityText=\\\"ERROR\\\" OR severityText=\\\"FATAL\\\" | sort"
                    + " time | fields severityText |  head 5",
                TEST_INDEX_OTEL_LOGS));
    verifyDataRows(result, rows("ERROR"), rows("FATAL"), rows("ERROR"));
  }

  @Test
  public void testSearchWithANDOperator() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "search source=%s severityText=\\\"INFO\\\" AND"
                    + " `resource.attributes.service.name`=\\\"cart-service\\\" | fields body",
                TEST_INDEX_OTEL_LOGS));
    verifyDataRows(
        result,
        rows("User e1ce63e6-8501-11f0-930d-c2fcbdc05f14 adding 4 of product HQTGWGPNH4 to cart"));
  }

  @Test
  public void testDifferenceBetweenNOTAndNotEquals() throws IOException {
    // Demonstrate the key difference between != and NOT:
    // != requires the field to exist and not equal the value
    // NOT field=value excludes docs where field equals value OR field doesn't exist

    JSONObject resultNotEquals =
        executeQuery(
            String.format(
                "search source=%s `attributes.http.status_code`!=200 | sort time | fields body |"
                    + " head 5",
                TEST_INDEX_OTEL_LOGS));
    // This should return documents where http.status_code exists but is not 200
    verifyDataRows(resultNotEquals);

    JSONObject resultNOT =
        executeQuery(
            String.format(
                "search source=%s NOT `attributes.http.status_code`=200 | sort time | fields body |"
                    + " head 5",
                TEST_INDEX_OTEL_LOGS));
    // This returns docs where http.status_code != 200 OR where http.status_code doesn't exist at
    // all
    verifyDataRows(
        resultNOT,
        rows("User e1ce63e6-8501-11f0-930d-c2fcbdc05f14 adding 4 of product HQTGWGPNH4 to cart"),
        rows("Payment failed: Insufficient funds for user@example.com"),
        rows(
            "Query contains Lucene special characters: +field:value -excluded AND (grouped OR"
                + " terms) NOT \"exact phrase\" wildcard* fuzzy~2 /regex/ [range TO search]"),
        rows(
            "Email notification sent to john.doe+newsletter@company.com with subject: 'Welcome!"
                + " Your order #12345 is confirmed'"),
        rows("Database connection pool exhausted: postgresql://db.example.com:5432/production"));

    // Test 3: Demonstrate with another optional field - attributes.user.email
    // != only returns docs where user.email exists and is not the specified value
    JSONObject resultEmailNotEquals =
        executeQuery(
            String.format(
                "search source=%s `attributes.user.email`!=\\\"user@example.com\\\" | sort time |"
                    + " fields body | head 2",
                TEST_INDEX_OTEL_LOGS));
    verifyDataRows(
        resultEmailNotEquals,
        rows(
            "[2024-01-15 10:30:09] production.INFO: User authentication successful for"
                + " admin@company.org using OAuth2"),
        rows(
            "Redis command: SETEX user:session:abc123 3600"
                + " {\"user_id\":\"456\",\"email\":\"alice@wonderland.net\"}"));

    // NOT returns all docs except where user.email="user@example.com"
    // Including docs that don't have user.email field at all
    JSONObject resultEmailNOT =
        executeQuery(
            String.format(
                "search source=%s NOT `attributes.user.email`=\\\"user@example.com\\\" | sort time"
                    + " | fields body | head 5",
                TEST_INDEX_OTEL_LOGS));
    verifyDataRows(
        resultEmailNOT,
        rows("User e1ce63e6-8501-11f0-930d-c2fcbdc05f14 adding 4 of product HQTGWGPNH4 to cart"),
        rows(
            "Query contains Lucene special characters: +field:value -excluded AND (grouped OR"
                + " terms) NOT \"exact phrase\" wildcard* fuzzy~2 /regex/ [range TO search]"),
        rows(
            "192.168.1.1 - - [15/Jan/2024:10:30:03 +0000] \"GET"
                + " /api/products?search=laptop&category=electronics HTTP/1.1\" 200 1234 \"-\""
                + " \"Mozilla/5.0\""),
        rows(
            "Email notification sent to john.doe+newsletter@company.com with subject: 'Welcome!"
                + " Your order #12345 is confirmed'"),
        rows("Database connection pool exhausted: postgresql://db.example.com:5432/production"));
  }

  @Test
  public void testSearchWithComplexBooleanExpression() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "search source=%s (severityText=\\\"ERROR\\\" OR severityText=\\\"WARN\\\") AND"
                    + " severityNumber>10 | sort time | fields severityText, severityNumber | head"
                    + " 5",
                TEST_INDEX_OTEL_LOGS));
    verifyDataRows(
        result, rows("ERROR", 17), rows("WARN", 13), rows("ERROR", 17), rows("WARN", 13));
  }

  @Test
  public void testSearchWithNestedParentheses() throws IOException {
    JSONObject resultWithParentheses =
        executeQuery(
            String.format(
                "search source=%s ((severityNumber<15 AND severityNumber>5) OR (severityNumber>20))"
                    + " | sort time | fields severityNumber | head 5",
                TEST_INDEX_OTEL_LOGS));
    verifyDataRows(resultWithParentheses, rows(9), rows(13), rows(9), rows(21), rows(13));

    JSONObject resultWithoutParentheses =
        executeQuery(
            String.format(
                "search source=%s severityNumber<15 AND severityNumber>5 OR severityNumber>20"
                    + " | sort time | fields severityNumber | head 5",
                TEST_INDEX_OTEL_LOGS));
    verifyDataRows(resultWithoutParentheses, rows(9), rows(13), rows(9), rows(13), rows(9));

    JSONObject resultDifferentParentheses =
        executeQuery(
            String.format(
                "search source=%s severityNumber<15 AND (severityNumber>5 OR severityNumber>20)"
                    + " | sort time | fields severityNumber | head 5",
                TEST_INDEX_OTEL_LOGS));
    verifyDataRows(resultDifferentParentheses, rows(9), rows(13), rows(9), rows(13), rows(9));
  }

  // ===== IN Operator Tests =====

  @Test
  public void testSearchWithINOperator() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "search source=%s severityText IN (\\\"ERROR\\\", \\\"WARN\\\", \\\"FATAL\\\") |"
                    + " sort time | fields severityText | head 5",
                TEST_INDEX_OTEL_LOGS));
    verifyDataRows(result, rows("ERROR"), rows("WARN"), rows("FATAL"), rows("ERROR"), rows("WARN"));
  }

  // ===== Free Text Search Tests =====

  @Test
  public void testSearchWithFreeText() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "search source=%s \\\"@example.com\\\" | fields body | head 5",
                TEST_INDEX_OTEL_LOGS));
    verifyDataRows(
        result,
        rows("Payment failed: Insufficient funds for user@example.com"),
        rows(
            "Elasticsearch query failed:"
                + " {\"query\":{\"bool\":{\"must\":[{\"match\":{\"email\":\"*@example.com\"}}]}}}"));
  }

  @Test
  public void testSearchWithPhraseSearch() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "search source=%s \\\"Payment failed\\\" | fields body", TEST_INDEX_OTEL_LOGS));
    verifyDataRows(result, rows("Payment failed: Insufficient funds for user@example.com"));
  }

  @Test
  public void testSearchWithMultipleFreeTextTerms() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "search source=%s \\\"email\\\"  \\\"user\\\" | sort time | fields body",
                TEST_INDEX_OTEL_LOGS));
    verifyDataRows(
        result,
        rows(
            "GraphQL query: { user(email: \"support@helpdesk.io\") { id name orders { id total } }"
                + " }"),
        rows(
            "gRPC call: /UserService/GetUserByEmail {\"email\":\"grpc-user@service.net\"} completed"
                + " in 45ms"),
        rows(
            "Data corruption detected in user table: email column contains invalid data for"
                + " user_id=999"));
  }

  // ===== Edge Cases with Special Characters =====

  @Test
  public void testSearchWithEmailInBody() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "search source=%s \\\"john.doe+newsletter@company.com\\\" | fields body",
                TEST_INDEX_OTEL_LOGS));
    verifyDataRows(
        result,
        rows(
            "Email notification sent to john.doe+newsletter@company.com with subject: 'Welcome!"
                + " Your order #12345 is confirmed'"));
  }

  @Test
  public void testSearchWithLuceneSpecialCharacters() throws IOException {
    // Test that special characters in quoted strings are searched literally
    JSONObject result =
        executeQuery(
            String.format(
                "search source=%s \\\"wildcard* fuzzy~2\\\" | fields body", TEST_INDEX_OTEL_LOGS));
    verifyDataRows(
        result,
        rows(
            "Query contains Lucene special characters: +field:value -excluded AND (grouped OR"
                + " terms) NOT \"exact phrase\" wildcard* fuzzy~2 /regex/ [range TO search]"));
  }

  @Test
  public void testWildcardPatternMatching() throws IOException {
    // Test wildcard pattern matching on different field types
    // * matches zero or more characters
    // ? matches exactly one character

    // Test 1: Wildcard on keyword field (severityText)
    // Search for severity levels starting with "ERR"
    JSONObject keywordWildcard =
        executeQuery(
            String.format(
                "search source=%s severityText=ERR* | sort time | fields severityText, body | head"
                    + " 5",
                TEST_INDEX_OTEL_LOGS));
    verifyDataRows(
        keywordWildcard,
        rows("ERROR", "Payment failed: Insufficient funds for user@example.com"),
        rows(
            "ERROR",
            "Failed to parse JSON with special characters: {\"key\": \"value with \\\"quotes\\\""
                + " and: $@#%^&*()[]{}<>|\\/?\"}"),
        rows(
            "ERROR2",
            "Elasticsearch query failed:"
                + " {\"query\":{\"bool\":{\"must\":[{\"match\":{\"email\":\"*@example.com\"}}]}}}"),
        rows(
            "ERROR3",
            "Failed to send email to multiple recipients: invalid@, not-an-email, missing@.com,"
                + " @no-local"),
        rows(
            "ERROR4",
            "Failed to process message from queue: Invalid JSON in message body containing email"
                + " notifications@queue.system"));

    // Test 2: Single character wildcard with ?
    // ? matches exactly one character
    // INFO? matches INFO2, INFO3, INFO4 (5 characters total)
    // INF? would match any 4-character string starting with "INF"
    JSONObject singleCharWildcard =
        executeQuery(
            String.format(
                "search source=%s severityText=\\\"INFO?\\\" | sort time | fields severityText,"
                    + " body | head 3",
                TEST_INDEX_OTEL_LOGS));
    verifyDataRows(
        singleCharWildcard,
        rows("INFO2", "Kafka message produced to topic 'user-events' with key 'user-123'"),
        rows(
            "INFO3",
            "Webhook delivered to"
                + " https://api.partner.com/webhook?token=abc123&email=webhook@partner.com"),
        rows(
            "INFO4",
            "Batch job completed: Processed 1000 user records, found 50 emails matching pattern"
                + " *@deprecated-domain.com"));

    // Test 3: Wildcard on text field (body)
    // Search for any log containing "user" followed by anything and "@example"
    JSONObject textFieldWildcard =
        executeQuery(
            String.format(
                "search source=%s body=\\\"user*\\\" | sort time | fields body | head 3",
                TEST_INDEX_OTEL_LOGS));
    verifyDataRows(
        textFieldWildcard,
        rows("User e1ce63e6-8501-11f0-930d-c2fcbdc05f14 adding 4 of product HQTGWGPNH4 to cart"),
        rows("Payment failed: Insufficient funds for user@example.com"),
        rows(
            "Executing SQL: SELECT * FROM users WHERE email LIKE '%@gmail.com' AND status !="
                + " 'deleted' ORDER BY created_at DESC"));

    // Test 4: Free text search with wildcards (no field specified)
    // Search for words starting with "fail"
    JSONObject freeTextWildcard =
        executeQuery(
            String.format(
                "search source=%s fail* | sort time | fields body | head 3", TEST_INDEX_OTEL_LOGS));
    verifyDataRows(
        freeTextWildcard,
        rows("Payment failed: Insufficient funds for user@example.com"),
        rows(
            "Failed to parse JSON with special characters: {\"key\": \"value with \\\"quotes\\\""
                + " and: $@#%^&*()[]{}<>|\\/?\"}"),
        rows(
            "Elasticsearch query failed:"
                + " {\"query\":{\"bool\":{\"must\":[{\"match\":{\"email\":\"*@example.com\"}}]}}}"));

    // Test 5: Complex wildcard patterns on service names
    // Search for service names ending with "-service"
    JSONObject serviceWildcard =
        executeQuery(
            String.format(
                "search source=%s `resource.attributes.service.name`=*-service | sort time | fields"
                    + " `resource.attributes.service.name`, body | head 4",
                TEST_INDEX_OTEL_LOGS));
    verifyDataRows(
        serviceWildcard,
        rows(
            "cart-service",
            "User e1ce63e6-8501-11f0-930d-c2fcbdc05f14 adding 4 of product HQTGWGPNH4 to cart"),
        rows("payment-service", "Payment failed: Insufficient funds for user@example.com"),
        rows(
            "search-service",
            "Query contains Lucene special characters: +field:value -excluded AND (grouped OR"
                + " terms) NOT \"exact phrase\" wildcard* fuzzy~2 /regex/ [range TO search]"),
        rows(
            "notification-service",
            "Email notification sent to john.doe+newsletter@company.com with subject: 'Welcome!"
                + " Your order #12345 is confirmed'"));

    // Test 6: Combining wildcards with other operators
    JSONObject combinedWildcard =
        executeQuery(
            String.format(
                "search source=%s severityText=ERR* AND severityNumber>16 | sort time | fields"
                    + " severityText, severityNumber | head 3",
                TEST_INDEX_OTEL_LOGS));
    verifyDataRows(combinedWildcard, rows("ERROR", 17), rows("ERROR", 17), rows("ERROR2", 18));
  }

  @Test
  public void testWildcardEscaping() throws IOException {
    // Test escaping wildcards to search for literal * and ? characters
    // The test data contains: "wildcard* fuzzy~2" as literal text

    // Test 1: Unescaped * is a wildcard - matches "wildcard" followed by anything
    JSONObject wildcardSearch =
        executeQuery(
            String.format(
                "search source=%s wildcard* | sort time | fields body, `attributes.error.message` |"
                    + " head 3",
                TEST_INDEX_OTEL_LOGS));
    // Should match any document containing words starting with "wildcard"
    verifyDataRows(
        wildcardSearch,
        rows(
            "Query contains Lucene special characters: +field:value -excluded AND (grouped OR"
                + " terms) NOT \"exact phrase\" wildcard* fuzzy~2 /regex/ [range TO search]",
            null),
        rows(
            "Elasticsearch query failed:"
                + " {\"query\":{\"bool\":{\"must\":[{\"match\":{\"email\":\"*@example.com\"}}]}}}",
            "Wildcards not allowed at start of term"));

    // Test 2: Escaped * searches for literal asterisk
    // To search for literal "wildcard*", need to escape the asterisk
    JSONObject literalAsterisk =
        executeQuery(
            String.format(
                "search source=%s \\\"wildcard\\\\*\\\" | fields body, `attributes.error.message`",
                TEST_INDEX_OTEL_LOGS));
    // Should match the document containing literal "wildcard*"
    verifyDataRows(
        literalAsterisk,
        rows(
            "Query contains Lucene special characters: +field:value -excluded AND (grouped OR"
                + " terms) NOT \"exact phrase\" wildcard* fuzzy~2 /regex/ [range TO search]",
            null));

    // Test 3: Search for paths with backslashes (like Windows paths)
    // The test data contains: "C:\\Users\\admin"
    // lucene escaping -> \\\\
    // rest request escaping -> \\\\\\\\
    // java string escaping ->\\\\\\\\
    JSONObject backslashSearch =
        executeQuery(
            String.format(
                "search source=%s"
                    + " `attributes.error.type`=\\\"C:\\\\\\\\\\\\\\\\Users\\\\\\\\\\\\\\\\admin\\\""
                    + " | sort time | fields attributes.error.type",
                TEST_INDEX_OTEL_LOGS));
    verifyDataRows(backslashSearch, rows("C:\\Users\\admin"));
  }

  @Test
  public void testSearchWithSQLInjectionPattern() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "search source=%s \\\"DROP TABLE users\\\" | fields body", TEST_INDEX_OTEL_LOGS));
    verifyDataRows(
        result,
        rows("Potential SQL injection detected: '; DROP TABLE users; -- in search parameter"));
  }

  @Test
  public void testSearchWithJSONSpecialChars() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "search source=%s\\\"quotes\\\\\\\" and: $@#\\\"  | fields body",
                TEST_INDEX_OTEL_LOGS));
    verifyDataRows(
        result,
        rows(
            "Failed to parse JSON with special characters: {\"key\": \"value with \\\"quotes\\\""
                + " and: $@#%^&*()[]{}<>|\\/?\"}"));
  }

  @Test
  public void testSearchWithIPAddress() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "search source=%s `attributes.client.ip`=\\\"192.168.1.1\\\" | fields body",
                TEST_INDEX_OTEL_LOGS));
    verifyDataRows(
        result,
        rows(
            "192.168.1.1 - - [15/Jan/2024:10:30:03 +0000] \"GET"
                + " /api/products?search=laptop&category=electronics HTTP/1.1\" 200 1234 \"-\""
                + " \"Mozilla/5.0\""));
  }

  // ===== Complex Mixed Queries =====

  @Test
  public void testSearchMixedWithPipeCommands() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "search source=%s severityNumber>10 | where severityText != \\\"INFO\\\" | sort"
                    + " time | fields severityText, body | head 5",
                TEST_INDEX_OTEL_LOGS));
    verifyDataRows(
        result,
        rows("ERROR", "Payment failed: Insufficient funds for user@example.com"),
        rows(
            "WARN",
            "Query contains Lucene special characters: +field:value -excluded AND (grouped OR"
                + " terms) NOT \"exact phrase\" wildcard* fuzzy~2 /regex/ [range TO search]"),
        rows(
            "FATAL",
            "Database connection pool exhausted: postgresql://db.example.com:5432/production"),
        rows(
            "ERROR",
            "Failed to parse JSON with special characters: {\"key\": \"value with \\\"quotes\\\""
                + " and: $@#%^&*()[]{}<>|\\/?\"}"),
        rows(
            "WARN",
            "Potential SQL injection detected: '; DROP TABLE users; -- in search parameter"));
  }

  @Test
  public void testSearchWithMultipleFieldTypes() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "search source=%s severityText=\\\"ERROR\\\" AND severityNumber=17 | fields"
                    + " severityText, severityNumber | head 2",
                TEST_INDEX_OTEL_LOGS));
    verifyDataRows(result, rows("ERROR", 17), rows("ERROR", 17));
  }

  // ===== Attribute Field Searches =====

  @Test
  public void testSearchWithAttributeFields() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "search source=%s `attributes.http.status_code`=200 | fields body",
                TEST_INDEX_OTEL_LOGS));
    verifyDataRows(
        result,
        rows(
            "192.168.1.1 - - [15/Jan/2024:10:30:03 +0000] \"GET"
                + " /api/products?search=laptop&category=electronics HTTP/1.1\" 200 1234 \"-\""
                + " \"Mozilla/5.0\""));
  }

  @Test
  public void testSearchWithNestedEmailAttribute() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "search source=%s `attributes.user.email`=\\\"user@example.com\\\" | fields body",
                TEST_INDEX_OTEL_LOGS));
    verifyDataRows(result, rows("Payment failed: Insufficient funds for user@example.com"));
  }

  // ===== Range Query Edge Cases =====

  @Test
  public void testSearchWithInclusiveRanges() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "search source=%s severityNumber>=9 AND severityNumber<=10 | sort time | fields"
                    + " severityNumber, body | head 6",
                TEST_INDEX_OTEL_LOGS));
    verifyDataRows(
        result,
        rows(9, "User e1ce63e6-8501-11f0-930d-c2fcbdc05f14 adding 4 of product HQTGWGPNH4 to cart"),
        rows(
            9,
            "Email notification sent to john.doe+newsletter@company.com with subject: 'Welcome!"
                + " Your order #12345 is confirmed'"),
        rows(
            9,
            "[2024-01-15 10:30:09] production.INFO: User authentication successful for"
                + " admin@company.org using OAuth2"),
        rows(10, "Kafka message produced to topic 'user-events' with key 'user-123'"),
        rows(9, "Health check passed for all services including email-service@health.monitor"),
        rows(
            9,
            "CORS request from origin https://app.example.com to access /api/users/email containing"
                + " sensitive@data.secure"));
  }

  @Test
  public void testSearchWithImpossibleRange() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "search source=%s severityNumber>30 AND severityNumber<5", TEST_INDEX_OTEL_LOGS));
    verifyDataRows(result);
  }

  // ===== IN Operator Edge Cases =====

  @Test
  public void testSearchWithEmptyINList() throws IOException {
    JSONObject result =
        executeQuery(
            String.format("search source=%s severityNumber IN (999)", TEST_INDEX_OTEL_LOGS));
    verifyDataRows(result);
  }

  @Test
  public void testSearchWithSingleValueIN() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "search source=%s severityText IN (\\\"ERROR\\\") | fields severityText | head 3",
                TEST_INDEX_OTEL_LOGS));
    verifyDataRows(result, rows("ERROR"), rows("ERROR"));
    // search with quotes included.
    result =
        executeQuery(
            String.format(
                "search source=%s severityText IN (\\\"\\\\\\\"ERROR\\\\\\\"\\\") | fields"
                    + " severityText | head 3",
                TEST_INDEX_OTEL_LOGS));
    verifyDataRows(result);
  }

  @Test
  public void testSearchWithUpperCaseValue() throws IOException {
    JSONObject result =
        executeQuery(
            String.format("search source=%s severityText=\\\"NOTEXIST\\\"", TEST_INDEX_OTEL_LOGS));
    verifyDataRows(result);
  }

  @Test
  public void testSearchWithInvalidFieldName() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "search source=%s nonexistent_field=\\\"value\\\"", TEST_INDEX_OTEL_LOGS));
    verifyDataRows(result);
  }

  @Test
  public void testSearchWithTypeMismatch() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "search source=%s severityNumber=\\\"not-a-number\\\"", TEST_INDEX_OTEL_LOGS));
    verifyDataRows(result);
  }

  @Test
  public void testSearchWithDoubleFieldComparisons() throws IOException {
    // Test 1: Exact match with decimal notation
    JSONObject exactMatch =
        executeQuery(
            String.format(
                "search source=%s `attributes.payment.amount`=1500.0 | fields"
                    + " `attributes.payment.amount`",
                TEST_INDEX_OTEL_LOGS));
    verifyDataRows(exactMatch, rows(1500.0));

    // Test 2: Double with 'd' suffix
    JSONObject doubleSuffix =
        executeQuery(
            String.format(
                "search source=%s `attributes.payment.amount`=1500.0d | fields"
                    + " `attributes.payment.amount`",
                TEST_INDEX_OTEL_LOGS));
    verifyDataRows(doubleSuffix, rows(1500.0));

    // Test 3: Float with 'f' suffix
    JSONObject floatSuffix =
        executeQuery(
            String.format(
                "search source=%s `attributes.payment.amount`=1500.0f | fields"
                    + " `attributes.payment.amount`",
                TEST_INDEX_OTEL_LOGS));
    verifyDataRows(floatSuffix, rows(1500.0));

    // Test 4: Integer value on double field (no decimal)
    JSONObject integerValue =
        executeQuery(
            String.format(
                "search source=%s `attributes.payment.amount`=1500 | fields"
                    + " `attributes.payment.amount`",
                TEST_INDEX_OTEL_LOGS));
    verifyDataRows(integerValue, rows(1500.0));
  }

  @Test
  public void testSearchWithDoubleRangeOperators() throws IOException {
    // Test 1: Greater than
    JSONObject greaterThan =
        executeQuery(
            String.format(
                "search source=%s `attributes.payment.amount`>1000.0 | fields"
                    + " `attributes.payment.amount`",
                TEST_INDEX_OTEL_LOGS));
    verifyDataRows(greaterThan, rows(1500.0));

    // Test 2: Greater or equal
    JSONObject greaterOrEqual =
        executeQuery(
            String.format(
                "search source=%s `attributes.payment.amount`>=1500.0 | fields"
                    + " `attributes.payment.amount`",
                TEST_INDEX_OTEL_LOGS));
    verifyDataRows(greaterOrEqual, rows(1500.0));

    // Test 3: Range query with AND
    JSONObject rangeQuery =
        executeQuery(
            String.format(
                "search source=%s `attributes.payment.amount`>=1000.0 AND"
                    + " `attributes.payment.amount`<=2000.0 | fields `attributes.payment.amount`",
                TEST_INDEX_OTEL_LOGS));
    verifyDataRows(rangeQuery, rows(1500.0));

    // Test 4: Not equals (should return no results as we only have 1500.0)
    JSONObject notEquals =
        executeQuery(
            String.format(
                "search source=%s `attributes.payment.amount`!=1500.0 | fields"
                    + " `attributes.payment.amount`",
                TEST_INDEX_OTEL_LOGS));
    verifyDataRows(notEquals);
  }

  @Test
  public void testSearchWithDoubleINOperator() throws IOException {
    // Test double with IN operator
    JSONObject result =
        executeQuery(
            String.format(
                "search source=%s `attributes.payment.amount` IN (1000.0, 1500.0, 2000.0) | fields"
                    + " `attributes.payment.amount`, body",
                TEST_INDEX_OTEL_LOGS));
    verifyDataRows(result, rows(1500.0, "Payment failed: Insufficient funds for user@example.com"));
  }

  @Test
  public void testSearchWithDateFormats() throws IOException {
    // Test 1: Full timestamp with nanoseconds
    JSONObject fullTimestamp =
        executeQuery(
            String.format(
                "search source=%s @timestamp=\\\"2024-01-15T10:30:00.123456789Z\\\" | sort"
                    + " @timestamp | fields @timestamp, body | head 1",
                TEST_INDEX_OTEL_LOGS));
    verifyDataRows(
        fullTimestamp,
        rows(
            "2024-01-15 10:30:00.123456789",
            "User e1ce63e6-8501-11f0-930d-c2fcbdc05f14 adding 4 of product HQTGWGPNH4 to cart"));

    // Test 2: Date only format (matches all records from that date)
    JSONObject dateOnly =
        executeQuery(
            String.format(
                "search source=%s @timestamp=\\\"2024-01-15\\\" | sort @timestamp | fields"
                    + " @timestamp | head 3",
                TEST_INDEX_OTEL_LOGS));
    // Should match multiple records from 2024-01-15
    verifyDataRows(
        dateOnly,
        rows("2024-01-15 10:30:00.123456789"),
        rows("2024-01-15 10:30:01.23456789"),
        rows("2024-01-15 10:30:02.345678901"));

    // Test 3: Timestamp without nanoseconds
    JSONObject timestampNoNanos =
        executeQuery(
            String.format(
                "search source=%s @timestamp=\\\"2024-01-15T10:30:01\\\" | sort @timestamp | fields"
                    + " @timestamp | head 2",
                TEST_INDEX_OTEL_LOGS));
    // Should match records at that second
    verifyDataRows(timestampNoNanos, rows("2024-01-15 10:30:01.23456789"));
  }

  @Test
  public void testSearchWithDateRangeComparisons() throws IOException {
    // Test 1: Greater than - finds records after specified time
    JSONObject greaterThan =
        executeQuery(
            String.format(
                "search source=%s @timestamp>\\\"2024-01-15T10:30:00Z\\\" | sort @timestamp |"
                    + " fields @timestamp | head 3",
                TEST_INDEX_OTEL_LOGS));
    verifyDataRows(
        greaterThan,
        rows("2024-01-15 10:30:01.23456789"),
        rows("2024-01-15 10:30:02.345678901"),
        rows("2024-01-15 10:30:03.456789012"));

    // Test 2: Less than or equal - finds records up to specified time
    JSONObject lessOrEqual =
        executeQuery(
            String.format(
                "search source=%s @timestamp<=\\\"2024-01-15T10:30:01Z\\\" | sort @timestamp |"
                    + " fields @timestamp | head 2",
                TEST_INDEX_OTEL_LOGS));
    verifyDataRows(
        lessOrEqual, rows("2024-01-15 10:30:00.123456789"), rows("2024-01-15 10:30:01.23456789"));

    // Test 3: Date range with AND - finds records within time window
    JSONObject dateRange =
        executeQuery(
            String.format(
                "search source=%s @timestamp>=\\\"2024-01-15T10:30:00Z\\\" AND"
                    + " @timestamp<\\\"2024-01-15T10:30:05Z\\\" | sort @timestamp | fields"
                    + " @timestamp | head 5",
                TEST_INDEX_OTEL_LOGS));
    // Should return records within that 5 second window
    verifyDataRows(
        dateRange,
        rows("2024-01-15 10:30:00.123456789"),
        rows("2024-01-15 10:30:01.23456789"),
        rows("2024-01-15 10:30:02.345678901"),
        rows("2024-01-15 10:30:03.456789012"),
        rows("2024-01-15 10:30:04.567890123"));
    // Test 4: Not equals (rarely used for timestamps but should work)
    JSONObject notEquals =
        executeQuery(
            String.format(
                "search source=%s @timestamp!=\\\"2024-01-15T10:30:00.123456789Z\\\" | sort"
                    + " @timestamp | fields @timestamp | head 3",
                TEST_INDEX_OTEL_LOGS));
    // Should return all other records
    verifyDataRows(
        notEquals,
        rows("2024-01-15 10:30:01.23456789"),
        rows("2024-01-15 10:30:02.345678901"),
        rows("2024-01-15 10:30:03.456789012"));
  }

  @Test
  public void testSearchWithDateINOperator() throws IOException {
    // Test date field with IN operator for specific timestamps
    JSONObject result =
        executeQuery(
            String.format(
                "search source=%s @timestamp IN (\\\"2024-01-15T10:30:00.123456789Z\\\","
                    + " \\\"2024-01-15T10:30:01.234567890Z\\\") | sort @timestamp | fields"
                    + " @timestamp, severityText",
                TEST_INDEX_OTEL_LOGS));
    verifyDataRows(
        result,
        rows("2024-01-15 10:30:00.123456789", "INFO"),
        rows("2024-01-15 10:30:01.23456789", "ERROR"));
  }

  @Test
  public void testSearchWithAbsoluteEarliestAndNow() throws IOException {
    JSONObject result1 =
        executeQuery(
            String.format(
                "search source=%s earliest='2025-08-01 03:47:41' latest=now | fields @timestamp",
                TEST_INDEX_TIME_DATA));
    verifySchema(result1, schema("@timestamp", "timestamp"));
    verifyDataRows(result1, rows("2025-08-01 03:47:41"));

    JSONObject result0 =
        executeQuery(
            String.format(
                "search source=%s earliest='2025-08-01 03:47:42' latest=now() | fields @timestamp",
                TEST_INDEX_TIME_DATA));
    verifyNumOfRows(result0, 0);

    JSONObject result2 =
        executeQuery(
            String.format(
                "search source=%s earliest='2025-08-01 02:00:55' | fields @timestamp",
                TEST_INDEX_TIME_DATA));
    verifyDataRows(result2, rows("2025-08-01 02:00:56"), rows("2025-08-01 03:47:41"));
  }

  @Test
  public void testSearchWithChainedRelativeTimeRange() throws IOException {
    JSONObject result1 =
        executeQuery(
            String.format(
                "search source=%s earliest='2025-08-01 03:47:41' latest=+10months@year | fields"
                    + " @timestamp",
                TEST_INDEX_TIME_DATA));
    verifySchema(result1, schema("@timestamp", "timestamp"));
    verifyDataRows(result1, rows("2025-08-01 03:47:41"));
  }

  @Test
  public void testSearchWithNumericTimeRange() throws IOException {
    JSONObject result1 =
        executeQuery(
            String.format(
                "search source=%s earliest=1754020060.123 latest=1754020061 | fields @timestamp",
                TEST_INDEX_TIME_DATA));
    verifySchema(result1, schema("@timestamp", "timestamp"));
    verifyDataRows(result1, rows("2025-08-01 03:47:41"));
  }
}
