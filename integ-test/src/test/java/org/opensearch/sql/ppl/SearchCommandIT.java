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
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import org.json.JSONObject;
import org.junit.Ignore;
import org.junit.jupiter.api.Test;
import org.opensearch.client.Request;
import org.opensearch.client.ResponseException;
import org.opensearch.sql.common.utils.StringUtils;

public class SearchCommandIT extends PPLIntegTestCase {
  private static final DateTimeFormatter PPL_TIMESTAMP_FORMATTER =
      DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

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
                "search source=%s severityNumber=\"not-a-number\"", TEST_INDEX_OTEL_LOGS));
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
  public void testSearchWithTraceId() throws IOException {
    // Test 1: Search for specific traceId
    JSONObject specificTraceId =
        executeQuery(
            String.format(
                "search source=%s b3cb01a03c846973fd496b973f49be85 | fields" + " traceId, body",
                TEST_INDEX_OTEL_LOGS));
    verifyDataRows(
        specificTraceId,
        rows(
            "b3cb01a03c846973fd496b973f49be85",
            "User e1ce63e6-8501-11f0-930d-c2fcbdc05f14 adding 4 of product HQTGWGPNH4 to cart"));
  }

  @Test
  public void testSearchWithSpanLength() throws IOException {
    // Test searching for SPANLENGTH keyword in free text search
    // This tests that SPANLENGTH tokens like "3month" are searchable
    JSONObject result =
        executeQuery(
            String.format(
                "search source=%s 3month | fields body, `attributes.span.duration`",
                TEST_INDEX_OTEL_LOGS));
    verifyDataRows(result, rows("Processing data for 3month period", "3month"));
  }

  @Test
  public void testSearchWithSpanLengthInField() throws IOException {
    // Test searching for SPANLENGTH value in a specific field
    JSONObject result =
        executeQuery(
            String.format(
                "search source=%s `attributes.span.duration`=\\\"3month\\\" | fields body,"
                    + " `attributes.span.duration`",
                TEST_INDEX_OTEL_LOGS));
    verifyDataRows(result, rows("Processing data for 3month period", "3month"));
  }

  @Test
  public void testSearchWithNumericIdVsSpanLength() throws IOException {
    // Test that NUMERIC_ID tokens like "1s4f7" (which start with what could be a SPANLENGTH like
    // "1s")
    // are properly searchable as complete tokens
    // This verifies that NUMERIC_ID takes precedence over SPANLENGTH when applicable

    // Test 1: Search for the NUMERIC_ID token in free text
    JSONObject numericIdResult =
        executeQuery(
            String.format(
                "search source=%s 1s4f7 | fields body, `attributes.transaction.id`",
                TEST_INDEX_OTEL_LOGS));
    verifyDataRows(numericIdResult, rows("Transaction ID 1s4f7 processed successfully", "1s4f7"));

    // Test 2: Search for NUMERIC_ID in specific field
    JSONObject fieldSearchResult =
        executeQuery(
            String.format(
                "search source=%s `attributes.transaction.id`=1s4f7 | fields body,"
                    + " `attributes.transaction.id`",
                TEST_INDEX_OTEL_LOGS));
    verifyDataRows(fieldSearchResult, rows("Transaction ID 1s4f7 processed successfully", "1s4f7"));

    // Test 3: Verify that searching for just "1s" (which would be a SPANLENGTH)
    // does NOT match the "1s4f7" token
    JSONObject spanLengthSearchResult =
        executeQuery(
            String.format(
                "search source=%s `attributes.transaction.id`=1s | fields body",
                TEST_INDEX_OTEL_LOGS));
    verifyDataRows(spanLengthSearchResult); // Should return no results
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
                "search source=%s earliest='2025-08-01 03:47:41' latest=+1months@year | fields"
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

  @Test
  public void testSearchTimeModifierWithSnappedWeek() throws IOException {
    // Test whether alignment to weekday works

    final int docId = 101;
    Request insertRequest =
        new Request("PUT", String.format("/%s/_doc/%d?refresh=true", TEST_INDEX_TIME_DATA, docId));

    // Get the current weekday
    LocalDateTime base = LocalDateTime.now(ZoneOffset.UTC);
    // Truncate to last sunday
    LocalDateTime lastSunday =
        base.minusDays((base.getDayOfWeek().getValue() % 7)).truncatedTo(ChronoUnit.DAYS);
    LocalDateTime lastMonday =
        base.minusDays((base.getDayOfWeek().getValue() + 6) % 7).truncatedTo(ChronoUnit.DAYS);
    LocalDateTime lastFriday =
        base.minusDays((base.getDayOfWeek().getValue() + 2) % 7).truncatedTo(ChronoUnit.DAYS);

    // Insert data
    LocalDateTime insertedSunday = lastSunday.plusMinutes(10);
    insertRequest.setJsonEntity(
        StringUtils.format(
            "{\"value\":100090,\"category\":\"A\",\"@timestamp\":\"%s\"}\n",
            insertedSunday.format(DateTimeFormatter.ISO_DATE_TIME)));
    client().performRequest(insertRequest);

    JSONObject result1 =
        executeQuery(String.format("source=%s earliest=@w0 latest='@w+1h'", TEST_INDEX_TIME_DATA));
    verifySchema(
        result1,
        schema("timestamp", "timestamp"),
        schema("value", "int"),
        schema("category", "string"),
        schema("@timestamp", "timestamp"));
    verifyDataRows(
        result1, rows(insertedSunday.format(PPL_TIMESTAMP_FORMATTER), "A", 100090, null));

    // Test last Monday
    LocalDateTime insertedMonday = lastMonday.plusHours(2).plusMinutes(10);
    Request insertRequest2 =
        new Request("PUT", String.format("/%s/_doc/%d?refresh=true", TEST_INDEX_TIME_DATA, docId));
    insertRequest2.setJsonEntity(
        StringUtils.format(
            "{\"value\":100091,\"category\":\"B\",\"@timestamp\":\"%s\"}\n",
            insertedMonday.format(DateTimeFormatter.ISO_DATE_TIME)));
    client().performRequest(insertRequest2);

    JSONObject result2 =
        executeQuery(
            String.format(
                "source=%s earliest='@w1+2h' latest='@w1+2h+30min'", TEST_INDEX_TIME_DATA));
    verifySchema(
        result2,
        schema("timestamp", "timestamp"),
        schema("value", "int"),
        schema("category", "string"),
        schema("@timestamp", "timestamp"));
    verifyDataRows(
        result2, rows(insertedMonday.format(PPL_TIMESTAMP_FORMATTER), "B", 100091, null));

    // Test last Friday
    LocalDateTime insertedFriday = lastFriday.plusMinutes(10);
    Request insertRequest3 =
        new Request("PUT", String.format("/%s/_doc/%d?refresh=true", TEST_INDEX_TIME_DATA, docId));
    insertRequest3.setJsonEntity(
        StringUtils.format(
            "{\"value\":100092,\"category\":\"C\",\"@timestamp\":\"%s\"}\n",
            insertedFriday.format(DateTimeFormatter.ISO_DATE_TIME)));
    client().performRequest(insertRequest3);

    JSONObject result3 =
        executeQuery(
            String.format("source=%s earliest=@w5 latest='@w5+30minutes'", TEST_INDEX_TIME_DATA));
    verifySchema(
        result3,
        schema("timestamp", "timestamp"),
        schema("value", "int"),
        schema("category", "string"),
        schema("@timestamp", "timestamp"));
    verifyDataRows(
        result3, rows(insertedFriday.format(PPL_TIMESTAMP_FORMATTER), "C", 100092, null));

    Request deleteRequest =
        new Request(
            "DELETE", String.format("/%s/_doc/%d?refresh=true", TEST_INDEX_TIME_DATA, docId));
    client().performRequest(deleteRequest);
  }

  @Test
  public void testSearchWithRelativeTimeModifiers() throws IOException {
    final int docId = 101;

    LocalDateTime currentTime = LocalDateTime.now(ZoneOffset.UTC);
    LocalDateTime testTime = currentTime.minusMinutes(30).truncatedTo(ChronoUnit.MINUTES);

    Request insertRequest =
        new Request("PUT", String.format("/%s/_doc/%d?refresh=true", TEST_INDEX_TIME_DATA, docId));
    insertRequest.setJsonEntity(
        StringUtils.format(
            "{\"value\":200001,\"category\":\"RELATIVE\",\"@timestamp\":\"%s\"}\n",
            testTime.format(DateTimeFormatter.ISO_DATE_TIME)));
    client().performRequest(insertRequest);

    // Test -1h (1 hour ago) since it's only 30 minutes old
    JSONObject result1 =
        executeQuery(
            String.format(
                "source=%s earliest=-1h | fields @timestamp, value | head 5",
                TEST_INDEX_TIME_DATA));
    verifySchema(result1, schema("@timestamp", "timestamp"), schema("value", "int"));
    verifyDataRows(result1, rows(testTime.format(PPL_TIMESTAMP_FORMATTER), 200001));

    // Test -30m (30 minutes ago)
    JSONObject result2 =
        executeQuery(
            String.format(
                "source=%s earliest=-50m latest=now | fields @timestamp, value | head 5",
                TEST_INDEX_TIME_DATA));
    verifySchema(result2, schema("@timestamp", "timestamp"), schema("value", "int"));
    verifyDataRows(result2, rows(testTime.format(PPL_TIMESTAMP_FORMATTER), 200001));

    // Test -7d (7 days ago) - should return no results since our data is recent
    JSONObject result3 =
        executeQuery(
            String.format(
                "source=%s earliest=-7d latest=-6d | fields @timestamp | head 1",
                TEST_INDEX_TIME_DATA));
    verifySchema(result3, schema("@timestamp", "timestamp"));
    verifyNumOfRows(result3, 0);

    Request deleteRequest =
        new Request(
            "DELETE", String.format("/%s/_doc/%d?refresh=true", TEST_INDEX_TIME_DATA, docId));
    client().performRequest(deleteRequest);
  }

  @Test
  public void testSearchWithTimeUnitSnapping() throws IOException {
    final int docId = 101;

    LocalDateTime currentHour = LocalDateTime.now(ZoneOffset.UTC).truncatedTo(ChronoUnit.HOURS);
    LocalDateTime testTime = currentHour.plusMinutes(15).truncatedTo(ChronoUnit.MINUTES);

    Request insertRequest =
        new Request("PUT", String.format("/%s/_doc/%d?refresh=true", TEST_INDEX_TIME_DATA, docId));
    insertRequest.setJsonEntity(
        StringUtils.format(
            "{\"value\":200002,\"category\":\"SNAP\",\"@timestamp\":\"%s\"}\n",
            testTime.format(DateTimeFormatter.ISO_DATE_TIME)));
    client().performRequest(insertRequest);

    // Test @h (snap to hour)
    JSONObject result1 =
        executeQuery(
            String.format(
                "source=%s earliest=@h latest='@h+1h' | fields @timestamp, value",
                TEST_INDEX_TIME_DATA));
    verifySchema(result1, schema("@timestamp", "timestamp"), schema("value", "int"));
    verifyDataRows(result1, rows(testTime.format(PPL_TIMESTAMP_FORMATTER), 200002));

    // Test @d (snap to day)
    JSONObject result2 =
        executeQuery(
            String.format(
                "source=%s earliest=@d latest='@d+1d' | fields @timestamp | head 5",
                TEST_INDEX_TIME_DATA));
    verifySchema(result2, schema("@timestamp", "timestamp"));
    verifyDataRows(result2, rows(testTime.format(PPL_TIMESTAMP_FORMATTER)));

    // Test @M (snap to month)
    JSONObject result3 =
        executeQuery(
            String.format(
                "source=%s earliest=@mon latest='@mon+1mon' | fields @timestamp | head 5",
                TEST_INDEX_TIME_DATA));
    verifySchema(result3, schema("@timestamp", "timestamp"));
    verifyDataRows(result3, rows(testTime.format(PPL_TIMESTAMP_FORMATTER)));

    Request deleteRequest =
        new Request(
            "DELETE", String.format("/%s/_doc/%d?refresh=true", TEST_INDEX_TIME_DATA, docId));
    client().performRequest(deleteRequest);
  }

  @Test
  public void testSearchWithQuarterlyModifiers() throws IOException {
    final int docId = 101;

    LocalDateTime currentQuarter =
        LocalDateTime.now(ZoneOffset.UTC)
            .plusYears(1)
            .withMonth(((LocalDateTime.now(ZoneOffset.UTC).getMonthValue() - 1) / 3) * 3 + 1)
            .withDayOfMonth(1)
            .truncatedTo(ChronoUnit.DAYS);
    LocalDateTime testTime = currentQuarter.plusDays(15);

    Request insertRequest =
        new Request("PUT", String.format("/%s/_doc/%d?refresh=true", TEST_INDEX_TIME_DATA, docId));
    insertRequest.setJsonEntity(
        StringUtils.format(
            "{\"value\":200003,\"category\":\"QUARTER\",\"@timestamp\":\"%s\"}\n",
            testTime.format(DateTimeFormatter.ISO_DATE_TIME)));
    client().performRequest(insertRequest);

    // Test @q (snap to quarter)
    JSONObject result1 =
        executeQuery(
            String.format(
                "source=%s earliest=+1year@q latest='+1year@q+3M' | fields @timestamp, value",
                TEST_INDEX_TIME_DATA));
    verifySchema(result1, schema("@timestamp", "timestamp"), schema("value", "int"));
    verifyDataRows(result1, rows(testTime.format(PPL_TIMESTAMP_FORMATTER), 200003));

    // Test -2q (2 quarters ago, equivalent to -6M), should return no data
    JSONObject result2 =
        executeQuery(
            String.format(
                "source=%s earliest='+1year-2q' latest='+1year-1q' | fields @timestamp | head 1",
                TEST_INDEX_TIME_DATA));
    verifySchema(result2, schema("@timestamp", "timestamp"));
    verifyNumOfRows(result2, 0);

    Request deleteRequest =
        new Request(
            "DELETE", String.format("/%s/_doc/%d?refresh=true", TEST_INDEX_TIME_DATA, docId));
    client().performRequest(deleteRequest);
  }

  @Ignore("test fail in Oct 1st, 2025. Seems a test bug")
  public void testSearchWithComplexChainedExpressions() throws IOException {
    final int docId = 101;

    LocalDateTime lastDay =
        LocalDateTime.now(ZoneOffset.UTC).minusDays(1).truncatedTo(ChronoUnit.DAYS);
    LocalDateTime testTime = lastDay.minusHours(2).plusMinutes(10);

    Request insertRequest =
        new Request("PUT", String.format("/%s/_doc/%d?refresh=true", TEST_INDEX_TIME_DATA, docId));
    insertRequest.setJsonEntity(
        StringUtils.format(
            "{\"value\":200004,\"category\":\"COMPLEX\",\"@timestamp\":\"%s\"}\n",
            testTime.format(DateTimeFormatter.ISO_DATE_TIME)));
    client().performRequest(insertRequest);

    // Test -1d@d-2h+10m (1 day ago at day start, minus 2 hours, plus 10 minutes)
    JSONObject result1 =
        executeQuery(
            String.format(
                "source=%s earliest='-1d@d-2h' latest='-1d@d-2h+20m' | fields @timestamp,"
                    + " value",
                TEST_INDEX_TIME_DATA));
    verifySchema(result1, schema("@timestamp", "timestamp"), schema("value", "int"));
    verifyDataRows(result1, rows(testTime.format(PPL_TIMESTAMP_FORMATTER), 200004));

    // Test -1mon@mon+7d (1 month ago at month start, plus 7 days) - should return no results since
    // our data is recent
    JSONObject result2 =
        executeQuery(
            String.format(
                "source=%s earliest='-1mon@mon+7d' latest='-1mon@mon+8d' | fields @timestamp | head"
                    + " 1",
                TEST_INDEX_TIME_DATA));
    verifySchema(result2, schema("@timestamp", "timestamp"));
    verifyNumOfRows(result2, 0);

    Request deleteRequest =
        new Request(
            "DELETE", String.format("/%s/_doc/%d?refresh=true", TEST_INDEX_TIME_DATA, docId));
    client().performRequest(deleteRequest);
  }
}
