/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.remote;

import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_ACCOUNT;

import java.io.IOException;
import org.json.JSONObject;
import org.junit.jupiter.api.Test;
import org.opensearch.sql.common.setting.Settings;
import org.opensearch.sql.ppl.PPLIntegTestCase;

public class CalciteRexCommandIT extends PPLIntegTestCase {
  @Override
  public void init() throws Exception {
    super.init();
    enableCalcite();
    loadIndex(Index.ACCOUNT);
  }

  @Test
  public void testRexBasicFieldExtraction() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "source=%s | rex field=email \\\"(?<user>[^@]+)@(?<domain>.+)\\\" | fields email,"
                    + " user, domain",
                TEST_INDEX_ACCOUNT));

    assertEquals(1000, result.getJSONArray("datarows").length());
    assertEquals("amberduke@pyrami.com", result.getJSONArray("datarows").getJSONArray(0).get(0));
    assertEquals("amberduke", result.getJSONArray("datarows").getJSONArray(0).get(1));
    assertEquals("pyrami.com", result.getJSONArray("datarows").getJSONArray(0).get(2));
  }

  @Test
  public void testRexErrorNoNamedGroups() throws IOException {
    try {
      executeQuery(
          String.format(
              "source=%s | rex field=email \\\"([^@]+)@(.+)\\\" | fields email",
              TEST_INDEX_ACCOUNT));
      fail("Should have thrown an exception for pattern without named capture groups");
    } catch (Exception e) {
      assertTrue(
          e.getMessage().contains("Rex pattern must contain at least one named capture group"));
    }
  }

  @Test
  public void testRexErrorInvalidGroupNameUnderscore() throws IOException {
    try {
      executeQuery(
          String.format(
              "source=%s | rex field=email \\\"(?<user_name>[^@]+)@(?<domain>.+)\\\" | fields"
                  + " email",
              TEST_INDEX_ACCOUNT));
      fail("Should have thrown an exception for underscore in named capture group");
    } catch (Exception e) {
      assertTrue(e.getMessage().contains("Invalid capture group name 'user_name'"));
      assertTrue(
          e.getMessage().contains("must start with a letter and contain only letters and digits"));
    }
  }

  @Test
  public void testRexErrorInvalidGroupNameHyphen() throws IOException {
    try {
      executeQuery(
          String.format(
              "source=%s | rex field=email \\\"(?<user-name>[^@]+)@(?<domain>.+)\\\" | fields"
                  + " email",
              TEST_INDEX_ACCOUNT));
      fail("Should have thrown an exception for hyphen in named capture group");
    } catch (Exception e) {
      assertTrue(e.getMessage().contains("Invalid capture group name 'user-name'"));
      assertTrue(
          e.getMessage().contains("must start with a letter and contain only letters and digits"));
    }
  }

  @Test
  public void testRexErrorInvalidGroupNameStartingWithDigit() throws IOException {
    try {
      executeQuery(
          String.format(
              "source=%s | rex field=email \\\"(?<1user>[^@]+)@(?<domain>.+)\\\" | fields"
                  + " email",
              TEST_INDEX_ACCOUNT));
      fail("Should have thrown an exception for group name starting with digit");
    } catch (Exception e) {
      assertTrue(e.getMessage().contains("Invalid capture group name '1user'"));
      assertTrue(
          e.getMessage().contains("must start with a letter and contain only letters and digits"));
    }
  }

  @Test
  public void testRexErrorInvalidGroupNameSpecialCharacter() throws IOException {
    try {
      executeQuery(
          String.format(
              "source=%s | rex field=email \\\"(?<user@name>[^@]+)@(?<domain>.+)\\\" | fields"
                  + " email",
              TEST_INDEX_ACCOUNT));
      fail("Should have thrown an exception for special character in named capture group");
    } catch (Exception e) {
      assertTrue(e.getMessage().contains("Invalid capture group name 'user@name'"));
      assertTrue(
          e.getMessage().contains("must start with a letter and contain only letters and digits"));
    }
  }

  @Test
  public void testRexWithFiltering() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "source=%s | rex field=address"
                    + " \\\"(?<streetnum>\\\\\\\\d+)\\\\\\\\s+(?<streetname>.+)\\\" | fields"
                    + " address, streetnum, streetname",
                TEST_INDEX_ACCOUNT));

    assertEquals(1000, result.getJSONArray("datarows").length());
    assertEquals("880 Holmes Lane", result.getJSONArray("datarows").getJSONArray(0).get(0));
    assertEquals("880", result.getJSONArray("datarows").getJSONArray(0).get(1));
    assertEquals("Holmes Lane", result.getJSONArray("datarows").getJSONArray(0).get(2));
  }

  @Test
  public void testRexMultipleMatches() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "source=%s | rex field=address \\\"(?<words>[A-Za-z]+)\\\" max_match=3 | fields"
                    + " address, words",
                TEST_INDEX_ACCOUNT));

    assertEquals(1000, result.getJSONArray("datarows").length());
    String wordsArray = result.getJSONArray("datarows").getJSONArray(0).get(1).toString();
    assertTrue(wordsArray.contains("Holmes") && wordsArray.contains("Lane"));
    assertTrue(wordsArray.startsWith("[") && wordsArray.endsWith("]"));
  }

  @Test
  public void testRexChainedCommands() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "source=%s | rex field=firstname \\\"(?<firstinitial>^.)\\\" | rex field=lastname"
                    + " \\\"(?<lastinitial>^.)\\\" | fields firstname, lastname, firstinitial,"
                    + " lastinitial",
                TEST_INDEX_ACCOUNT));

    assertEquals(1000, result.getJSONArray("datarows").length());
    assertEquals("Amber", result.getJSONArray("datarows").getJSONArray(0).get(0));
    assertEquals("Duke", result.getJSONArray("datarows").getJSONArray(0).get(1));
    assertEquals("A", result.getJSONArray("datarows").getJSONArray(0).get(2));
    assertEquals("D", result.getJSONArray("datarows").getJSONArray(0).get(3));
  }

  @Test
  public void testRexComplexPattern() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "source=%s | rex field=email"
                    + " \\\"(?<user>[a-zA-Z0-9._%%+-]+)@(?<domain>[a-zA-Z0-9.-]+)\\\\\\\\.(?<tld>[a-zA-Z]{2,})\\\""
                    + " | fields email, user, domain, tld",
                TEST_INDEX_ACCOUNT));

    assertEquals(1000, result.getJSONArray("datarows").length());
    assertEquals("amberduke@pyrami.com", result.getJSONArray("datarows").getJSONArray(0).get(0));
    assertEquals("amberduke", result.getJSONArray("datarows").getJSONArray(0).get(1));
    assertEquals("pyrami", result.getJSONArray("datarows").getJSONArray(0).get(2));
    assertEquals("com", result.getJSONArray("datarows").getJSONArray(0).get(3));
  }

  @Test
  public void testRexWithWhere() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "source=%s | where state=\\\"CA\\\" | rex field=email"
                    + " \\\"(?<user>[^@]+)@(?<domain>.+)\\\" | fields email, user, domain",
                TEST_INDEX_ACCOUNT));

    assertTrue(result.getJSONArray("datarows").length() > 0);
    String email = result.getJSONArray("datarows").getJSONArray(0).get(0).toString();
    String user = result.getJSONArray("datarows").getJSONArray(0).get(1).toString();
    String domain = result.getJSONArray("datarows").getJSONArray(0).get(2).toString();
    assertTrue(email.startsWith(user));
    assertTrue(email.endsWith(domain));
  }

  @Test
  public void testRexWithStatsCommand() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "source=%s | rex field=email \\\"[^@]+@(?<domain>[^.]+)\\\" | stats count() by"
                    + " domain",
                TEST_INDEX_ACCOUNT));

    assertTrue(result.getJSONArray("datarows").length() > 0);
    int count = Integer.parseInt(result.getJSONArray("datarows").getJSONArray(0).get(0).toString());
    String domain = result.getJSONArray("datarows").getJSONArray(0).get(1).toString();
    assertTrue(count > 0);
    assertFalse(domain.contains("@"));
    assertTrue(domain.matches("[a-z]+"));
  }

  @Test
  public void testRexMaxMatchZeroLimitedToDefaultTen() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "source=%s | rex field=address \\\"(?<digit>\\\\\\\\d*)\\\" max_match=0 | eval"
                    + " digit_count=array_length(digit) | fields address, digit_count | head 1",
                TEST_INDEX_ACCOUNT));

    assertEquals(1, result.getJSONArray("datarows").length());
    // Should be capped at 10 matches
    assertEquals(10, result.getJSONArray("datarows").getJSONArray(0).get(1));
  }

  @Test
  public void testRexMaxMatchExceedsDefaultLimit() throws IOException {
    try {
      executeQuery(
          String.format(
              "source=%s | rex field=address \\\"(?<digit>\\\\\\\\d+)\\\" max_match=100 | fields"
                  + " address, digit",
              TEST_INDEX_ACCOUNT));
      fail("Should have thrown an exception for max_match exceeding default limit");
    } catch (Exception e) {
      assertTrue(e.getMessage().contains("exceeds the configured limit (10)"));
      assertTrue(e.getMessage().contains("Consider using a smaller max_match value"));
    }
  }

  @Test
  public void testRexMaxMatchWithinDefaultLimit() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "source=%s | rex field=address \\\"(?<digit>\\\\\\\\d*)\\\" max_match=5 | eval"
                    + " digit_count=array_length(digit) | fields address, digit_count | head 1",
                TEST_INDEX_ACCOUNT));

    assertEquals(1, result.getJSONArray("datarows").length());
    // Should respect the specified limit of 5
    assertEquals(5, result.getJSONArray("datarows").getJSONArray(0).get(1));
  }

  @Test
  public void testRexMaxMatchAtDefaultLimit() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "source=%s | rex field=address \\\"(?<digit>\\\\\\\\d*)\\\" max_match=10 | eval"
                    + " digit_count=array_length(digit) | fields address, digit_count | head 1",
                TEST_INDEX_ACCOUNT));

    assertEquals(1, result.getJSONArray("datarows").length());
    // Should accept exactly the limit
    assertEquals(10, result.getJSONArray("datarows").getJSONArray(0).get(1));
  }

  @Test
  public void testRexMaxMatchConfigurableLimit() throws IOException {
    // Set a custom limit of 5
    updateClusterSettings(
        new ClusterSetting(PERSISTENT, Settings.Key.PPL_REX_MAX_MATCH_LIMIT.getKeyValue(), "5"));

    try {
      // Test that max_match=0 is capped to the new limit
      JSONObject result =
          executeQuery(
              String.format(
                  "source=%s | rex field=address \\\"(?<digit>\\\\\\\\d*)\\\" max_match=0 | eval"
                      + " digit_count=array_length(digit) | fields address, digit_count | head 1",
                  TEST_INDEX_ACCOUNT));

      assertEquals(1, result.getJSONArray("datarows").length());
      // Should be capped at the configured limit of 5
      assertEquals(5, result.getJSONArray("datarows").getJSONArray(0).get(1));

      // Test that exceeding the custom limit throws an error
      try {
        executeQuery(
            String.format(
                "source=%s | rex field=address \\\"(?<digit>\\\\\\\\d+)\\\" max_match=10 | fields"
                    + " address, digit",
                TEST_INDEX_ACCOUNT));
        fail("Should have thrown an exception for max_match exceeding custom limit");
      } catch (Exception e) {
        assertTrue(e.getMessage().contains("exceeds the configured limit (5)"));
        assertTrue(e.getMessage().contains("adjust the plugins.ppl.rex.max_match.limit setting"));
      }
    } finally {
      updateClusterSettings(
          new ClusterSetting(PERSISTENT, Settings.Key.PPL_REX_MAX_MATCH_LIMIT.getKeyValue(), null));
    }
  }

  @Test
  public void testRexNestedCaptureGroupsBugFix() throws IOException {
    JSONObject resultWithNested =
        executeQuery(
            String.format(
                "source=%s | rex field=email"
                    + " \\\"(?<user>[^@]+)@(?<domain>(pyrami|gmail|yahoo))\\\\\\\\.(?<tld>(com|org|net))\\\""
                    + " | fields user, domain, tld | head 1",
                TEST_INDEX_ACCOUNT));

    assertEquals(1, resultWithNested.getJSONArray("datarows").length());
    assertEquals(
        "amberduke",
        resultWithNested
            .getJSONArray("datarows")
            .getJSONArray(0)
            .get(0)); // user should be "amberduke"
    assertEquals(
        "pyrami",
        resultWithNested
            .getJSONArray("datarows")
            .getJSONArray(0)
            .get(1)); // domain should be "pyrami", NOT "amberduke"
    assertEquals(
        "com",
        resultWithNested
            .getJSONArray("datarows")
            .getJSONArray(0)
            .get(2)); // tld should be "com", NOT "pyrami"

    // More complex nested alternation
    JSONObject complexNested =
        executeQuery(
            String.format(
                "source=%s | rex field=firstname"
                    + " \\\"(?<initial>(A|B|C|D|E))[a-z]*(?<suffix>(ley|nne|ber|ton|son))\\\" |"
                    + " fields initial, suffix | head 1",
                TEST_INDEX_ACCOUNT));

    if (!complexNested.getJSONArray("datarows").isEmpty()) {
      String initial = complexNested.getJSONArray("datarows").getJSONArray(0).getString(0);
      String suffix = complexNested.getJSONArray("datarows").getJSONArray(0).getString(1);

      assertTrue("Initial should be a single letter A-E", initial.matches("[A-E]"));
      assertTrue(
          "Suffix should match alternation pattern", suffix.matches("(ley|nne|ber|ton|son)"));
    }
  }
}
