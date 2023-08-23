/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.legacy.executor.format;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

public class ResultSetTest {

  private final ResultSet resultSet =
      new ResultSet() {
        @Override
        public Schema getSchema() {
          return super.getSchema();
        }
      };

  /**
   * Case #1: LIKE 'test%' is converted to:
   *
   * <ol>
   *   <li>Regex pattern: test.*
   *   <li>OpenSearch search pattern: test*
   * </ol>
   *
   * In this case, what OpenSearch returns is the final result.
   */
  @Test
  public void testWildcardForZeroOrMoreCharacters() {
    assertTrue(resultSet.matchesPatternIfRegex("test123", "test.*"));
  }

  /**
   * Case #2: LIKE 'test_123' is converted to:
   *
   * <ol>
   *   x
   *   <li>Regex pattern: test.123
   *   <li>OpenSearch search pattern: (all)
   * </ol>
   *
   * Because OpenSearch doesn't support single wildcard character, in this case, none is passed as
   * OpenSearch search pattern. So all index names are returned and need to be filtered by regex
   * pattern again.
   */
  @Test
  public void testWildcardForSingleCharacter() {
    assertFalse(resultSet.matchesPatternIfRegex("accounts", "test.23"));
    assertFalse(resultSet.matchesPatternIfRegex(".opensearch_dashboards", "test.23"));
    assertTrue(resultSet.matchesPatternIfRegex("test123", "test.23"));
  }

  /**
   * Case #3: LIKE 'acc' has same regex and OpenSearch pattern. In this case, only index name(s)
   * aliased by 'acc' is returned. So regex match is skipped to avoid wrong empty result. The
   * assumption here is OpenSearch won't return unrelated index names if LIKE pattern doesn't
   * include any wildcard.
   */
  @Test
  public void testIndexAlias() {
    assertTrue(resultSet.matchesPatternIfRegex("accounts", "acc"));
  }

  /**
   * Case #4: LIKE 'test.2020.10' has same regex pattern. Because it includes dot (wildcard),
   * OpenSearch search pattern is all. In this case, all index names are returned. Because the
   * pattern includes dot, it's treated as regex and regex match won't be skipped.
   */
  @Test
  public void testIndexNameWithDot() {
    assertFalse(resultSet.matchesPatternIfRegex("accounts", "test.2020.10"));
    assertFalse(resultSet.matchesPatternIfRegex(".opensearch_dashboards", "test.2020.10"));
    assertTrue(resultSet.matchesPatternIfRegex("test.2020.10", "test.2020.10"));
  }
}
