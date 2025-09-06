/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ppl;

import static org.opensearch.sql.legacy.TestsConstants.SYNTAX_EX_MSG_FRAGMENT;
import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_ACCOUNT;

import java.io.IOException;
import org.junit.Test;
import org.opensearch.client.ResponseException;
import org.opensearch.sql.common.antlr.SyntaxCheckException;
import org.opensearch.sql.exception.SemanticCheckException;

public class QueryAnalysisIT extends PPLIntegTestCase {
  @Override
  public void init() throws Exception {
    super.init();
    loadIndex(Index.ACCOUNT);
  }

  /** Valid commands should pass both syntax analysis and semantic check. */
  @Test
  public void searchCommandShouldPassSemanticCheck() {
    String query = "search " + withSource(TEST_INDEX_ACCOUNT, "age=20");
    queryShouldPassSyntaxAndSemanticCheck(query);
  }

  @Test
  public void whereCommandShouldPassSemanticCheck() {
    String query = "search " + withSource(TEST_INDEX_ACCOUNT, "where age=20");
    queryShouldPassSyntaxAndSemanticCheck(query);
  }

  @Test
  public void fieldsCommandShouldPassSemanticCheck() {
    String query = "search " + withSource(TEST_INDEX_ACCOUNT, "fields firstname");
    queryShouldPassSyntaxAndSemanticCheck(query);
  }

  @Test
  public void renameCommandShouldPassSemanticCheck() {
    String query = "search " + withSource(TEST_INDEX_ACCOUNT, "rename firstname as first");
    queryShouldPassSyntaxAndSemanticCheck(query);
  }

  @Test
  public void statsCommandShouldPassSemanticCheck() {
    String query = "search " + withSource(TEST_INDEX_ACCOUNT, "stats avg(age)");
    queryShouldPassSyntaxAndSemanticCheck(query);
  }

  @Test
  public void dedupCommandShouldPassSemanticCheck() {
    String query = "search " + withSource(TEST_INDEX_ACCOUNT, "dedup firstname, lastname");
    queryShouldPassSyntaxAndSemanticCheck(query);
  }

  @Test
  public void sortCommandShouldPassSemanticCheck() {
    String query = "search " + withSource(TEST_INDEX_ACCOUNT, "sort age");
    queryShouldPassSyntaxAndSemanticCheck(query);
  }

  @Test
  public void evalCommandShouldPassSemanticCheck() {
    String query = "search " + withSource(TEST_INDEX_ACCOUNT, "eval age=abs(age)");
    queryShouldPassSyntaxAndSemanticCheck(query);
    queryShouldPassSyntaxAndSemanticCheck(query);
  }

  @Test
  public void queryShouldBeCaseInsensitiveInKeywords() {
    String query = "SEARCH " + withSource(TEST_INDEX_ACCOUNT, "");
    queryShouldPassSyntaxAndSemanticCheck(query);
  }

  /** Commands that fail syntax analysis should throw {@link SyntaxCheckException}. */
  @Test
  public void queryNotStartingWithSearchCommandShouldFailSyntaxCheck() {
    String query = "fields firstname";
    queryShouldThrowSyntaxException(query, SYNTAX_EX_MSG_FRAGMENT);
  }

  @Test
  public void queryWithIncorrectCommandShouldFailSyntaxCheck() {
    String query = "search " + withSource(TEST_INDEX_ACCOUNT, "field firstname");
    queryShouldThrowSyntaxException(query, SYNTAX_EX_MSG_FRAGMENT);
  }

  @Test
  public void queryWithIncorrectKeywordsShouldFailSyntaxCheck() {
    String query = String.format("search sources=%s", TEST_INDEX_ACCOUNT);
    queryShouldThrowSyntaxException(query, SYNTAX_EX_MSG_FRAGMENT);
  }

  @Test
  public void unsupportedAggregationFunctionShouldFailSyntaxCheck() {
    String query = "search " + withSource(TEST_INDEX_ACCOUNT, "stats range(age)");
    queryShouldThrowSyntaxException(query, SYNTAX_EX_MSG_FRAGMENT);
  }

  /** Commands that fail semantic analysis should throw {@link SemanticCheckException}. */
  @Test
  public void nonexistentFieldShouldFailSemanticCheck() {
    String query = "search " + withSource(TEST_INDEX_ACCOUNT, "fields name");
    queryShouldThrowSemanticException(
        query, "can't resolve Symbol(namespace=FIELD_NAME, name=name) in type env");
  }

  private void queryShouldPassSyntaxAndSemanticCheck(String query) {
    try {
      executeQuery(query);
    } catch (SemanticCheckException e) {
      fail("Expected to pass semantic check but failed for query: " + query);
    } catch (IOException e) {
      throw new IllegalStateException("Unexpected IOException raised for query: " + query);
    }
  }

  private void queryShouldThrowSyntaxException(String query, String... messages) {
    try {
      executeQuery(query);
      fail("Expected to throw SyntaxCheckException, but none was thrown for query: " + query);
    } catch (ResponseException e) {
      String errorMsg = e.getMessage();
      assertTrue(errorMsg.contains("SyntaxCheckException"));
      for (String msg : messages) {
        assertTrue(errorMsg.contains(msg));
      }
    } catch (IOException e) {
      throw new IllegalStateException("Unexpected exception raised for query: " + query);
    }
  }

  private void queryShouldThrowSemanticException(String query, String... messages) {
    try {
      executeQuery(query);
      fail("Expected to throw SemanticCheckException, but none was thrown for query: " + query);
    } catch (ResponseException e) {
      String errorMsg = e.getMessage();
      assertTrue(errorMsg.contains("SemanticCheckException"));
      for (String msg : messages) {
        assertTrue(errorMsg.contains(msg));
      }
    } catch (IOException e) {
      throw new IllegalStateException("Unexpected exception raised for query: " + query);
    }
  }
}
