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
    String query = Index.ACCOUNT.pplSearch_("age=20");
    queryShouldPassSyntaxAndSemanticCheck(query);
  }

  @Test
  public void whereCommandShouldPassSemanticCheck() {
    String query = Index.ACCOUNT.pplSearch("where age=20");
    queryShouldPassSyntaxAndSemanticCheck(query);
  }

  @Test
  public void fieldsCommandShouldPassSemanticCheck() {
    String query = Index.ACCOUNT.pplSearch("fields firstname");
    queryShouldPassSyntaxAndSemanticCheck(query);
  }

  @Test
  public void renameCommandShouldPassSemanticCheck() {
    String query = Index.ACCOUNT.pplSearch("rename firstname as first");
    queryShouldPassSyntaxAndSemanticCheck(query);
  }

  @Test
  public void statsCommandShouldPassSemanticCheck() {
    String query = Index.ACCOUNT.pplSearch("stats avg(age)");
    queryShouldPassSyntaxAndSemanticCheck(query);
  }

  @Test
  public void dedupCommandShouldPassSemanticCheck() {
    String query = Index.ACCOUNT.pplSearch("dedup firstname, lastname");
    queryShouldPassSyntaxAndSemanticCheck(query);
  }

  @Test
  public void sortCommandShouldPassSemanticCheck() {
    String query = Index.ACCOUNT.pplSearch("sort age");
    queryShouldPassSyntaxAndSemanticCheck(query);
  }

  @Test
  public void evalCommandShouldPassSemanticCheck() {
    String query = Index.ACCOUNT.pplSearch("eval age=abs(age)");
    queryShouldPassSyntaxAndSemanticCheck(query);
  }

  @Test
  public void queryShouldBeCaseInsensitiveInKeywords() {
    String query = String.format("SEARCH SourCE=%s", TEST_INDEX_ACCOUNT);
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
    String query = Index.ACCOUNT.pplSearch("field firstname");
    queryShouldThrowSyntaxException(query, SYNTAX_EX_MSG_FRAGMENT);
  }

  @Test
  public void queryWithIncorrectKeywordsShouldFailSyntaxCheck() {
    String query = String.format("search sources=%s", TEST_INDEX_ACCOUNT);
    queryShouldThrowSyntaxException(query, SYNTAX_EX_MSG_FRAGMENT);
  }

  @Test
  public void unsupportedAggregationFunctionShouldFailSyntaxCheck() {
    String query = Index.ACCOUNT.pplSearch("stats range(age)");
    queryShouldThrowSyntaxException(query, SYNTAX_EX_MSG_FRAGMENT);
  }

  /** Commands that fail semantic analysis should throw {@link SemanticCheckException}. */
  @Test
  public void nonexistentFieldShouldFailSemanticCheck() {
    String query = Index.ACCOUNT.pplSearch("fields name");
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
