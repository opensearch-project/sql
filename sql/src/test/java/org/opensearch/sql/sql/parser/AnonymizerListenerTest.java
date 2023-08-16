/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.sql.parser;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;

import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.tree.ErrorNode;
import org.junit.jupiter.api.Test;
import org.opensearch.sql.common.antlr.CaseInsensitiveCharStream;
import org.opensearch.sql.sql.antlr.AnonymizerListener;
import org.opensearch.sql.sql.antlr.parser.OpenSearchSQLLexer;
import org.opensearch.sql.sql.antlr.parser.OpenSearchSQLParser;

public class AnonymizerListenerTest {

  private final AnonymizerListener anonymizerListener = new AnonymizerListener();

  /**
   * Helper function to parse SQl queries for testing purposes.
   * @param query SQL query to be anonymized.
   */
  private void parse(String query) {
    OpenSearchSQLLexer lexer = new OpenSearchSQLLexer(new CaseInsensitiveCharStream(query));
    OpenSearchSQLParser parser = new OpenSearchSQLParser(new CommonTokenStream(lexer));
    parser.addParseListener(anonymizerListener);

    parser.root();
  }

  @Test
  public void queriesShouldHaveAnonymousFieldAndIndex() {
    String query = "SELECT ABS(balance) FROM accounts WHERE age > 30 GROUP BY ABS(balance)";
    String expectedQuery = "( SELECT ABS ( identifier ) FROM table "
        + "WHERE identifier > number GROUP BY ABS ( identifier ) )";
    parse(query);
    assertEquals(expectedQuery, anonymizerListener.getAnonymizedQueryString());
  }

  @Test
  public void queriesShouldAnonymousNumbers() {
    String query = "SELECT ABS(20), LOG(20.20) FROM accounts";
    String expectedQuery = "( SELECT ABS ( number ), LOG ( number ) FROM table )";
    parse(query);
    assertEquals(expectedQuery, anonymizerListener.getAnonymizedQueryString());
  }

  @Test
  public void queriesShouldHaveAnonymousBooleanLiterals() {
    String query = "SELECT TRUE FROM accounts";
    String expectedQuery = "( SELECT boolean_literal FROM table )";
    parse(query);
    assertEquals(expectedQuery, anonymizerListener.getAnonymizedQueryString());
  }

  @Test
  public void queriesShouldHaveAnonymousInputStrings() {
    String query = "SELECT * FROM accounts WHERE name = 'Oliver'";
    String expectedQuery = "( SELECT * FROM table WHERE identifier = 'string_literal' )";
    parse(query);
    assertEquals(expectedQuery, anonymizerListener.getAnonymizedQueryString());
  }

  @Test
  public void queriesWithAliasesShouldAnonymizeSensitiveData() {
    String query = "SELECT balance AS b FROM accounts AS a";
    String expectedQuery = "( SELECT identifier AS identifier FROM table AS identifier )";
    parse(query);
    assertEquals(expectedQuery, anonymizerListener.getAnonymizedQueryString());
  }

  @Test
  public void queriesWithFunctionsShouldAnonymizeSensitiveData() {
    String query = "SELECT LTRIM(firstname) FROM accounts";
    String expectedQuery = "( SELECT LTRIM ( identifier ) FROM table )";
    parse(query);
    assertEquals(expectedQuery, anonymizerListener.getAnonymizedQueryString());
  }

  @Test
  public void queriesWithAggregatesShouldAnonymizeSensitiveData() {
    String query = "SELECT MAX(price) - MIN(price) from tickets";
    String expectedQuery = "( SELECT MAX ( identifier ) - MIN ( identifier ) FROM table )";
    parse(query);
    assertEquals(expectedQuery, anonymizerListener.getAnonymizedQueryString());
  }

  @Test
  public void queriesWithSubqueriesShouldAnonymizeSensitiveData() {
    String query = "SELECT a.f, a.l, a.a FROM "
        + "(SELECT firstname AS f, lastname AS l, age AS a FROM accounts WHERE age > 30) a";
    String expectedQuery =
        "( SELECT identifier.identifier, identifier.identifier, identifier.identifier FROM "
        + "( SELECT identifier AS identifier, identifier AS identifier, identifier AS identifier "
        + "FROM table WHERE identifier > number ) identifier )";
    parse(query);
    assertEquals(expectedQuery, anonymizerListener.getAnonymizedQueryString());
  }

  @Test
  public void queriesWithLimitShouldAnonymizeSensitiveData() {
    String query = "SELECT balance FROM accounts LIMIT 5";
    String expectedQuery = "( SELECT identifier FROM table LIMIT number )";
    parse(query);
    assertEquals(expectedQuery, anonymizerListener.getAnonymizedQueryString());
  }

  @Test
  public void queriesWithOrderByShouldAnonymizeSensitiveData() {
    String query = "SELECT firstname FROM accounts ORDER BY lastname";
    String expectedQuery = "( SELECT identifier FROM table ORDER BY identifier )";
    parse(query);
    assertEquals(expectedQuery, anonymizerListener.getAnonymizedQueryString());
  }

  @Test
  public void queriesWithHavingShouldAnonymizeSensitiveData() {
    String query = "SELECT SUM(balance) FROM accounts GROUP BY lastname HAVING COUNT(balance) > 2";
    String expectedQuery = "( SELECT SUM ( identifier ) FROM table "
        + "GROUP BY identifier HAVING COUNT ( identifier ) > number )";
    parse(query);
    assertEquals(expectedQuery, anonymizerListener.getAnonymizedQueryString());
  }

  @Test
  public void queriesWithHighlightShouldAnonymizeSensitiveData() {
    String query = "SELECT HIGHLIGHT(str0) FROM CALCS WHERE QUERY_STRING(['str0'], 'FURNITURE')";
    String expectedQuery = "( SELECT HIGHLIGHT ( identifier ) FROM table WHERE "
        + "QUERY_STRING ( [ 'string_literal' ], 'string_literal' ) )";
    parse(query);
    assertEquals(expectedQuery, anonymizerListener.getAnonymizedQueryString());
  }

  @Test
  public void queriesWithMatchShouldAnonymizeSensitiveData() {
    String query = "SELECT str0 FROM CALCS WHERE MATCH(str0, 'FURNITURE')";
    String expectedQuery = "( SELECT identifier FROM table "
        + "WHERE MATCH ( identifier, 'string_literal' ) )";
    parse(query);
    assertEquals(expectedQuery, anonymizerListener.getAnonymizedQueryString());
  }

  @Test
  public void queriesWithPositionShouldAnonymizeSensitiveData() {
    String query = "SELECT POSITION('world' IN 'helloworld')";
    String expectedQuery = "( SELECT POSITION ( 'string_literal' IN 'string_literal' ) )";
    parse(query);
    assertEquals(expectedQuery, anonymizerListener.getAnonymizedQueryString());
  }

  @Test
  public void queriesWithMatch_Bool_Prefix_ShouldAnonymizeSensitiveData() {
    String query = "SELECT firstname, address FROM accounts WHERE "
        + "match_bool_prefix(address, 'Bristol Street', minimum_should_match=2)";
    String expectedQuery = "( SELECT identifier, identifier FROM table WHERE MATCH_BOOL_PREFIX "
        + "( identifier, 'string_literal', MINIMUM_SHOULD_MATCH = number ) )";
    parse(query);
    assertEquals(expectedQuery, anonymizerListener.getAnonymizedQueryString());
  }

  @Test
  public void queriesWithGreaterOrEqualShouldAnonymizeSensitiveData() {
    String query = "SELECT int0 FROM accounts WHERE int0 >= 0";
    String expectedQuery = "( SELECT identifier FROM table WHERE identifier >= number )";
    parse(query);
    assertEquals(expectedQuery, anonymizerListener.getAnonymizedQueryString());
  }

  @Test
  public void queriesWithLessOrEqualShouldAnonymizeSensitiveData() {
    String query = "SELECT int0 FROM accounts WHERE int0 <= 0";
    String expectedQuery = "( SELECT identifier FROM table WHERE identifier <= number )";
    parse(query);
    assertEquals(expectedQuery, anonymizerListener.getAnonymizedQueryString());
  }

  @Test
  public void queriesWithNotEqualShouldAnonymizeSensitiveData() {
    String query = "SELECT int0 FROM accounts WHERE int0 != 0";
    String expectedQuery = "( SELECT identifier FROM table WHERE identifier != number )";
    parse(query);
    assertEquals(expectedQuery, anonymizerListener.getAnonymizedQueryString());
  }

  @Test
  public void queriesWithNotEqualAlternateShouldAnonymizeSensitiveData() {
    String query = "SELECT int0 FROM calcs WHERE int0 <> 0";
    String expectedQuery = "( SELECT identifier FROM table WHERE identifier <> number )";
    parse(query);
    assertEquals(expectedQuery, anonymizerListener.getAnonymizedQueryString());
  }


  /**
   * Test added for coverage, but the errorNode will not be hit normally.
   */
  @Test
  public void enterErrorNote() {
    ErrorNode node = mock(ErrorNode.class);
    anonymizerListener.visitErrorNode(node);
  }
}
