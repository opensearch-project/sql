package org.opensearch.sql.ppl.antlr;

import static org.junit.Assert.assertThrows;

import org.antlr.v4.runtime.tree.ParseTree;
import org.junit.Test;
import org.mockito.Mockito;
import org.opensearch.sql.common.antlr.SyntaxCheckException;
import org.opensearch.sql.common.setting.Settings;
import org.opensearch.sql.ppl.parser.AstBuilder;
import org.opensearch.sql.ppl.parser.AstStatementBuilder;

/**
 * Unit test to reproduce issue #5067
 * https://github.com/opensearch-project/sql/issues/5067
 */
public class Issue5067Test {

  private final Settings settings = Mockito.mock(Settings.class);

  @Test
  public void testDuplicateSourceKeywordShouldThrowSyntaxError() {
    // This query has duplicate "source" keyword: "source source=..."
    // Expected: SyntaxCheckException
    // Actual (bug): Query gets parsed and later throws QueryShardException
    String query = "source source=test-index | head 10";
    PPLSyntaxParser parser = new PPLSyntaxParser();
    ParseTree tree = parser.parse(query);
    
    assertThrows(
        SyntaxCheckException.class,
        () -> tree.accept(
            new AstStatementBuilder(
                new AstBuilder(query, settings),
                AstStatementBuilder.StatementBuilderContext.builder().build())));
  }

  @Test
  public void testCorrectSourceSyntaxShouldParse() {
    // This is the correct syntax
    String query = "source=test-index | head 10";
    PPLSyntaxParser parser = new PPLSyntaxParser();
    ParseTree tree = parser.parse(query);
    
    // Should not throw
    tree.accept(
        new AstStatementBuilder(
            new AstBuilder(query, settings),
            AstStatementBuilder.StatementBuilderContext.builder().build()));
  }

  @Test
  public void testDuplicateIndexKeywordShouldThrowSyntaxError() {
    // Similar issue with INDEX keyword
    String query = "index index=test-index | head 10";
    PPLSyntaxParser parser = new PPLSyntaxParser();
    ParseTree tree = parser.parse(query);
    
    assertThrows(
        SyntaxCheckException.class,
        () -> tree.accept(
            new AstStatementBuilder(
                new AstBuilder(query, settings),
                AstStatementBuilder.StatementBuilderContext.builder().build())));
  }

  @Test
  public void testSearchWithDuplicateSourceShouldThrowSyntaxError() {
    // With explicit SEARCH keyword
    String query = "search source source=test-index | head 10";
    PPLSyntaxParser parser = new PPLSyntaxParser();
    ParseTree tree = parser.parse(query);
    
    assertThrows(
        SyntaxCheckException.class,
        () -> tree.accept(
            new AstStatementBuilder(
                new AstBuilder(query, settings),
                AstStatementBuilder.StatementBuilderContext.builder().build())));
  }

  @Test
  public void testSearchingForLiteralSourceValueShouldWork() {
    // Searching for the literal word "source" in a field should still work
    String query = "source=test-index field=\"source\" | head 10";
    PPLSyntaxParser parser = new PPLSyntaxParser();
    ParseTree tree = parser.parse(query);
    
    // Should not throw - this is a valid search for the literal word "source"
    tree.accept(
        new AstStatementBuilder(
            new AstBuilder(query, settings),
            AstStatementBuilder.StatementBuilderContext.builder().build()));
  }
}
