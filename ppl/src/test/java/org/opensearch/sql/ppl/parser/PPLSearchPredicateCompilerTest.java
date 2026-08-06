/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ppl.parser;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

import org.junit.Test;
import org.opensearch.sql.exception.SemanticCheckException;

public class PPLSearchPredicateCompilerTest {

  private final PPLSearchPredicateCompiler compiler = PPLSearchPredicateCompiler.INSTANCE;

  @Test
  public void testCompileCanonicalFormatOutput() {
    assertEquals(
        "(((status:500 AND host:api\\-01)))",
        compiler.compile("( ( status=\"500\" AND host=\"api-01\" ) )"));
  }

  @Test
  public void testCompileRuntimeSearchFieldValue() {
    assertEquals(
        "(status:>=500 OR host:api\\-01)", compiler.compile("status>=500 OR host=\"api-01\""));
  }

  @Test
  public void testCompileBacktickQuotedFieldName() {
    assertEquals("display\\ name:value", compiler.compile("`display name`=\"value\""));
  }

  @Test
  public void testCompileEmptyFormatResultAsMatchNone() {
    assertEquals("*:* AND NOT *:*", compiler.compile("NOT ()"));
  }

  @Test
  public void testCompileEmptyRawSearchFieldAsMatchAll() {
    assertEquals("*:*", compiler.compile(""));
    assertEquals("*:*", compiler.compile(null));
    assertEquals("*:*", compiler.compile("   "));
  }

  @Test
  public void testCompileEmptyFormatResultIgnoresCaseAndWhitespace() {
    assertEquals("*:* AND NOT *:*", compiler.compile("not (  )"));
  }

  @Test
  public void testGeneratedPredicateCannotInjectPipelineCommands() {
    SemanticCheckException exception =
        assertThrows(SemanticCheckException.class, () -> compiler.compile("status=500 | head 1"));

    assertEquals(
        "The subsearch produced a value that is not a valid search predicate. Ensure the 'search'"
            + " field contains a search expression and does not include pipeline commands.",
        exception.getMessage());
  }
}
