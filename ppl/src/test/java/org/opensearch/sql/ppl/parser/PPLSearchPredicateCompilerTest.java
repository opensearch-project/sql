/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ppl.parser;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

import org.junit.Test;

public class PPLSearchPredicateCompilerTest {

  private final PPLSearchPredicateCompiler compiler = new PPLSearchPredicateCompiler();

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
  }

  @Test
  public void testGeneratedPredicateCannotInjectPipelineCommands() {
    assertThrows(RuntimeException.class, () -> compiler.compile("status=500 | head 1"));
  }
}
