/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ppl.parser;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.HashSet;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.antlr.v4.runtime.tree.ParseTree;
import org.junit.Test;
import org.opensearch.sql.ast.Node;
import org.opensearch.sql.executor.TimeBounds;
import org.opensearch.sql.ppl.antlr.PPLSyntaxParser;

/**
 * Which sources the request's time bounds are encoded into. They narrow what the query searches, so
 * a lookup's dimension table is left alone -- it holds a static dataset with no window to speak of.
 */
public class AstTimeBoundsTest {

  private static final TimeBounds BOUNDS = new TimeBounds("ts", "now-7d", "now");
  private static final String ENCODED = "<ts,now-7d,now>";

  @Test
  public void shouldNarrowTheSearchedSource() {
    assertEquals(Set.of("logs-*"), narrowedSources("source=logs-*"));
  }

  @Test
  public void shouldNarrowEverySourceOfAMultisearch() {
    assertEquals(
        Set.of("a", "b"), narrowedSources("| multisearch [ search source=a ] [ search source=b ]"));
  }

  @Test
  public void shouldNarrowASubsearchsSource() {
    assertEquals(
        Set.of("outer", "inner"),
        narrowedSources("source=outer | where a in [ source=inner | fields a ]"));
  }

  /** A dimension table, not a searched source: the source is narrowed, the lookup index is not. */
  @Test
  public void shouldLeaveALookupTableAlone() {
    String plan = plan("source=logs-* | lookup countries id", BOUNDS);

    assertTrue(plan, plan.contains("logs-*" + ENCODED));
    assertFalse(plan, plan.contains("countries" + ENCODED));
  }

  @Test
  public void shouldNarrowNothingWithoutBounds() {
    assertEquals(Set.of(), narrowedSources("source=logs-* | lookup countries id", null));
  }

  private Set<String> narrowedSources(String query) {
    return narrowedSources(query, BOUNDS);
  }

  /** The names the bounds were encoded into, however often the rendered plan repeats a node. */
  private Set<String> narrowedSources(String query, TimeBounds bounds) {
    Matcher matcher =
        Pattern.compile("([\\w.*-]+)" + Pattern.quote(ENCODED)).matcher(plan(query, bounds));
    Set<String> names = new HashSet<>();
    while (matcher.find()) {
      names.add(matcher.group(1));
    }
    return names;
  }

  private String plan(String query, TimeBounds bounds) {
    ParseTree cst = new PPLSyntaxParser().parse(query);
    Node plan = cst.accept(new AstBuilder(query, null, bounds));
    return plan.toString();
  }
}
