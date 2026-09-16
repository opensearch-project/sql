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
import org.opensearch.sql.ppl.AstPlanningTestBase;

/**
 * Which sources the request's time bounds are encoded into: the outermost pipeline's own source,
 * and nothing else. A secondary source keeps every row -- a client splices its time filter after
 * the first command only -- so narrowing one would drop indices nothing filtered.
 */
public class AstTimeBoundsTest extends AstPlanningTestBase {

  private static final TimeBounds BOUNDS = new TimeBounds("ts", "now-7d", "now");
  private static final String ENCODED = "<ts,now-7d,now>";

  @Test
  public void shouldNarrowTheSearchedSource() {
    assertEquals(Set.of("logs-*"), narrowedSources("source=logs-*"));
  }

  @Test
  public void shouldLeaveAJoinsOtherSideAlone() {
    assertEquals(
        Set.of("logs-*"),
        narrowedSources("source=logs-* | inner join left=l right=r on l.id = r.id refs-*"));
  }

  @Test
  public void shouldLeaveASubsearchsSourceAlone() {
    assertEquals(
        Set.of("outer"), narrowedSources("source=outer | where a in [ source=inner | fields a ]"));
  }

  /**
   * No outermost source of its own, so there is nothing a client's time filter is known to cover.
   */
  @Test
  public void shouldNarrowNoDatasetOfAMultisearch() {
    assertEquals(
        Set.of(), narrowedSources("| multisearch [ search source=a ] [ search source=b ]"));
  }

  /** A dimension table, not a searched source. */
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
    ParseTree cst = parser.parse(query);
    Node plan = cst.accept(new AstBuilder(query, settings, bounds));
    return plan.toString();
  }
}
