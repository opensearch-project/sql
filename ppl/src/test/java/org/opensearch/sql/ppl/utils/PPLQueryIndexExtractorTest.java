/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ppl.utils;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.List;
import org.junit.Test;
import org.opensearch.sql.ast.tree.UnresolvedPlan;
import org.opensearch.sql.ppl.AstPlanningTestBase;

/** Unit tests for {@link PPLQueryIndexExtractor}. */
public class PPLQueryIndexExtractorTest extends AstPlanningTestBase {

  private List<String> indices(String query) {
    return PPLQueryIndexExtractor.extractIndexNames((UnresolvedPlan) plan(query));
  }

  @Test
  public void singleSource() {
    assertEquals(List.of("accounts"), indices("source=accounts | fields firstname"));
  }

  @Test
  public void indexKeyword() {
    assertEquals(List.of("logs"), indices("search index=logs"));
  }

  @Test
  public void multipleCommaSeparatedSourcesWithSpace() {
    // The reviewer's case: "source=accounts, account2" must keep BOTH indices — the old regex
    // stopped at the whitespace after the comma and dropped account2.
    assertEquals(List.of("accounts", "account2"), indices("source=accounts, account2"));
  }

  @Test
  public void multipleCommaSeparatedSourcesNoSpace() {
    assertEquals(List.of("a", "b", "c"), indices("source=a,b,c | stats count()"));
  }

  @Test
  public void joinCapturesBothSides() {
    List<String> result = indices("source=t1 | join on t1.id = t2.id t2");
    assertTrue(result.contains("t1"));
    assertTrue(result.contains("t2"));
  }

  @Test
  public void lookupCapturesLookupTable() {
    List<String> result = indices("source=t1 | lookup t2 id");
    assertTrue(result.contains("t1"));
    assertTrue(result.contains("t2"));
  }

  @Test
  public void datasourceQualifiedName() {
    assertEquals(List.of("myds.myindex"), indices("source=myds.myindex | head 1"));
  }

  @Test
  public void filterAndFieldsDoNotAddIndices() {
    assertEquals(List.of("accounts"), indices("source=accounts | where age > 30 | fields name"));
  }

  @Test
  public void nullPlanYieldsEmpty() {
    assertTrue(PPLQueryIndexExtractor.extractIndexNames(null).isEmpty());
  }
}
