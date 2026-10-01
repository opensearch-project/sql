/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ppl.utils;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.List;
import org.junit.Test;
import org.opensearch.sql.ast.expression.DataType;
import org.opensearch.sql.ast.expression.Literal;
import org.opensearch.sql.ast.expression.QualifiedName;
import org.opensearch.sql.ast.tree.AppendCol;
import org.opensearch.sql.ast.tree.AppendPipe;
import org.opensearch.sql.ast.tree.Filter;
import org.opensearch.sql.ast.tree.Relation;
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
  public void appendCapturesSubSearchSource() {
    List<String> result = indices("source=accounts | append [ source=account2 | stats count() ]");
    assertTrue(result.contains("accounts"));
    assertTrue(result.contains("account2"));
  }

  @Test
  public void subqueryInWhereCapturesInnerSource() {
    List<String> result = indices("source=outer | where a in [ source=inner | fields b ]");
    assertTrue(result.contains("outer"));
    assertTrue(result.contains("inner"));
  }

  @Test
  public void evalScalarSubqueryCapturesInnerSource() {
    List<String> result = indices("source=outer | eval m = [ source=inner | stats max(b) ]");
    assertTrue(result.contains("outer"));
    assertTrue(result.contains("inner"));
  }

  @Test
  public void graphLookupCapturesFromTable() {
    List<String> result =
        indices(
            "source=t | graphLookup employees start=reportsTo edge=manager-->name"
                + " as reportingHierarchy");
    assertTrue(result.contains("t"));
    assertTrue(result.contains("employees"));
  }

  @Test
  public void appendColCapturesSubSearchSource() {
    // Built directly, not parsed: the grammar runs appendcol's sub-search over the piped input, so
    // a nested source= can't be expressed in text.
    UnresolvedPlan plan = new AppendCol(false, subPipelineOver("appended")).attach(baseRelation());
    List<String> result = PPLQueryIndexExtractor.extractIndexNames(plan);
    assertTrue(result.contains("base"));
    assertTrue(result.contains("appended"));
  }

  @Test
  public void appendPipeCapturesSubQuerySource() {
    // Built directly for the same reason as appendcol above.
    UnresolvedPlan plan = new AppendPipe(subPipelineOver("appended")).attach(baseRelation());
    List<String> result = PPLQueryIndexExtractor.extractIndexNames(plan);
    assertTrue(result.contains("base"));
    assertTrue(result.contains("appended"));
  }

  private UnresolvedPlan baseRelation() {
    return new Relation(new QualifiedName("base"));
  }

  /** A {@link Relation} nested under a piped command, like real parser output (not a bare root). */
  private UnresolvedPlan subPipelineOver(String index) {
    return new Filter(new Literal(true, DataType.BOOLEAN))
        .attach(new Relation(new QualifiedName(index)));
  }

  @Test
  public void nullPlanYieldsEmpty() {
    assertTrue(PPLQueryIndexExtractor.extractIndexNames(null).isEmpty());
  }
}
