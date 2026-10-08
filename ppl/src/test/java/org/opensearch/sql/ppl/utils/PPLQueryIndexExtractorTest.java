/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ppl.utils;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.Test;
import org.opensearch.sql.ast.expression.DataType;
import org.opensearch.sql.ast.expression.Field;
import org.opensearch.sql.ast.expression.Let;
import org.opensearch.sql.ast.expression.Literal;
import org.opensearch.sql.ast.expression.QualifiedName;
import org.opensearch.sql.ast.expression.UnresolvedExpression;
import org.opensearch.sql.ast.tree.Aggregation;
import org.opensearch.sql.ast.tree.AppendCol;
import org.opensearch.sql.ast.tree.AppendPipe;
import org.opensearch.sql.ast.tree.Eval;
import org.opensearch.sql.ast.tree.FillNull;
import org.opensearch.sql.ast.tree.Filter;
import org.opensearch.sql.ast.tree.Foreach;
import org.opensearch.sql.ast.tree.Join;
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

  // Every command whose expression the engine evaluates can carry a subquery. These were found by
  // probing each expression-bearing command; all but eventstats/streamstats execute on a live
  // cluster, so the inner source is a real read.

  @Test
  public void joinOnConditionSubqueryCapturesInnerSource() {
    // Documented form: `on l.a = r.a AND r.a in [ source = inner | fields d ]` (subquery.md).
    List<String> result =
        indices(
            "source = t1 | inner join left = l right = r"
                + " on l.a = r.a AND r.a in [ source = inner | fields d ] t2");
    assertEquals(List.of("t1", "t2", "inner"), result);
  }

  @Test
  public void joinOnConditionAndRightSubsearchBothCaptured() {
    List<String> result =
        indices(
            "source = t1 | inner join left = l right = r on l.a = r.a AND r.a in [ source = inner |"
                + " fields d ] [ source = t2 | fields a ]");
    assertEquals(List.of("t1", "t2", "inner"), result);
  }

  @Test
  public void statsArgumentSubqueryCapturesInnerSource() {
    List<String> result = indices("source=outer | stats max([ source=inner | stats max(b) ])");
    assertEquals(List.of("outer", "inner"), result);
  }

  @Test
  public void eventstatsArgumentSubqueryCapturesInnerSource() {
    List<String> result = indices("source=outer | eventstats max([ source=inner | stats max(b) ])");
    assertEquals(List.of("outer", "inner"), result);
  }

  @Test
  public void streamstatsArgumentSubqueryCapturesInnerSource() {
    List<String> result =
        indices("source=outer | streamstats max([ source=inner | stats max(b) ])");
    assertEquals(List.of("outer", "inner"), result);
  }

  @Test
  public void chartArgumentSubqueryCapturesInnerSource() {
    List<String> result = indices("source=outer | chart max([ source=inner | stats max(b) ]) by c");
    assertEquals(List.of("outer", "inner"), result);
  }

  @Test
  public void timechartArgumentSubqueryCapturesInnerSource() {
    List<String> result = indices("source=outer | timechart max([ source=inner | stats max(b) ])");
    assertEquals(List.of("outer", "inner"), result);
  }

  @Test
  public void fillnullReplacementSubqueryCapturesInnerSource() {
    assertEquals(
        List.of("outer", "inner"),
        indices("source=outer | fillnull with [ source=inner | stats max(b) ] in a"));
    assertEquals(
        List.of("outer", "inner"),
        indices("source=outer | fillnull value=[ source=inner | stats max(b) ]"));
    assertEquals(
        List.of("outer", "inner"),
        indices("source=outer | fillnull using a = [ source=inner | stats max(b) ]"));
  }

  @Test
  public void foreachAssignmentSubqueryCapturesInnerSource() {
    List<String> result =
        indices("source=outer | foreach a [ eval x = [ source=inner | stats max(b) ] ]");
    assertEquals(List.of("outer", "inner"), result);
  }

  @Test
  public void aliasedExpressionIsWalkedThrough() {
    // Alias keeps Node's null getChild(); without an explicit bridge the aggregate beneath it, and
    // any subquery in its argument, is invisible and the walk itself NPEs.
    List<String> result =
        indices("source=outer | stats max([ source=inner | stats max(b) ]) as m by c");
    assertEquals(List.of("outer", "inner"), result);
  }

  @Test
  public void existsSubqueryCapturesInnerSource() {
    // exists takes a different AST node than `in`, so it needs its own visit to be reached.
    List<String> result = indices("source=outer | where exists [ source=inner | where a = b ]");
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

  // The walk guards against absent sub-plans and sub-expressions because it runs on best-effort
  // metadata: a shape it did not expect must cost the query nothing. The parser does not produce
  // these shapes, so they are built by hand.

  @Test
  public void nullSourceInGetSourcesIsSkipped() {
    UnresolvedPlan declaresNullSource =
        new UnresolvedPlan() {
          @Override
          public List<UnresolvedPlan> getSources() {
            return Collections.singletonList(null);
          }

          @Override
          public List<UnresolvedPlan> getChild() {
            return List.of();
          }

          @Override
          public UnresolvedPlan attach(UnresolvedPlan child) {
            return this;
          }
        };
    assertEquals(List.of(), PPLQueryIndexExtractor.extractIndexNames(declaresNullSource));
  }

  @Test
  public void filterWithoutConditionStillWalksItsChild() {
    UnresolvedPlan plan = new Filter(null).attach(baseRelation());
    assertEquals(List.of("base"), PPLQueryIndexExtractor.extractIndexNames(plan));
  }

  @Test
  public void evalWithoutExpressionsStillWalksItsChild() {
    UnresolvedPlan plan = new Eval(null).attach(baseRelation());
    assertEquals(List.of("base"), PPLQueryIndexExtractor.extractIndexNames(plan));
  }

  @Test
  public void planWithNullChildListIsWalkedAsLeaf() {
    // Node.getChild() defaults to null; a plan that keeps the default must read as "no children",
    // not NPE, and still contribute its declared sources.
    UnresolvedPlan nullChildren =
        new UnresolvedPlan() {
          @Override
          public List<UnresolvedPlan> getSources() {
            return List.of(baseRelation());
          }

          @Override
          public UnresolvedPlan attach(UnresolvedPlan child) {
            return this;
          }
        };
    assertEquals(List.of("base"), PPLQueryIndexExtractor.extractIndexNames(nullChildren));
  }

  @Test
  public void nullEntryInChildListIsSkipped() {
    UnresolvedPlan holeyChildren =
        new UnresolvedPlan() {
          @Override
          public List<UnresolvedPlan> getChild() {
            return Arrays.asList(null, baseRelation());
          }

          @Override
          public UnresolvedPlan attach(UnresolvedPlan child) {
            return this;
          }
        };
    assertEquals(List.of("base"), PPLQueryIndexExtractor.extractIndexNames(holeyChildren));
  }

  @Test
  public void aggregationWithNullExpressionListsStillWalksItsChild() {
    UnresolvedPlan plan = new Aggregation(null, null, null).attach(baseRelation());
    assertEquals(List.of("base"), PPLQueryIndexExtractor.extractIndexNames(plan));
  }

  @Test
  public void fillnullWithNullOrMissingReplacementsStillWalksItsChild() {
    assertEquals(
        List.of("base"),
        PPLQueryIndexExtractor.extractIndexNames(
            FillNull.ofVariousValue(null).attach(baseRelation())));
    List<Pair<Field, UnresolvedExpression>> holey = Arrays.asList(null, Pair.of(null, null));
    assertEquals(
        List.of("base"),
        PPLQueryIndexExtractor.extractIndexNames(
            FillNull.ofVariousValue(holey).attach(baseRelation())));
  }

  @Test
  public void foreachWithNullOrMissingClausesStillWalksItsChild() {
    assertEquals(
        List.of("base"),
        PPLQueryIndexExtractor.extractIndexNames(
            new Foreach(null, null, null, null, null).attach(baseRelation())));
    List<Foreach.ForeachEvalClause> holey =
        Arrays.asList(null, new Foreach.ForeachEvalClause("x", null));
    assertEquals(
        List.of("base"),
        PPLQueryIndexExtractor.extractIndexNames(
            new Foreach(null, null, null, null, holey).attach(baseRelation())));
  }

  @Test
  public void joinWithNullConditionStillWalksBothSides() {
    UnresolvedPlan plan =
        new Join(
                new Relation(new QualifiedName("right")),
                Optional.empty(),
                Optional.empty(),
                Join.JoinType.INNER,
                null,
                new Join.JoinHint(),
                Optional.empty(),
                new org.opensearch.sql.ast.expression.Argument.ArgumentMap(List.of()))
            .attach(baseRelation());
    assertEquals(List.of("base", "right"), PPLQueryIndexExtractor.extractIndexNames(plan));
  }

  @Test
  public void evalWithNullAssignmentOrExpressionStillWalksItsChild() {
    // Covers both halves of the guard: a null Let, and a Let carrying no expression.
    Let noExpression = new Let(new Field(new QualifiedName("x")), null);
    List<Let> lets = Arrays.asList(null, noExpression);
    UnresolvedPlan plan = new Eval(lets).attach(baseRelation());
    assertEquals(List.of("base"), PPLQueryIndexExtractor.extractIndexNames(plan));
  }
}
