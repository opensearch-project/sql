/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ppl.parser;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.when;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.opensearch.sql.ast.analysis.FieldResolutionResult;
import org.opensearch.sql.ast.analysis.FieldResolutionVisitor;
import org.opensearch.sql.ast.tree.Relation;
import org.opensearch.sql.ast.tree.SPath;
import org.opensearch.sql.ast.tree.UnresolvedPlan;
import org.opensearch.sql.common.setting.Settings;
import org.opensearch.sql.ppl.antlr.PPLSyntaxParser;

public class FieldResolutionVisitorTest {

  private final FieldResolutionVisitor visitor = new FieldResolutionVisitor();
  private final PPLSyntaxParser parser = new PPLSyntaxParser();
  private Settings settings;

  @Before
  public void setUp() {
    settings = Mockito.mock(Settings.class);
    when(settings.getSettingValue(Settings.Key.PPL_REX_MAX_MATCH_LIMIT)).thenReturn(10);
  }

  @Test
  public void testSimpleRelation() {
    assertSingleRelationFields("source=logs", Set.of(), "*");
  }

  @Test
  public void testFilterOnly() {
    assertSingleRelationFields("source=logs | where status > 200", Set.of("status"), "*");
  }

  @Test
  public void testMultipleFilters() {
    assertSingleRelationFields(
        "source=logs | where status > 200 AND region = 'us-west'", Set.of("status", "region"), "*");
  }

  @Test
  public void testProjectOnly() {
    assertSingleRelationFields(
        "source=logs | fields status, region", Set.of("status", "region"), "");
  }

  @Test
  public void testMultipleProject() {
    assertSingleRelationFields(
        "source=logs | fields status, region, *age | fields message, st*",
        Set.of("status", "message"),
        "(st*) & (*age)");
  }

  @Test
  public void testFilterThenProject() {
    assertSingleRelationFields(
        "source=logs | where status > 200 | fields region", Set.of("region", "status"), "");
  }

  @Test
  public void testAggregationWithGroupBy() {
    assertSingleRelationFields("source=logs | stats count() by region", Set.of("region"), "");
  }

  @Test
  public void testAggregationWithFieldAndGroupBy() {
    assertSingleRelationFields(
        "source=logs | stats avg(response_time) by region", Set.of("region", "response_time"), "");
  }

  @Test
  public void testComplexQuery() {
    assertSingleRelationFields(
        "source=logs | where status > 200 | stats count() by region",
        Set.of("region", "status"),
        "");
  }

  @Test
  public void testSortCommand() {
    assertSingleRelationFields("source=logs | sort status", Set.of("status"), "*");
  }

  @Test
  public void testEvalCommand() {
    assertSingleRelationFields(
        "source=logs | eval new_field = old_field + 1", Set.of("old_field"), "*");
  }

  @Test
  public void testEvalThenFilter() {
    assertSingleRelationFields(
        "source=logs | eval doubled = value * 2 | where doubled > 100", Set.of("value"), "*");
  }

  @Test
  public void testNestedFields() {
    assertSingleRelationFields(
        "source=logs | where `user.name` = 'john'", Set.of("user.name"), "*");
  }

  @Test
  public void testFunctionInFilter() {
    assertSingleRelationFields("source=logs | where length(message) > 100", Set.of("message"), "*");
  }

  @Test
  public void testMultipleAggregations() {
    assertSingleRelationFields(
        "source=logs | stats count(), avg(response_time), max(bytes) by region, status",
        Set.of("region", "status", "response_time", "bytes"),
        "");
  }

  @Test
  public void testComplexNestedQuery() {
    assertSingleRelationFields(
        "source=logs | where status > 200 AND region = 'us-west' "
            + "| eval response_ms = response_time * 1000 "
            + "| stats avg(response_ms), max(bytes) by region, status "
            + "| sort region",
        Set.of("status", "region", "response_time", "bytes"),
        "");
  }

  @Test
  public void testWildcardPatternMerging() {
    assertSingleRelationFields(
        "source=logs | fields `prefix*`, `prefix_sub*`", Set.of(), "prefix* | prefix_sub*");
  }

  @Test
  public void testSingleWildcardPattern() {
    assertSingleRelationFields("source=logs | fields `prefix*`", Set.of(), "prefix*");
  }

  @Test
  public void testWildcardWithRegularFields() {
    assertSingleRelationFields(
        "source=logs | fields status, `prefix*`, region", Set.of("status", "region"), "prefix*");
  }

  @Test
  public void testMultiRelationResult() {
    UnresolvedPlan plan = parse("source=logs | where status > 200");
    Map<UnresolvedPlan, FieldResolutionResult> results = visitor.analyze(plan);

    assertEquals(1, results.size());

    UnresolvedPlan relation = results.keySet().iterator().next();
    assertTrue(relation instanceof Relation);
    assertEquals("logs", ((Relation) relation).getTableQualifiedName().toString());

    FieldResolutionResult result = results.get(relation);
    assertEquals(Set.of("status"), result.getRegularFields());
    assertEquals("*", result.getWildcard().toString());
  }

  @Test
  public void testMvCombineAddsTargetFieldToRequirements() {
    assertSingleRelationFields("source=logs | mvcombine packets_str", Set.of("packets_str"), "*");
  }

  @Test
  public void testMvCombineAddsWildcard() {
    assertSingleRelationFields("source=logs | mvcombine packets_str", Set.of("packets_str"), "*");
  }

  @Test
  public void testSimpleJoin() {
    assertMultiRelationFields(
        "source=logs1 | join left=l right=r ON l.id = r.id logs2",
        Map.of(
            "logs1", new FieldResolutionResult(Set.of("id"), "*"),
            "logs2", new FieldResolutionResult(Set.of("id"), "*")));
  }

  @Test
  public void testJoinWithFilter() {
    assertMultiRelationFields(
        "source=logs1 | where status > 200 | join left=l right=r ON l.id = r.id logs2",
        Map.of(
            "logs1", new FieldResolutionResult(Set.of("status", "id"), "*"),
            "logs2", new FieldResolutionResult(Set.of("id"), "*")));
  }

  @Test
  public void testJoinWithProject() {
    assertMultiRelationFields(
        "source=logs1 | join left=l right=r ON l.id = r.id logs2 | fields l.name, r.value",
        Map.of(
            "logs1", new FieldResolutionResult(Set.of("name", "id")),
            "logs2", new FieldResolutionResult(Set.of("value", "id"))));
  }

  @Test
  public void testJoinWithNestedFields() {
    assertMultiRelationFields(
        "source=logs1 | join left=l right=r ON l.id = r.id logs2 | fields l.name, r.value, field,"
            + " nested.field",
        Map.of(
            "logs1", new FieldResolutionResult(Set.of("name", "id", "field", "nested.field")),
            "logs2", new FieldResolutionResult(Set.of("value", "id", "field", "nested.field"))));
  }

  @Test
  public void testSelfJoin() {
    UnresolvedPlan plan =
        parse("source=logs | fields id | join left=l right=r ON l.id = r.parent_id logs");
    Map<UnresolvedPlan, FieldResolutionResult> results = visitor.analyze(plan);

    assertEquals(2, results.size());

    for (Map.Entry<UnresolvedPlan, FieldResolutionResult> entry : results.entrySet()) {
      assertTrue(entry.getKey() instanceof Relation);
      Relation relation = (Relation) entry.getKey();
      FieldResolutionResult result = entry.getValue();
      String tableName = relation.getTableQualifiedName().toString();

      assertEquals("logs", tableName);
      Set<String> fields = result.getRegularFields();
      assertEquals(1, fields.size());
      if (fields.contains("id")) {
        assertEquals("", result.getWildcard().toString());
      } else {
        assertTrue(fields.contains("parent_id"));
        assertEquals("*", result.getWildcard().toString());
      }
    }
  }

  @Test
  public void testJoinWithAggregation() {
    assertMultiRelationFields(
        "source=logs1 | join left=l right=r ON l.id = r.id logs2 | stats count() by l.region",
        Map.of(
            "logs1", new FieldResolutionResult(Set.of("region", "id")),
            "logs2", new FieldResolutionResult(Set.of("id"))));
  }

  @Test
  public void testJoinWithSubsearch() {
    assertMultiRelationFields(
        "source=idx1 | where b > 1 | join a [source=idx2 | where c > 2 ] | eval result = c * d",
        Map.of(
            "idx1", new FieldResolutionResult(Set.of("a", "b", "c", "d"), "*"),
            "idx2", new FieldResolutionResult(Set.of("a", "c", "d"), "*")));
  }

  @Test
  public void testWhereWithSubsearch() {
    assertThrows(
        "Filter by subquery is not supported with field resolution.",
        IllegalArgumentException.class,
        () ->
            visitor.analyze(
                parse(
                    "source=idx1 | where b in [source=idx2 | where a > 2 | fields b] | fields"
                        + " c, d")));
  }

  @Test
  public void testRegexCommand() {
    assertSingleRelationFields("source=logs | regex status='error.*'", Set.of("status"), "*");
  }

  @Test
  public void testRexCommand() {
    assertSingleRelationFields(
        "source=logs | rex field=message \"(?<user>[^@]+)@(?<domain>.+)\" | fields user, domain,"
            + " other*",
        Set.of("message"),
        "other*");
  }

  @Test
  public void testPatternsCommand() {
    assertSingleRelationFields(
        "source=logs | patterns message by other method=brain mode=aggregation"
            + " show_numbered_token=true | fields patterns_field, pattern_count, tokens, other*",
        Set.of("message", "other"),
        "other*");
  }

  @Test
  public void testDedupeCommand() {
    assertSingleRelationFields("source=logs | dedup host, status", Set.of("host", "status"), "*");
  }

  @Test
  public void testReverseCommand() {
    assertSingleRelationFields("source=logs | reverse", Set.of(), "*");
  }

  @Test
  public void testHeadCommand() {
    assertSingleRelationFields("source=logs | head 10", Set.of(), "*");
  }

  @Test
  public void testRenameCommand() {
    assertSingleRelationFields("source=logs | rename old_name as new_name", Set.of(), "*");
  }

  @Test
  public void testFillnull() {
    assertSingleRelationFields(
        "source=logs | fillnull with 'NULL' in a, b | fields c, *", Set.of("a", "b", "c"), "*");
    assertSingleRelationFields(
        "source=logs | fillnull using a = 'NULL', b = 'NULL' | fields c, *",
        Set.of("a", "b", "c"),
        "*");
    assertSingleRelationFields(
        "source=logs | fillnull value='NULL' a, b | fields c, *", Set.of("a", "b", "c"), "*");
  }

  @Test
  public void testFillnullWithoutFields() {
    assertThrows(
        "Fields need to be specified with fillnull command",
        IllegalArgumentException.class,
        () -> visitor.analyze(parse("source=logs | fillnull with 'NULL'")));
  }

  @Test
  public void testReplaceCommand() {
    assertSingleRelationFields(
        "source=logs | replace 'IL' WITH 'Illinois' IN a, b | fields c, *",
        Set.of("a", "b", "c"),
        "*");
  }

  @Test
  public void testSpathCommand() {
    String query = "source=logs | spath input=json | fields a, *";
    assertSingleRelationFields(query, Set.of("a", "json"), "*");
    assertSingleSpathFields(query, Set.of("a"), "*");
  }

  @Test
  public void testSpathTwice() {
    assertSingleRelationFields(
        "source=logs | spath input=json | spath input=doc | fields a, *",
        Set.of("a", "doc", "json"),
        "*");
  }

  @Test
  public void testMvExpandCommand() {
    assertSingleRelationFields("source=logs | mvexpand skills", Set.of("skills"), "*");
  }

  @Test
  public void testMvExpandCommandWithLimit() {
    assertSingleRelationFields("source=logs | mvexpand skills limit=5", Set.of("skills"), "*");
  }

  @Test
  public void testUnimplementedVisitDetected() {
    assertThrows(
        "Unsupported command for field resolution: Kmeans",
        IllegalArgumentException.class,
        () -> visitor.analyze(parse("source=idx1 | kmeans centroids=3")));
  }

  @Test
  public void testAppend() {
    String query =
        "source=main | where testCase='simple' | eval c = 4 | "
            + "append [source=sub | where testCase='simple' ] | fields a, c, *";
    assertMultiRelationFields(
        query,
        Map.of(
            "main", new FieldResolutionResult(Set.of("a", "testCase"), "*"),
            "sub", new FieldResolutionResult(Set.of("a", "c", "testCase"), "*")));
  }

  @Test
  public void testAppendCol() {
    String query =
        "source=main | where testCase='simple' | eval c = 4 | "
            + "appendcol [where testCase='simple' ] | fields a, c, *";
    assertMultiRelationFields(
        query, Map.of("main", new FieldResolutionResult(Set.of("a", "testCase"), "*")));
  }

  @Test
  public void testAppendpipe() {
    String query =
        "source=main | where testCase='simple' | stats sum(a) as sum_a by b | "
            + "appendpipe [stats sum(sum_a) as total] | head 5";
    assertMultiRelationFields(
        query, Map.of("main", new FieldResolutionResult(Set.of("a", "b", "testCase"))));
  }

  @Test
  public void testMultisearch() {
    String query =
        "| multisearch [source=main | where testCase='simple'] [source=sub | where"
            + " testCase='simple'] | fields a, c, *";
    assertMultiRelationFields(
        query,
        Map.of(
            "main",
            new FieldResolutionResult(Set.of("a", "c", "testCase"), "*"),
            "sub",
            new FieldResolutionResult(Set.of("a", "c", "testCase"), "*")));
  }

  @Test
  public void testAppendWithSpathInMain() {
    String query =
        "source=main | where testCase='simple' | spath input=doc | "
            + "append [source=sub | where testCase='simple' | eval d = 4] | fields a, c, *";
    assertMultiRelationFields(
        query,
        Map.of(
            "main", new FieldResolutionResult(Set.of("a", "c", "doc", "testCase"), "*"),
            "sub", new FieldResolutionResult(Set.of("a", "c", "testCase"), "*")));
    assertSingleSpathFields(query, Set.of("a", "c"), "*");
  }

  @Test
  public void testAppendWithSpathSubquery() {
    String query =
        "source=main | where testCase='simple' | append [source=sub | where testCase='simple' |"
            + " spath input=doc | eval c = 4] | fields a, c, *";
    assertMultiRelationFields(
        query,
        Map.of(
            "main", new FieldResolutionResult(Set.of("a", "c", "testCase"), "*"),
            "sub", new FieldResolutionResult(Set.of("a", "doc", "testCase"), "*")));
    assertSingleSpathFields(query, Set.of("a"), "*");
  }

  private UnresolvedPlan parse(String query) {
    AstBuilder astBuilder = new AstBuilder(query, settings);
    return astBuilder.visit(parser.parse(query));
  }

  private FieldResolutionResult getSingleRelationResult(String query) {
    UnresolvedPlan plan = parse(query);
    Map<UnresolvedPlan, FieldResolutionResult> results = visitor.analyze(plan);
    return getRelationResult(results);
  }

  private FieldResolutionResult getRelationResult(
      Map<UnresolvedPlan, FieldResolutionResult> results) {
    for (UnresolvedPlan key : results.keySet()) {
      if (key instanceof Relation) {
        return results.get(key);
      }
    }
    fail("Relation result not found");
    return null;
  }

  private FieldResolutionResult getSingleSpathResult(String query) {
    UnresolvedPlan plan = parse(query);
    Map<UnresolvedPlan, FieldResolutionResult> results = visitor.analyze(plan);
    return getSpathResult(results);
  }

  private FieldResolutionResult getSpathResult(Map<UnresolvedPlan, FieldResolutionResult> results) {
    for (UnresolvedPlan key : results.keySet()) {
      if (key instanceof SPath) {
        return results.get(key);
      }
    }
    fail("Spath result not found");
    return null;
  }

  private void assertSingleSpathFields(
      String query, Set<String> expectedFields, String expectedWildcard) {
    FieldResolutionResult result = getSingleSpathResult(query);
    assertEquals(expectedFields, result.getRegularFields());
    assertEquals(expectedWildcard, result.getWildcard().toString());
  }

  private void assertSingleRelationFields(
      String query, Set<String> expectedFields, String expectedWildcard) {
    FieldResolutionResult result = getSingleRelationResult(query);
    assertEquals(expectedFields, result.getRegularFields());
    assertEquals(expectedWildcard, result.getWildcard().toString());
  }

  private void assertMultiRelationFields(
      String query, Map<String, FieldResolutionResult> expectedResultsByTable) {
    UnresolvedPlan plan = parse(query);
    Map<UnresolvedPlan, FieldResolutionResult> results = visitor.analyze(plan);

    Set<String> foundTables = new HashSet<>();
    for (Map.Entry<UnresolvedPlan, FieldResolutionResult> entry : results.entrySet()) {
      if (!(entry.getKey() instanceof Relation)) {
        continue;
      }
      String tableName = ((Relation) entry.getKey()).getTableQualifiedName().toString();
      FieldResolutionResult expectedResult = expectedResultsByTable.get(tableName);

      if (expectedResult != null) {
        assertEquals(expectedResult.getRegularFields(), entry.getValue().getRegularFields());
        assertEquals(expectedResult.getWildcard(), entry.getValue().getWildcard());
        foundTables.add(tableName);
      }
    }

    assertEquals(expectedResultsByTable.size(), foundTables.size());
  }
}
