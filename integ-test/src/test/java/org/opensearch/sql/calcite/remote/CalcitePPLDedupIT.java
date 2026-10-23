/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.remote;

import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_ACCOUNT;
import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_DUPLICATION_NULLABLE;
import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_DUPLICATION_NULLABLE_ORDERED;
import static org.opensearch.sql.util.Capability.DEDUP_NONDETERMINISTIC;
import static org.opensearch.sql.util.MatcherUtils.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import org.json.JSONArray;
import org.json.JSONObject;
import org.junit.jupiter.api.Test;
import org.opensearch.sql.ppl.PPLIntegTestCase;
import org.opensearch.sql.util.RequiresCapability;

public class CalcitePPLDedupIT extends PPLIntegTestCase {

  @Override
  public void init() throws Exception {
    super.init();
    enableCalcite();

    loadIndex(Index.DUPLICATION_NULLABLE);
    loadIndex(Index.DUPLICATION_NULLABLE_ORDERED);
    loadIndex(Index.ACCOUNT);
  }

  @Test
  public void testDedup() throws IOException {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | dedup 1 name | fields name", TEST_INDEX_DUPLICATION_NULLABLE));
    verifyDataRows(actual, rows("A"), rows("B"), rows("C"), rows("D"), rows("E"));
  }

  @Test
  public void testDedupMultipleFields() throws IOException {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | dedup 1 name, category | fields name, category",
                TEST_INDEX_DUPLICATION_NULLABLE));
    verifyDataRows(
        actual,
        rows("A", "X"),
        rows("A", "Y"),
        rows("B", "Z"),
        rows("C", "X"),
        rows("D", "Z"),
        rows("B", "Y"));
  }

  @Test
  public void testDedupKeepEmpty() throws IOException {
    // dedup 1 name KEEPEMPTY=true keeps the first row per distinct non-null name plus every
    // null-name row. An added `sort name, category` (PPL default ASC NULLS FIRST) pins each
    // non-null name's surviving row to its smallest category on any shard layout and route -- the
    // same sort-before-dedup determinism the #3922 regression tests rely on -- so the kept
    // representative is exact rather than merely "some valid pair". Per name that is A->X,
    // B->null, C->X, D->Z, E->null; the four null-name rows are always kept in full.
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | sort name, category | dedup 1 name KEEPEMPTY=true | fields name,"
                    + " category",
                TEST_INDEX_DUPLICATION_NULLABLE));
    verifyDataRows(
        actual,
        rows("A", "X"),
        rows("B", null),
        rows("C", "X"),
        rows("D", "Z"),
        rows("E", null),
        rows(null, "Y"),
        rows(null, "X"),
        rows(null, "Z"),
        rows(null, null));
  }

  @Test
  public void testDedupMultipleFieldsKeepEmpty() throws IOException {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | dedup 1 name, category KEEPEMPTY=true | fields name, category",
                TEST_INDEX_DUPLICATION_NULLABLE));
    verifyDataRows(
        actual,
        rows("A", "X"),
        rows("A", "Y"),
        rows("B", "Z"),
        rows("C", "X"),
        rows("D", "Z"),
        rows("B", "Y"),
        rows(null, "Y"),
        rows("E", null),
        rows(null, "X"),
        rows("B", null),
        rows(null, "Z"),
        rows(null, null));
  }

  /**
   * {@code CONSECUTIVE=true} collapses only <em>adjacent</em> duplicates, so its result depends
   * entirely on the row-encounter order. A multi-shard index has no stable merge order (the counts
   * observed on a single shard, 8/12/12/16, become 12/... on five shards). To keep the real index
   * route while making the encounter order deterministic, this drives a seq-augmented fixture
   * ({@code duplication_nullable_ordered}, same rows plus an explicit {@code seq}) and adds {@code
   * | sort seq} before dedup, restoring the historical {@code duplication_nullable} insertion
   * sequence on any shard layout while still exercising real CONSECUTIVE semantics over the index.
   * The AE route has no stable per-fragment tiebreaker (DEDUP_NONDETERMINISTIC), so the assertion
   * stays gated to the routes that produce a deterministic ordered stream.
   */
  @Test
  @RequiresCapability(
      value = DEDUP_NONDETERMINISTIC,
      note = "dedup CONSECUTIVE behavior diverges on the AE route.")
  public void testConsecutiveImplicitFallbackV2() throws IOException {
    JSONObject actual =
        executeQuery(
            String.format(
                "source = %s | sort seq | dedup 1 name CONSECUTIVE=true | fields name",
                TEST_INDEX_DUPLICATION_NULLABLE_ORDERED));
    verifyNumOfRows(actual, 8);

    actual =
        executeQuery(
            String.format(
                "source = %s | sort seq | dedup 1 name KEEPEMPTY=true CONSECUTIVE=true | fields"
                    + " name",
                TEST_INDEX_DUPLICATION_NULLABLE_ORDERED));
    verifyNumOfRows(actual, 12);

    actual =
        executeQuery(
            String.format(
                "source = %s | sort seq | dedup 2 name CONSECUTIVE=true | fields name",
                TEST_INDEX_DUPLICATION_NULLABLE_ORDERED));
    verifyNumOfRows(actual, 12);

    actual =
        executeQuery(
            String.format(
                "source = %s | sort seq | dedup 2 name KEEPEMPTY=true CONSECUTIVE=true | fields"
                    + " name",
                TEST_INDEX_DUPLICATION_NULLABLE_ORDERED));
    verifyNumOfRows(actual, 16);
  }

  @Test
  public void testDedup2() throws IOException {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | dedup 2 name | fields name", TEST_INDEX_DUPLICATION_NULLABLE));
    verifyDataRows(
        actual, rows("A"), rows("A"), rows("B"), rows("B"), rows("C"), rows("C"), rows("D"),
        rows("E"));
  }

  @Test
  public void testDedupMultipleFields2() throws IOException {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | dedup 2 name, category | fields name, category",
                TEST_INDEX_DUPLICATION_NULLABLE));
    verifyDataRows(
        actual,
        rows("A", "X"),
        rows("A", "X"),
        rows("A", "Y"),
        rows("A", "Y"),
        rows("B", "Y"),
        rows("B", "Z"),
        rows("B", "Z"),
        rows("C", "X"),
        rows("C", "X"),
        rows("D", "Z"));
  }

  @Test
  public void testDedupKeepEmpty2() throws IOException {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | dedup 2 name KEEPEMPTY=true | fields name, category",
                TEST_INDEX_DUPLICATION_NULLABLE));
    // dedup 2 keeps up to two rows per distinct non-null name (A/B/C have >=2, D/E have 1) plus
    // every null-name row. Which two categories survive per name has no stable cross-shard
    // tiebreaker, so assert the per-name kept-count, valid pairs, and the fixed null-name rows.
    List<List<Object>> rows = dataRows(actual);
    assertEquals(12, rows.size());
    Map<Object, Integer> nameCounts = new HashMap<>();
    Set<List<Object>> nullNameRows = new HashSet<>();
    for (List<Object> row : rows) {
      Object name = row.get(0);
      Object category = row.get(1);
      if (name == null) {
        nullNameRows.add(Arrays.asList(name, category));
      } else {
        nameCounts.merge(name, 1, Integer::sum);
        assertValidPair(name, category);
      }
    }
    assertEquals(Map.of("A", 2, "B", 2, "C", 2, "D", 1, "E", 1), nameCounts);
    assertEquals(NULL_NAME_ROWS, nullNameRows);
  }

  @Test
  public void testDedupMultipleFieldsKeepEmpty2() throws IOException {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | dedup 2 name, category KEEPEMPTY=true | fields name, category",
                TEST_INDEX_DUPLICATION_NULLABLE));
    verifyDataRows(
        actual,
        rows("A", "X"),
        rows("A", "X"),
        rows("A", "Y"),
        rows("A", "Y"),
        rows("B", "Y"),
        rows("B", "Z"),
        rows("B", "Z"),
        rows("C", "X"),
        rows("C", "X"),
        rows("D", "Z"),
        rows(null, "Y"),
        rows("E", null),
        rows(null, "X"),
        rows("B", null),
        rows(null, "Z"),
        rows(null, null));
  }

  @Test
  public void testReorderDedupFieldsShouldNotAffectResult() throws IOException {
    JSONObject actual1 =
        executeQuery(
            String.format(
                "source=%s | dedup 2 name, category | fields name, category, id",
                TEST_INDEX_DUPLICATION_NULLABLE));
    verifySchemaInOrder(
        actual1,
        schema("name", null, "string"),
        schema("category", null, "string"),
        schema("id", null, "int"));
    JSONObject actual2 =
        executeQuery(
            String.format(
                "source=%s | dedup 2 category, name | fields name, category, id",
                TEST_INDEX_DUPLICATION_NULLABLE));
    verifySchemaInOrder(
        actual2,
        schema("name", null, "string"),
        schema("category", null, "string"),
        schema("id", null, "int"));
    JSONObject actual3 =
        executeQuery(
            String.format(
                "source=%s | dedup 2 name, category KEEPEMPTY=true | fields name, category, id",
                TEST_INDEX_DUPLICATION_NULLABLE));
    verifySchemaInOrder(
        actual3,
        schema("name", null, "string"),
        schema("category", null, "string"),
        schema("id", null, "int"));
    JSONObject actual4 =
        executeQuery(
            String.format(
                "source=%s | dedup 2 category, name KEEPEMPTY=true | fields name, category, id",
                TEST_INDEX_DUPLICATION_NULLABLE));
    verifySchemaInOrder(
        actual4,
        schema("name", null, "string"),
        schema("category", null, "string"),
        schema("id", null, "int"));
  }

  @Test
  @RequiresCapability(
      value = DEDUP_NONDETERMINISTIC,
      note =
          "dedup surviving-duplicate selection diverges on the AE route (no stable merge order).")
  public void testDedupComplex() throws IOException {
    JSONObject actual =
        executeQuery(String.format("source=%s | dedup 1 name", TEST_INDEX_DUPLICATION_NULLABLE));
    // dedup 1 name keeps one row per distinct non-null name. The surviving row's category (and any
    // other non-key column) has no stable cross-shard tiebreaker, so assert the dedup invariant:
    // exactly the five names, each once, and each surviving (name, category) is a real data pair.
    List<List<Object>> byName = dataRows(actual);
    assertEquals(5, byName.size());
    Set<Object> names = new HashSet<>();
    for (List<Object> row : byName) {
      Object category = row.get(0);
      Object name = row.get(1);
      names.add(name);
      assertEquals(1, ((Number) row.get(2)).intValue());
      assertValidPair(name, category);
    }
    assertEquals(Set.of("A", "B", "C", "D", "E"), names);
    actual =
        executeQuery(
            String.format(
                "source=%s | fields category, name | dedup 1 name",
                TEST_INDEX_DUPLICATION_NULLABLE));
    List<List<Object>> byNameProjected = dataRows(actual);
    assertEquals(5, byNameProjected.size());
    Set<Object> projectedNames = new HashSet<>();
    for (List<Object> row : byNameProjected) {
      Object category = row.get(0);
      Object name = row.get(1);
      projectedNames.add(name);
      assertValidPair(name, category);
    }
    assertEquals(Set.of("A", "B", "C", "D", "E"), projectedNames);
    actual =
        executeQuery(
            String.format("source=%s | dedup 1 name, category", TEST_INDEX_DUPLICATION_NULLABLE));
    verifyDataRows(
        actual,
        rows("X", "A", 1),
        rows("Y", "A", 1),
        rows("Y", "B", 1),
        rows("Z", "B", 1),
        rows("X", "C", 1),
        rows("Z", "D", 1));
    actual =
        executeQuery(
            String.format(
                "source=%s | fields category, id, name | dedup 2 name, category",
                TEST_INDEX_DUPLICATION_NULLABLE));
    verifyDataRows(
        actual,
        rows("X", 1, "A"),
        rows("X", 1, "A"),
        rows("Y", 1, "A"),
        rows("Y", 1, "A"),
        rows("Y", 1, "B"),
        rows("Z", 1, "B"),
        rows("Z", 1, "B"),
        rows("X", 1, "C"),
        rows("X", 1, "C"),
        rows("Z", 1, "D"));
  }

  /** Regression test for https://github.com/opensearch-project/sql/issues/3922 */
  @Test
  public void testSortThenDedup() throws IOException {
    // Verify sort order is preserved through dedup
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | sort category | dedup 1 name | fields category, name",
                TEST_INDEX_DUPLICATION_NULLABLE));
    // PPL default sort is ASC NULLS FIRST, so null-category rows come first in the sort.
    // For each name, dedup keeps the first row in sort order:
    //   name=A first cat=X, name=B first cat=null (row #14), name=C first cat=X,
    //   name=D first cat=Z, name=E first cat=null.
    verifyDataRows(
        actual, rows(null, "B"), rows(null, "E"), rows("X", "A"), rows("X", "C"), rows("Z", "D"));
  }

  /**
   * Regression test for multi-field sort pushed through dedup.
   *
   * <p>Verifies that when a PPL {@code sort} has multiple fields before a {@code dedup}, every
   * field is preserved through the pushdown (not only the first one). A single-field pushdown would
   * lose the tie-breaker and return a non-deterministic row for each dedup group.
   *
   * <p>Data used: the {@code accounts} test index. In state {@code AK} there are multiple F and M
   * accounts; under {@code sort state, age, account_number} the first M row is {@code (state=AK,
   * age=20, account_number=23)} and the first F row is {@code (state=AK, age=21,
   * account_number=334)}. Only a correct multi-field pushdown produces these exact rows.
   */
  @Test
  public void testMultiColumnSortThenDedup() throws IOException {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | sort state, age, account_number | dedup 1 gender | fields gender,"
                    + " state, age, account_number",
                TEST_INDEX_ACCOUNT));
    verifyDataRows(actual, rows("M", "AK", 20, 23), rows("F", "AK", 21, 334));
  }

  /**
   * Regression test for https://github.com/opensearch-project/sql/issues/5150
   *
   * <p>A renamed field that is not the dedup key must retain its value after dedup aggregation
   * pushdown. Previously the top_hits response returned the original index field name ({@code
   * name}), which the enumerator could not resolve to the renamed output name ({@code nm}),
   * yielding null.
   */
  @Test
  public void testDedupWithRenamedField() throws IOException {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | where isnotnull(name) | rename name as nm | sort nm | dedup 1 category"
                    + " | fields category, nm",
                TEST_INDEX_DUPLICATION_NULLABLE));
    // Pin the surviving representative deterministically across shards: excluding null names and
    // sorting by nm makes dedup keep the lexicographically-first name per category (X->A, Y->A,
    // Z->B). This still exercises the #5150 fix (the renamed non-key field must resolve to a
    // non-null value in the dedup top_hits response).
    verifyDataRows(actual, rows("X", "A"), rows("Z", "B"), rows("Y", "A"));
  }

  /**
   * Regression test for https://github.com/opensearch-project/sql/issues/5197
   *
   * <p>When both a {@code rename} and an {@code eval} column-reference resolve to the same original
   * index field, the old {@code Map&lt;String,String&gt;} mapping silently dropped one alias on
   * collision. With {@code Map&lt;String,List&lt;String&gt;&gt;} both aliases must appear in the
   * result with correct values.
   */
  @Test
  public void testDedupWithRenamedFieldMappingCollision() throws IOException {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | where isnotnull(name) | eval nm2 = name | rename name as nm | sort nm"
                    + " | dedup 1 category | fields category, nm, nm2",
                TEST_INDEX_DUPLICATION_NULLABLE));
    // Pin the surviving representative deterministically across shards (see testDedupWithRenamed
    // Field). Both nm (from rename) and nm2 (from the eval col-ref) must carry the same non-null
    // name value, exercising the #5197 alias-collision fix.
    verifyDataRows(actual, rows("X", "A", "A"), rows("Z", "B", "B"), rows("Y", "A", "A"));
  }

  /** Regression test for https://github.com/opensearch-project/sql/issues/3922 */
  @Test
  public void testSortThenDedupKeepEmpty() throws IOException {
    // Verify sort order is preserved through dedup with keepempty=true
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | sort category | dedup 1 name KEEPEMPTY=true | fields category, name",
                TEST_INDEX_DUPLICATION_NULLABLE));
    // category should be in ascending order (with nulls first due to ASC-nulls-first)
    // dedup 1 name KEEPEMPTY=true: keep first occurrence of each name, plus ALL null-name rows
    verifyDataRows(
        actual,
        rows(null, null),
        rows(null, "B"),
        rows(null, "E"),
        rows("X", null),
        rows("X", "A"),
        rows("X", "C"),
        rows("Y", null),
        rows("Z", null),
        rows("Z", "D"));
  }

  @Test
  @RequiresCapability(
      value = DEDUP_NONDETERMINISTIC,
      note =
          "dedup surviving-duplicate selection diverges on the AE route (no stable merge order).")
  public void testDedupExpr() throws IOException {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | eval new_name = lower(name) | dedup 1 new_name",
                TEST_INDEX_DUPLICATION_NULLABLE));
    // dedup 1 new_name keeps one row per distinct lower(name). name and new_name are fully
    // determined (a<->A ...), but the surviving category has no stable cross-shard tiebreaker;
    // assert the five keys, the derived-column relation, and a valid (name, category) pair.
    List<List<Object>> byNewName = dataRows(actual);
    assertEquals(5, byNewName.size());
    Set<Object> newNames = new HashSet<>();
    for (List<Object> row : byNewName) {
      Object category = row.get(0);
      Object name = row.get(1);
      Object newName = row.get(3);
      newNames.add(newName);
      assertEquals(1, ((Number) row.get(2)).intValue());
      assertEquals(((String) name).toLowerCase(Locale.ROOT), newName);
      assertValidPair(name, category);
    }
    assertEquals(Set.of("a", "b", "c", "d", "e"), newNames);
    actual =
        executeQuery(
            String.format(
                "source=%s | fields category, name, id | eval new_name = lower(name), new_category"
                    + " = lower(category) | dedup 1 new_name, new_category",
                TEST_INDEX_DUPLICATION_NULLABLE));
    verifyDataRows(
        actual,
        rows("X", "C", 1, "c", "x"),
        rows("Z", "D", 1, "d", "z"),
        rows("X", "A", 1, "a", "x"),
        rows("Y", "B", 1, "b", "y"),
        rows("Y", "A", 1, "a", "y"),
        rows("Z", "B", 1, "b", "z"));
    actual =
        executeQuery(
            String.format(
                "source=%s | eval new_name = lower(name), new_category = lower(category) | dedup 2"
                    + " name, category",
                TEST_INDEX_DUPLICATION_NULLABLE));
    verifyDataRows(
        actual,
        rows("Y", "A", 1, "a", "y"),
        rows("Y", "A", 1, "a", "y"),
        rows("Z", "B", 1, "b", "z"),
        rows("Z", "B", 1, "b", "z"),
        rows("X", "A", 1, "a", "x"),
        rows("X", "A", 1, "a", "x"),
        rows("Y", "B", 1, "b", "y"),
        rows("Z", "D", 1, "d", "z"),
        rows("X", "C", 1, "c", "x"),
        rows("X", "C", 1, "c", "x"));
    actual =
        executeQuery(
            String.format(
                "source=%s | fields category, id, name | eval new_name = lower(name) | eval"
                    + " new_category = lower(category) | sort name, -category | dedup 2 new_name,"
                    + " new_category",
                TEST_INDEX_DUPLICATION_NULLABLE));
    verifyDataRows(
        actual,
        rows("X", 1, "C", "c", "x"),
        rows("X", 1, "C", "c", "x"),
        rows("Z", 1, "D", "d", "z"),
        rows("X", 1, "A", "a", "x"),
        rows("X", 1, "A", "a", "x"),
        rows("Y", 1, "B", "b", "y"),
        rows("Y", 1, "A", "a", "y"),
        rows("Y", 1, "A", "a", "y"),
        rows("Z", 1, "B", "b", "z"),
        rows("Z", 1, "B", "b", "z"));
  }

  // ---- Multi-shard dedup helpers ------------------------------------------------------------

  /**
   * Every (name, category) pair with a non-null name present in the {@code duplication_nullable}
   * dataset. dedup keeps one (or N) representative row(s) per key; because the merge has no stable
   * cross-shard tiebreaker, tests assert the surviving row is a real pair rather than a fixed one.
   */
  private static final Set<List<Object>> VALID_NAME_CATEGORY =
      Set.of(
          Arrays.asList("A", "X"),
          Arrays.asList("A", "Y"),
          Arrays.asList("B", "Z"),
          Arrays.asList("B", "Y"),
          Arrays.asList("B", null),
          Arrays.asList("C", "X"),
          Arrays.asList("D", "Z"),
          Arrays.asList("E", null));

  /**
   * The null-name rows kept by {@code KEEPEMPTY=true} are fully determined: dedup keys on name
   * only, so every null-name document survives, one per distinct category those rows carry.
   */
  private static final Set<List<Object>> NULL_NAME_ROWS =
      Set.of(
          Arrays.asList(null, "Y"),
          Arrays.asList(null, "X"),
          Arrays.asList(null, "Z"),
          Arrays.asList(null, null));

  private static void assertValidPair(Object name, Object category) {
    assertTrue(
        "unexpected surviving (name, category) = (" + name + ", " + category + ")",
        VALID_NAME_CATEGORY.contains(Arrays.asList(name, category)));
  }

  /** Materialize {@code datarows} into a list of rows, mapping JSON null to Java {@code null}. */
  private static List<List<Object>> dataRows(JSONObject response) {
    List<List<Object>> rows = new ArrayList<>();
    JSONArray arr = response.getJSONArray("datarows");
    for (int i = 0; i < arr.length(); i++) {
      JSONArray r = arr.getJSONArray(i);
      List<Object> row = new ArrayList<>();
      for (int j = 0; j < r.length(); j++) {
        row.add(r.isNull(j) ? null : r.get(j));
      }
      rows.add(row);
    }
    return rows;
  }
}
