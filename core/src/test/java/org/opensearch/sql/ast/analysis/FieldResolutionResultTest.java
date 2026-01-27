/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ast.analysis;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Set;
import org.junit.jupiter.api.Test;
import org.opensearch.sql.ast.analysis.FieldResolutionResult.AndWildcard;
import org.opensearch.sql.ast.analysis.FieldResolutionResult.OrWildcard;
import org.opensearch.sql.ast.analysis.FieldResolutionResult.SingleWildcard;
import org.opensearch.sql.ast.analysis.FieldResolutionResult.Wildcard;

class FieldResolutionResultTest {

  @Test
  void testSingleWildcardMatching() {
    Wildcard wildcard = new SingleWildcard("user*");

    assertTrue(wildcard.matches("user"));
    assertTrue(wildcard.matches("username"));
    assertTrue(wildcard.matches("user_id"));
    assertFalse(wildcard.matches("admin"));
    assertFalse(wildcard.matches("name"));
  }

  @Test
  void testSingleWildcardWithMultipleWildcards() {
    Wildcard wildcard = new SingleWildcard("*_id_*");

    assertTrue(wildcard.matches("user_id_123"));
    assertTrue(wildcard.matches("_id_"));
    assertTrue(wildcard.matches("prefix_id_suffix"));
    assertFalse(wildcard.matches("user_id"));
    assertFalse(wildcard.matches("id_123"));
  }

  @Test
  void testSingleWildcardExactMatch() {
    Wildcard wildcard = new SingleWildcard("exact_field");

    assertTrue(wildcard.matches("exact_field"));
    assertFalse(wildcard.matches("exact_field_suffix"));
    assertFalse(wildcard.matches("prefix_exact_field"));
  }

  @Test
  void testSingleWildcardToString() {
    Wildcard wildcard = new SingleWildcard("user*");
    assertEquals("user*", wildcard.toString());
  }

  @Test
  void testSingleWildcardEquality() {
    Wildcard w1 = new SingleWildcard("user*");
    Wildcard w2 = new SingleWildcard("user*");
    Wildcard w3 = new SingleWildcard("admin*");

    assertEquals(w1, w2);
    assertEquals(w1.hashCode(), w2.hashCode());
    assertNotEquals(w1, w3);
  }

  @Test
  void testOrWildcardMatching() {
    Wildcard wildcard = new OrWildcard(new SingleWildcard("user*"), new SingleWildcard("admin*"));

    assertTrue(wildcard.matches("username"));
    assertTrue(wildcard.matches("admin_role"));
    assertFalse(wildcard.matches("guest"));
  }

  @Test
  void testOrWildcardWithSinglePattern() {
    Wildcard wildcard = new OrWildcard(new SingleWildcard("user*"));

    assertTrue(wildcard.matches("username"));
    assertFalse(wildcard.matches("admin"));
  }

  @Test
  void testOrWildcardToString() {
    Wildcard wildcard = new OrWildcard(new SingleWildcard("user*"), new SingleWildcard("admin*"));

    assertEquals("user* | admin*", wildcard.toString());
  }

  @Test
  void testOrWildcardEquality() {
    Wildcard w1 = new OrWildcard(new SingleWildcard("user*"), new SingleWildcard("admin*"));
    Wildcard w2 = new OrWildcard(new SingleWildcard("user*"), new SingleWildcard("admin*"));
    Wildcard w3 = new OrWildcard(new SingleWildcard("guest*"), new SingleWildcard("admin*"));

    assertEquals(w1, w2);
    assertEquals(w1.hashCode(), w2.hashCode());
    assertNotEquals(w1, w3);
  }

  @Test
  void testAndWildcardMatching() {
    Wildcard wildcard = new AndWildcard(new SingleWildcard("*user*"), new SingleWildcard("*name"));

    assertTrue(wildcard.matches("username"));
    assertTrue(wildcard.matches("user_full_name"));
    assertFalse(wildcard.matches("user_id"));
    assertFalse(wildcard.matches("admin_name"));
  }

  @Test
  void testAndWildcardWithSinglePattern() {
    Wildcard wildcard = new AndWildcard(new SingleWildcard("user*"));

    assertTrue(wildcard.matches("username"));
    assertFalse(wildcard.matches("admin"));
  }

  @Test
  void testAndWildcardToString() {
    Wildcard wildcard = new AndWildcard(new SingleWildcard("*user*"), new SingleWildcard("*name"));

    assertEquals("(*user*) & (*name)", wildcard.toString());
  }

  @Test
  void testAndWildcardEquality() {
    Wildcard w1 = new AndWildcard(new SingleWildcard("*user*"), new SingleWildcard("*name"));
    Wildcard w2 = new AndWildcard(new SingleWildcard("*user*"), new SingleWildcard("*name"));
    Wildcard w3 = new AndWildcard(new SingleWildcard("*admin*"), new SingleWildcard("*name"));

    assertEquals(w1, w2);
    assertEquals(w1.hashCode(), w2.hashCode());
    assertNotEquals(w1, w3);
  }

  @Test
  void testNestedWildcardCombinations() {
    Wildcard or1 = new OrWildcard(new SingleWildcard("user*"), new SingleWildcard("admin*"));
    Wildcard or2 = new OrWildcard(new SingleWildcard("*_id"), new SingleWildcard("*_name"));
    Wildcard and = new AndWildcard(or1, or2);

    assertTrue(and.matches("user_id"));
    assertTrue(and.matches("admin_name"));
    assertFalse(and.matches("user_role"));
    assertFalse(and.matches("guest_id"));
  }

  @Test
  void testNestedWildcardToString() {
    Wildcard or1 = new OrWildcard(new SingleWildcard("user*"), new SingleWildcard("admin*"));
    Wildcard or2 = new OrWildcard(new SingleWildcard("*_id"), new SingleWildcard("*_name"));
    Wildcard and = new AndWildcard(or1, or2);

    assertEquals("(user* | admin*) & (*_id | *_name)", and.toString());
  }

  @Test
  void testFieldResolutionResultWithNoWildcard() {
    FieldResolutionResult result = new FieldResolutionResult(Set.of("field1", "field2"));

    assertFalse(result.hasWildcards());
    assertTrue(result.hasRegularFields());
    assertEquals(2, result.getRegularFields().size());
    assertTrue(result.getWildcard().toString().isEmpty());
  }

  @Test
  void testFieldResolutionResultWithEmptyFields() {
    FieldResolutionResult result = new FieldResolutionResult(Set.of());

    assertFalse(result.hasWildcards());
    assertFalse(result.hasRegularFields());
    assertEquals(0, result.getRegularFields().size());
  }

  @Test
  void testFieldResolutionResultWithNullOrEmptyWildcardString() {
    FieldResolutionResult result1 = new FieldResolutionResult(Set.of("field1"), (String) null);
    FieldResolutionResult result2 = new FieldResolutionResult(Set.of("field1"), "");

    assertTrue(result1.hasRegularFields());
    assertFalse(result1.hasWildcards());
    assertTrue(result1.getWildcard().toString().isEmpty());

    assertTrue(result2.hasRegularFields());
    assertFalse(result2.hasWildcards());
    assertTrue(result2.getWildcard().toString().isEmpty());
  }

  @Test
  void testFieldResolutionResultWithAnyWildcard() {
    FieldResolutionResult result = new FieldResolutionResult(Set.of("field1"), "*");

    assertTrue(result.hasWildcards());
    assertTrue(result.getWildcard().matches("anything"));
    assertTrue(result.getWildcard().matches("field1"));
    assertTrue(result.getWildcard().matches(""));
    assertEquals("*", result.getWildcard().toString());
  }

  @Test
  void testFieldResolutionResultWithNullOrEmptyWildcardSet() {
    FieldResolutionResult result1 = new FieldResolutionResult(Set.of("field1"), Set.of());
    FieldResolutionResult result2 = new FieldResolutionResult(Set.of("field1"), (Set<String>) null);

    assertTrue(result1.hasRegularFields());
    assertFalse(result1.hasWildcards());

    assertTrue(result2.hasRegularFields());
    assertFalse(result2.hasWildcards());
  }

  @Test
  void testGetRegularFieldsUnmodifiable() {
    FieldResolutionResult result = new FieldResolutionResult(Set.of("field1", "field2"));
    Set<String> unmodifiable = result.getRegularFieldsUnmodifiable();

    assertEquals(2, unmodifiable.size());
    assertTrue(unmodifiable.contains("field1"));
    assertTrue(unmodifiable.contains("field2"));

    try {
      unmodifiable.add("field3");
      assertTrue(false, "Should throw UnsupportedOperationException");
    } catch (UnsupportedOperationException e) {
      // Expected
    }
  }

  @Test
  void testFieldResolutionResultEquality() {
    FieldResolutionResult result1 = new FieldResolutionResult(Set.of("field1", "field2"), "user*");
    FieldResolutionResult result2 = new FieldResolutionResult(Set.of("field1", "field2"), "user*");
    FieldResolutionResult result3 = new FieldResolutionResult(Set.of("field1"), "user*");
    FieldResolutionResult result4 = new FieldResolutionResult(Set.of("field1", "field2"), "admin*");

    assertEquals(result1, result2);
    assertEquals(result1.hashCode(), result2.hashCode());
    assertNotEquals(result1, result3);
    assertNotEquals(result1, result4);
  }

  @Test
  void testFieldResolutionResultToString() {
    FieldResolutionResult result = new FieldResolutionResult(Set.of("field1", "field2"), "user*");
    String str = result.toString();

    assertTrue(str.contains("field1"));
    assertTrue(str.contains("field2"));
    assertTrue(str.contains("user*"));
  }

  @Test
  void testFieldResolutionResultWithStringWildcard() {
    FieldResolutionResult result = new FieldResolutionResult(Set.of("field1"), "user*");

    assertTrue(result.hasWildcards());
    assertTrue(result.hasRegularFields());
    assertTrue(result.getWildcard() instanceof SingleWildcard);
    assertEquals("user*", result.getWildcard().toString());
  }

  @Test
  void testFieldResolutionResultWithWildcardObject() {
    Wildcard wildcard = new SingleWildcard("admin*");
    FieldResolutionResult result = new FieldResolutionResult(Set.of("field1"), wildcard);

    assertTrue(result.hasWildcards());
    assertEquals(wildcard, result.getWildcard());
  }

  @Test
  void testFieldResolutionResultWithMultipleWildcardPatterns() {
    FieldResolutionResult result =
        new FieldResolutionResult(Set.of("field1"), Set.of("user*", "admin*"));

    assertTrue(result.hasWildcards());
    assertTrue(result.getWildcard() instanceof OrWildcard);
    assertEquals("admin* | user*", result.getWildcard().toString());
  }

  @Test
  void testFieldResolutionResultWithSingleWildcardPattern() {
    FieldResolutionResult result = new FieldResolutionResult(Set.of("field1"), Set.of("user*"));

    assertTrue(result.hasWildcards());
    assertTrue(result.getWildcard() instanceof SingleWildcard);
    assertEquals("user*", result.getWildcard().toString());
  }

  @Test
  void testFieldResolutionResultAndOperation() {
    FieldResolutionResult result1 =
        new FieldResolutionResult(Set.of("user_id", "user_name", "admin_id"), "user*");
    FieldResolutionResult result2 =
        new FieldResolutionResult(Set.of("user_id", "admin_id"), "*_id");

    FieldResolutionResult combined = result1.and(result2);

    assertEquals(Set.of("user_id", "admin_id"), combined.getRegularFields());
    assertTrue(combined.hasWildcards());
    assertTrue(combined.getWildcard() instanceof AndWildcard);
    assertEquals("(user*) & (*_id)", combined.getWildcard().toString());
  }

  @Test
  void testFieldResolutionResultAndWithWildcardMatching() {
    FieldResolutionResult result1 =
        new FieldResolutionResult(Set.of("user_id", "user_name", "admin_role"), "user*");
    FieldResolutionResult result2 = new FieldResolutionResult(Set.of("admin_id"), "*_id");

    FieldResolutionResult combined = result1.and(result2);

    assertTrue(combined.getRegularFields().contains("user_id"));
    assertFalse(combined.getRegularFields().contains("user_name"));
    assertFalse(combined.getRegularFields().contains("admin_role"));
  }

  @Test
  void testFieldResolutionResultAndWithNullWildcards() {
    FieldResolutionResult result1 = new FieldResolutionResult(Set.of("user_id", "admin_id"));
    FieldResolutionResult result2 = new FieldResolutionResult(Set.of("user_id"), "user*");

    FieldResolutionResult combined = result1.and(result2);

    assertEquals(Set.of("user_id"), combined.getRegularFields());
    assertFalse(combined.hasWildcards());
  }

  @Test
  void testFieldResolutionResultAndWithBothNullWildcards() {
    FieldResolutionResult result1 = new FieldResolutionResult(Set.of("user_id", "admin_id"));
    FieldResolutionResult result2 = new FieldResolutionResult(Set.of("user_id"));

    FieldResolutionResult combined = result1.and(result2);

    assertEquals(Set.of("user_id"), combined.getRegularFields());
    assertFalse(combined.hasWildcards());
  }

  @Test
  void testWildcardAnyAndAny() {
    FieldResolutionResult result1 = new FieldResolutionResult(Set.of("a"), "*");
    FieldResolutionResult result2 = new FieldResolutionResult(Set.of("b"), "*");

    FieldResolutionResult combined = result1.and(result2);

    assertEquals(Set.of("a", "b"), combined.getRegularFields());
    assertEquals("*", combined.getWildcard().toString());
  }

  @Test
  void testWildcardAnyAndNull() {
    FieldResolutionResult result1 = new FieldResolutionResult(Set.of("a"), "*");
    FieldResolutionResult result2 = new FieldResolutionResult(Set.of("b"));

    FieldResolutionResult combined = result1.and(result2);

    assertEquals(Set.of("b"), combined.getRegularFields());
    assertEquals("", combined.getWildcard().toString());
  }

  @Test
  void testFieldResolutionResultOrOperation() {
    FieldResolutionResult result = new FieldResolutionResult(Set.of("field1"), "user*");
    FieldResolutionResult updated = result.or(Set.of("field2", "field3"));

    assertEquals(Set.of("field1", "field2", "field3"), updated.getRegularFields());
    assertEquals("user*", updated.getWildcard().toString());
  }

  @Test
  void testFieldResolutionResultExcludeOperation() {
    FieldResolutionResult result =
        new FieldResolutionResult(Set.of("field1", "field2", "field3"), "user*");
    FieldResolutionResult updated = result.exclude(Set.of("field2"));

    assertEquals(Set.of("field1", "field3"), updated.getRegularFields());
    assertEquals("user*", updated.getWildcard().toString());
  }

  @Test
  void testWildcardAndWithAnyWildcard() {
    Wildcard single = new SingleWildcard("user*");

    Wildcard result = FieldResolutionResult.ANY_WILDCARD.and(single);
    assertEquals(single, result);
    assertTrue(result.matches("username"));
    assertFalse(result.matches("admin"));
  }

  @Test
  void testWildcardAndWithNullWildcard() {
    Wildcard single = new SingleWildcard("user*");

    Wildcard result = FieldResolutionResult.NULL_WILDCARD.and(single);
    assertEquals(FieldResolutionResult.NULL_WILDCARD, result);
    assertFalse(result.matches("username"));
  }

  @Test
  void testWildcardOrWithAnyWildcard() {
    Wildcard single = new SingleWildcard("user*");

    Wildcard result = FieldResolutionResult.ANY_WILDCARD.or(single);
    assertEquals(FieldResolutionResult.ANY_WILDCARD, result);
    assertTrue(result.matches("anything"));
  }

  @Test
  void testWildcardOrWithNullWildcard() {
    Wildcard single = new SingleWildcard("user*");

    Wildcard result = FieldResolutionResult.NULL_WILDCARD.or(single);
    assertEquals(single, result);
    assertTrue(result.matches("username"));
    assertFalse(result.matches("admin"));
  }

  @Test
  void testOrWildcardOrWithSingleWildcard() {
    OrWildcard or = new OrWildcard(new SingleWildcard("user*"), new SingleWildcard("admin*"));
    SingleWildcard single = new SingleWildcard("guest*");

    Wildcard result = or.or(single);
    assertTrue(result instanceof OrWildcard);
    assertTrue(result.matches("username"));
    assertTrue(result.matches("admin_role"));
    assertTrue(result.matches("guest_id"));
    assertFalse(result.matches("other"));
  }

  @Test
  void testAndWildcardAndWithSingleWildcard() {
    AndWildcard and = new AndWildcard(new SingleWildcard("*user*"), new SingleWildcard("*name"));
    SingleWildcard single = new SingleWildcard("*full*");

    Wildcard result = and.and(single);
    assertTrue(result instanceof AndWildcard);
    assertTrue(result.matches("user_full_name"));
    assertFalse(result.matches("username"));
    assertFalse(result.matches("user_id"));
  }

  @Test
  void testOrWildcardOrWithOrWildcard() {
    OrWildcard or1 = new OrWildcard(new SingleWildcard("user*"), new SingleWildcard("admin*"));
    OrWildcard or2 = new OrWildcard(new SingleWildcard("guest*"), new SingleWildcard("root*"));

    Wildcard result = or1.or(or2);
    assertTrue(result instanceof OrWildcard);
    assertTrue(result.matches("username"));
    assertTrue(result.matches("admin_role"));
    assertTrue(result.matches("guest_id"));
    assertTrue(result.matches("root_access"));
    assertFalse(result.matches("other"));
  }

  @Test
  void testOrWildcardOrWithAndWildcard() {
    OrWildcard or = new OrWildcard(new SingleWildcard("user*"), new SingleWildcard("admin*"));
    AndWildcard and = new AndWildcard(new SingleWildcard("*_id"), new SingleWildcard("guest*"));

    Wildcard result = or.or(and);
    assertTrue(result instanceof OrWildcard);
    assertTrue(result.matches("username"));
    assertTrue(result.matches("admin_role"));
    assertTrue(result.matches("guest_id"));
    assertFalse(result.matches("guest_name"));
  }

  @Test
  void testAndWildcardAndWithAndWildcard() {
    AndWildcard and1 = new AndWildcard(new SingleWildcard("*user*"), new SingleWildcard("*name"));
    AndWildcard and2 = new AndWildcard(new SingleWildcard("*full*"), new SingleWildcard("user*"));

    Wildcard result = and1.and(and2);
    assertTrue(result instanceof AndWildcard);
    assertTrue(result.matches("user_full_name"));
    assertFalse(result.matches("username"));
    assertFalse(result.matches("user_name"));
    assertFalse(result.matches("full_name"));
  }

  @Test
  void testAndWildcardAndWithOrWildcard() {
    AndWildcard and = new AndWildcard(new SingleWildcard("*user*"), new SingleWildcard("*name"));
    OrWildcard or = new OrWildcard(new SingleWildcard("*full*"), new SingleWildcard("*first*"));

    Wildcard result = and.and(or);
    assertTrue(result instanceof AndWildcard);
    assertTrue(result.matches("user_full_name"));
    assertTrue(result.matches("user_first_name"));
    assertFalse(result.matches("username"));
    assertFalse(result.matches("user_last_name"));
  }

  @Test
  void testOrWildcardOrWithNullWildcard() {
    OrWildcard or = new OrWildcard(new SingleWildcard("user*"), new SingleWildcard("admin*"));

    Wildcard result = or.or(FieldResolutionResult.NULL_WILDCARD);
    assertEquals(or, result);
    assertTrue(result.matches("username"));
    assertTrue(result.matches("admin_role"));
  }

  @Test
  void testOrWildcardOrWithAnyWildcard() {
    OrWildcard or = new OrWildcard(new SingleWildcard("user*"), new SingleWildcard("admin*"));

    Wildcard result = or.or(FieldResolutionResult.ANY_WILDCARD);
    assertEquals(FieldResolutionResult.ANY_WILDCARD, result);
    assertTrue(result.matches("anything"));
  }

  @Test
  void testAndWildcardAndWithNullWildcard() {
    AndWildcard and = new AndWildcard(new SingleWildcard("*user*"), new SingleWildcard("*name"));

    Wildcard result = and.and(FieldResolutionResult.NULL_WILDCARD);
    assertEquals(FieldResolutionResult.NULL_WILDCARD, result);
    assertFalse(result.matches("username"));
  }

  @Test
  void testAndWildcardAndWithAnyWildcard() {
    AndWildcard and = new AndWildcard(new SingleWildcard("*user*"), new SingleWildcard("*name"));

    Wildcard result = and.and(FieldResolutionResult.ANY_WILDCARD);
    assertEquals(and, result);
    assertTrue(result.matches("username"));
  }

  @Test
  void testHasPartialWildcardsWithNoWildcard() {
    FieldResolutionResult result = new FieldResolutionResult(Set.of("field1", "field2"));
    assertFalse(result.hasPartialWildcards());
  }

  @Test
  void testHasPartialWildcardsWithAnyWildcard() {
    FieldResolutionResult result = new FieldResolutionResult(Set.of("field1"), "*");
    assertFalse(result.hasPartialWildcards());
  }

  @Test
  void testHasPartialWildcardsWithSingleWildcard() {
    FieldResolutionResult result = new FieldResolutionResult(Set.of("field1"), "user*");
    assertTrue(result.hasPartialWildcards());
  }
}
