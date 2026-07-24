/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.sql.parser.bucket;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.Set;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator;
import org.junit.jupiter.api.Test;
import org.opensearch.sql.ast.dsl.AstDSL;
import org.opensearch.sql.ast.expression.Function;
import org.opensearch.sql.ast.expression.Literal;
import org.opensearch.sql.ast.expression.UnresolvedExpression;
import org.opensearch.sql.exception.SemanticCheckException;

@DisplayNameGeneration(DisplayNameGenerator.ReplaceUnderscores.class)
class NamedArgumentsTest {

  @Test
  void empty_arg_list_is_not_named_arguments() {
    assertFalse(NamedArguments.isNamedArguments(List.of()));
  }

  @Test
  void single_kv_pair_is_named_arguments() {
    assertTrue(NamedArguments.isNamedArguments(List.of(kv("k", AstDSL.intLiteral(1)))));
  }

  @Test
  void plain_function_call_is_not_named_arguments() {
    UnresolvedExpression nonKv = AstDSL.qualifiedName("col");
    assertFalse(NamedArguments.isNamedArguments(List.of(nonKv)));
  }

  @Test
  void mixed_args_are_not_named_arguments() {
    assertFalse(
        NamedArguments.isNamedArguments(
            List.of(kv("k", AstDSL.intLiteral(1)), AstDSL.qualifiedName("col"))));
  }

  @Test
  void non_equals_function_is_not_named_arguments() {
    UnresolvedExpression notEq =
        new Function("+", List.of(AstDSL.stringLiteral("a"), AstDSL.intLiteral(1)));
    assertFalse(NamedArguments.isNamedArguments(List.of(notEq)));
  }

  @Test
  void equals_with_non_string_left_is_not_named_arguments() {
    UnresolvedExpression intEqInt =
        new Function("=", List.of(AstDSL.intLiteral(1), AstDSL.intLiteral(2)));
    assertFalse(NamedArguments.isNamedArguments(List.of(intEqInt)));
  }

  @Test
  void parse_keeps_keys_in_source_order_and_lower_cases_them() {
    NamedArguments bag =
        NamedArguments.parse(
            List.of(
                kv("Field", AstDSL.stringLiteral("ts")),
                kv("INTERVAL", AstDSL.stringLiteral("1d"))));

    assertEquals(AstDSL.stringLiteral("ts"), bag.remove("field"));
    assertEquals(AstDSL.stringLiteral("1d"), bag.remove("interval"));
    assertEquals(0, bag.size());
  }

  @Test
  void parse_rejects_duplicate_keys() {
    SemanticCheckException ex =
        assertThrows(
            SemanticCheckException.class,
            () ->
                NamedArguments.parse(
                    List.of(
                        kv("field", AstDSL.stringLiteral("a")),
                        kv("field", AstDSL.stringLiteral("b")))));
    assertTrue(ex.getMessage().contains("field"));
  }

  @Test
  void parse_rejects_non_key_value_arg_with_clear_message() {
    UnresolvedExpression bareColumn = AstDSL.qualifiedName("age");
    SemanticCheckException ex =
        assertThrows(
            SemanticCheckException.class,
            () ->
                NamedArguments.parse(List.of(kv("field", AstDSL.stringLiteral("a")), bareColumn)));
    assertTrue(ex.getMessage().contains("'key'=value"));
  }

  @Test
  void remove_returns_value_when_present_and_null_when_absent() {
    NamedArguments bag = NamedArguments.parse(List.of(kv("field", AstDSL.stringLiteral("ts"))));
    assertEquals(AstDSL.stringLiteral("ts"), bag.remove("field"));
    assertNull(bag.remove("field"));
    assertNull(bag.remove("never_inserted"));
    assertEquals(0, bag.size());
  }

  @Test
  void require_returns_value_and_removes_it() {
    NamedArguments bag = NamedArguments.parse(List.of(kv("field", AstDSL.stringLiteral("ts"))));
    assertEquals(AstDSL.stringLiteral("ts"), bag.require("field", "histogram"));
    assertEquals(0, bag.size());
  }

  @Test
  void require_throws_when_missing_with_function_name_in_message() {
    NamedArguments bag = NamedArguments.parse(List.of(kv("other", AstDSL.intLiteral(1))));
    SemanticCheckException ex =
        assertThrows(SemanticCheckException.class, () -> bag.require("field", "HISTOGRAM"));
    assertTrue(ex.getMessage().contains("histogram"));
    assertTrue(ex.getMessage().contains("field"));
  }

  @Test
  void requireString_returns_string_literal() {
    NamedArguments bag = NamedArguments.parse(List.of(kv("interval", AstDSL.stringLiteral("1d"))));
    Literal interval = bag.requireString("interval", "date_histogram");
    assertEquals(AstDSL.stringLiteral("1d"), interval);
  }

  @Test
  void requireString_rejects_non_string_value() {
    NamedArguments bag = NamedArguments.parse(List.of(kv("interval", AstDSL.intLiteral(100))));
    assertThrows(
        SemanticCheckException.class, () -> bag.requireString("interval", "date_histogram"));
  }

  @Test
  void requireStringIfPresent_returns_null_when_absent() {
    NamedArguments bag = NamedArguments.parse(List.of(kv("field", AstDSL.stringLiteral("ts"))));
    assertNull(bag.requireStringIfPresent("format"));
  }

  @Test
  void requireStringIfPresent_returns_value_when_present() {
    NamedArguments bag = NamedArguments.parse(List.of(kv("format", AstDSL.stringLiteral("yyyy"))));
    assertEquals(AstDSL.stringLiteral("yyyy"), bag.requireStringIfPresent("format"));
  }

  @Test
  void requireStringIfPresent_rejects_non_string_value_when_present() {
    NamedArguments bag = NamedArguments.parse(List.of(kv("format", AstDSL.intLiteral(2024))));
    assertThrows(SemanticCheckException.class, () -> bag.requireStringIfPresent("format"));
  }

  @Test
  void rejectIfPresent_throws_when_key_present() {
    NamedArguments bag = NamedArguments.parse(List.of(kv("script", AstDSL.stringLiteral("x"))));
    SemanticCheckException ex =
        assertThrows(SemanticCheckException.class, () -> bag.rejectIfPresent("script", "no!"));
    assertTrue(ex.getMessage().contains("no!"));
  }

  @Test
  void rejectIfPresent_no_op_when_key_absent() {
    NamedArguments bag = NamedArguments.parse(List.of(kv("field", AstDSL.stringLiteral("ts"))));
    bag.rejectIfPresent("script", "no!");
    assertEquals(1, bag.size());
  }

  @Test
  void consumeSilently_drops_listed_keys() {
    NamedArguments bag =
        NamedArguments.parse(
            List.of(
                kv("alias", AstDSL.stringLiteral("x")),
                kv("nested", AstDSL.stringLiteral("p")),
                kv("interval", AstDSL.intLiteral(10))));
    bag.consumeSilently(Set.of("alias", "nested"));
    assertEquals(1, bag.size());
  }

  @Test
  void rejectRemaining_single_key_uses_parameter_label() {
    NamedArguments bag = NamedArguments.parse(List.of(kv("mystery", AstDSL.intLiteral(5))));
    SemanticCheckException ex =
        assertThrows(SemanticCheckException.class, () -> bag.rejectRemaining("HISTOGRAM"));
    assertTrue(ex.getMessage().contains("histogram"));
    assertTrue(ex.getMessage().contains("does not accept parameter:"));
    assertTrue(ex.getMessage().contains("mystery"));
  }

  @Test
  void rejectRemaining_multiple_keys_listed_in_source_order_with_plural_label() {
    NamedArguments bag =
        NamedArguments.parse(
            List.of(
                kv("foo", AstDSL.intLiteral(1)),
                kv("bar", AstDSL.intLiteral(2)),
                kv("baz", AstDSL.intLiteral(3))));
    SemanticCheckException ex =
        assertThrows(SemanticCheckException.class, () -> bag.rejectRemaining("HISTOGRAM"));
    assertTrue(ex.getMessage().contains("does not accept parameters:"));
    assertTrue(ex.getMessage().contains("foo, bar, baz"));
  }

  @Test
  void rejectRemaining_no_op_when_bag_empty() {
    NamedArguments bag = NamedArguments.parse(List.of(kv("alias", AstDSL.stringLiteral("x"))));
    bag.consumeSilently(Set.of("alias"));
    bag.rejectRemaining("histogram"); // does not throw
  }

  /** Builds a Function("=", [stringLiteral(key), value]) — same shape ANTLR produces for 'k'=v. */
  private static UnresolvedExpression kv(String key, UnresolvedExpression value) {
    return new Function("=", List.of(AstDSL.stringLiteral(key), value));
  }
}
