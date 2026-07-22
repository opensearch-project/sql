/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.utils;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.Test;
import org.opensearch.sql.ast.expression.And;
import org.opensearch.sql.ast.expression.Compare;
import org.opensearch.sql.ast.expression.Field;
import org.opensearch.sql.ast.expression.QualifiedName;
import org.opensearch.sql.ast.expression.UnresolvedExpression;

public class JoinAndLookupUtilsTest {

  @Test
  public void collectBareFields_singleField() {
    UnresolvedExpression expr = bareField("a");
    Optional<List<String>> result = JoinAndLookupUtils.collectBareFields(expr);
    assertTrue(result.isPresent());
    assertEquals(List.of("a"), result.get());
  }

  @Test
  public void collectBareFields_multipleFieldsAndChain() {
    UnresolvedExpression expr = new And(bareField("a"), new And(bareField("b"), bareField("c")));
    Optional<List<String>> result = JoinAndLookupUtils.collectBareFields(expr);
    assertTrue(result.isPresent());
    assertEquals(List.of("a", "b", "c"), result.get());
  }

  @Test
  public void collectBareFields_mixedConditionReturnsEmpty() {
    // `a AND l.x = r.x` — the Compare node makes this not all-bare-fields
    UnresolvedExpression compare =
        new Compare("=", qualifiedField("l", "x"), qualifiedField("r", "x"));
    UnresolvedExpression expr = new And(bareField("a"), compare);
    Optional<List<String>> result = JoinAndLookupUtils.collectBareFields(expr);
    assertTrue(result.isEmpty());
  }

  @Test
  public void collectBareFields_qualifiedFieldReturnsEmpty() {
    // A two-part name like `t.x` is not a bare field
    UnresolvedExpression expr = qualifiedField("t", "x");
    Optional<List<String>> result = JoinAndLookupUtils.collectBareFields(expr);
    assertTrue(result.isEmpty());
  }

  @Test
  public void collectBareFields_mixedLeftSideReturnsEmptyNoPartialState() {
    // Compare on left, bare field on right — must return empty (not ["b"])
    UnresolvedExpression compare = new Compare("=", bareField("x"), bareField("y"));
    UnresolvedExpression expr = new And(compare, bareField("b"));
    Optional<List<String>> result = JoinAndLookupUtils.collectBareFields(expr);
    assertTrue(result.isEmpty());
  }

  private static Field bareField(String name) {
    return new Field(QualifiedName.of(name));
  }

  private static Field qualifiedField(String qualifier, String name) {
    return new Field(QualifiedName.of(qualifier, name));
  }
}
