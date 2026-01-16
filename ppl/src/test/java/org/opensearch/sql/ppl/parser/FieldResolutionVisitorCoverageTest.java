/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ppl.parser;

import static org.junit.Assert.assertTrue;

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;
import org.junit.Test;
import org.opensearch.sql.ast.AbstractNodeVisitor;
import org.opensearch.sql.ast.analysis.FieldResolutionVisitor;
import org.opensearch.sql.calcite.CalciteRelNodeVisitor;

/**
 * Test to verify that FieldResolutionVisitor overrides all methods that CalciteRelNodeVisitor
 * overrides from AbstractNodeVisitor.
 *
 * <p>This ensures that FieldResolutionVisitor provides field resolution logic for all AST node
 * types that CalciteRelNodeVisitor handles.
 */
public class FieldResolutionVisitorCoverageTest {

  @Test
  public void testFieldResolutionVisitorOverridesAllCalciteRelNodeVisitorMethods() {
    Set<String> calciteOverriddenMethods = getOverriddenMethods(CalciteRelNodeVisitor.class);
    Set<String> fieldResolutionOverriddenMethods =
        getOverriddenMethods(FieldResolutionVisitor.class);

    // Find methods that CalciteRelNodeVisitor overrides but FieldResolutionVisitor doesn't
    Set<String> missingMethods = new HashSet<>(calciteOverriddenMethods);
    missingMethods.removeAll(fieldResolutionOverriddenMethods);

    // Only allow unsupported Calcite commands to be missing
    Set<String> unsupportedCalciteCommands =
        Set.of(
            "visitAD",
            "visitCloseCursor",
            "visitFetchCursor",
            "visitML",
            "visitPaginate",
            "visitKmeans",
            "visitTableFunction");

    missingMethods.removeAll(unsupportedCalciteCommands);

    assertTrue(
        "FieldResolutionVisitor must override all supported methods that CalciteRelNodeVisitor "
            + "overrides. Missing methods: "
            + missingMethods,
        missingMethods.isEmpty());
  }

  private Set<String> getOverriddenMethods(Class<?> clazz) {
    Set<String> abstractMethods = getAbstractNodeVisitorMethods();
    return Arrays.stream(clazz.getDeclaredMethods())
        .filter(method -> abstractMethods.contains(method.getName()))
        .map(Method::getName)
        .collect(Collectors.toSet());
  }

  private Set<String> getAbstractNodeVisitorMethods() {
    return Arrays.stream(AbstractNodeVisitor.class.getDeclaredMethods())
        .filter(method -> method.getName().startsWith("visit"))
        .map(Method::getName)
        .collect(Collectors.toSet());
  }
}
