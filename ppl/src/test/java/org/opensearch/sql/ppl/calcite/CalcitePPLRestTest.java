/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ppl.calcite;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

import org.junit.Test;
import org.opensearch.sql.ast.Node;
import org.opensearch.sql.ast.tree.Project;
import org.opensearch.sql.ast.tree.RestRelation;
import org.opensearch.sql.common.setting.Settings;
import org.opensearch.sql.ppl.antlr.PPLSyntaxParser;
import org.opensearch.sql.ppl.parser.AstBuilder;
import org.opensearch.sql.utils.SystemIndexUtils;

/**
 * Calcite-path coverage for the {@code rest} leading command at the parse / AST tier.
 *
 * <p>The {@code rest} row source resolves through {@code OpenSearchStorageEngine.getTable} ->
 * {@code RestSourceTable} -> {@code CalciteLogicalRestScan}, which lives in the {@code opensearch}
 * module. This ppl-module Calcite harness binds a Calcite SCOTT schema rather than the OpenSearch
 * storage engine, so the optimized {@code CalciteEnumerableRestScan} logical-plan assertion is
 * exercised in {@code RestSourceTableTest} (logical scan + fixed row type, unit) and {@code
 * CalcitePPLRestIT} (schema + datarows on a live single-node cluster). This test pins the
 * Calcite-facing contract that the ppl module owns: the grammar/AST rewrite of {@code rest} into a
 * {@code RestRelation} carrying the validated, reserved-name-encoded endpoint spec that rides
 * {@code visitRelation} exactly like {@code DESCRIBE}.
 */
public class CalcitePPLRestTest {

  private final PPLSyntaxParser parser = new PPLSyntaxParser();
  private final Settings settings = mock(Settings.class);

  private Node parse(String ppl) {
    return new AstBuilder(ppl, settings).visit(parser.parse(ppl));
  }

  @Test
  public void restHealthProjectsDeclaredColumns() {
    Project project =
        (Project) parse("| rest \"/_cluster/health\" | fields status, number_of_nodes");
    RestRelation rest = (RestRelation) project.getChild().get(0);
    SystemIndexUtils.RestSpec spec =
        SystemIndexUtils.decodeRestSpec(rest.getTableQualifiedName().toString());
    assertEquals("/_cluster/health", spec.getEndpoint());
    // downstream fields compose on top of the rest row source.
    assertEquals(2, project.getProjectList().size());
  }

  @Test
  public void restReservedNameRoundTrips() {
    RestRelation rest =
        (RestRelation) parse("| rest \"/_cat/indices\" count=10 timeout=\"5s\" health=\"green\"");
    String reserved = rest.getTableQualifiedName().toString();
    assertTrue(SystemIndexUtils.isRestSource(reserved));
    SystemIndexUtils.RestSpec spec = SystemIndexUtils.decodeRestSpec(reserved);
    assertEquals("/_cat/indices", spec.getEndpoint());
    assertEquals(Integer.valueOf(10), spec.getCount());
    assertEquals("5s", spec.getTimeout());
    assertEquals("green", spec.getArgs().get("health"));
  }
}
