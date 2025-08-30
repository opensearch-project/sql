/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ppl.utils;

import static org.junit.Assert.assertEquals;
import static org.opensearch.sql.ast.dsl.AstDSL.eval;
import static org.opensearch.sql.ast.dsl.AstDSL.field;
import static org.opensearch.sql.ast.dsl.AstDSL.function;
import static org.opensearch.sql.ast.dsl.AstDSL.let;
import static org.opensearch.sql.ast.dsl.AstDSL.relation;
import static org.opensearch.sql.ast.dsl.AstDSL.spath;
import static org.opensearch.sql.ast.dsl.AstDSL.stringLiteral;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.Test;
import org.mockito.Mockito;
import org.opensearch.sql.ast.Node;
import org.opensearch.sql.ast.tree.Eval;
import org.opensearch.sql.ast.tree.SPath;
import org.opensearch.sql.calcite.CalcitePlanContext;
import org.opensearch.sql.common.setting.Settings;
import org.opensearch.sql.ppl.antlr.PPLSyntaxParser;
import org.opensearch.sql.ppl.calcite.CalcitePPLAbstractTest;
import org.opensearch.sql.ppl.parser.AstBuilder;

public class SPathRewriteTest extends CalcitePPLAbstractTest {
  private final Settings settings = Mockito.mock(Settings.class);
  private final PPLSyntaxParser parser = new PPLSyntaxParser();

  private Node plan(String query) {
    AstBuilder astBuilder = new AstBuilder(query, settings);
    return astBuilder.visit(parser.parse(query));
  }

  private CalcitePlanContext createTestFieldContext(String[] fieldNames) {
    CalcitePlanContext ctx = this.createBuilderContext();
    RelDataTypeFactory typeFactory = ctx.relBuilder.getTypeFactory();

    List<Map.Entry<String, RelDataType>> fields = new ArrayList<>();
    for (String fieldName : fieldNames) {
      fields.add(Map.entry(fieldName, typeFactory.createSqlType(SqlTypeName.ANY)));
    }
    RelDataType rowType = typeFactory.createStructType(fields);

    ctx.relBuilder.values(rowType);
    return ctx;
  }

  // Control test to make sure something fundamental hasn't changed about the json_extract parsing
  @Test
  public void testEvalControl() {
    assertEquals(
        eval(
            relation("t"),
            let(field("o"), function("json_extract", field("f"), stringLiteral("simple.nested")))),
        plan("source = t | eval o=json_extract(f, \"simple.nested\")"));
  }

  @Test(expected = IllegalArgumentException.class)
  public void testSpathMissingInputArgumentHandling() {
    plan("source = t | spath path=a output=a");
  }

  @Test(expected = IllegalArgumentException.class)
  public void testSpathMissingPathArgumentHandling() {
    plan("source = t | spath input=a output=a");
  }

  @Test
  public void testSpathArgumentDeshuffle() {
    assertEquals(plan("source = t | spath path=a input=a"), plan("source = t | spath input=a a"));
  }

  @Test
  public void testSpathSimpleRewriteWithExistingFields() {
    // Test when we have the exact field available
    CalcitePlanContext ctx = createTestFieldContext(new String[] {"f"});
    SPath sp = spath(relation("t"), "f", "o", "simple.nested");
    Eval ev = (Eval) plan("source = t | eval o=json_extract(f, \"simple.nested\")");
    assertEquals(ev, sp.rewriteAsEval(ctx));
  }

  @Test
  public void testSpathPartialMatchRewrite() {
    // Test when we have a partial field match
    CalcitePlanContext ctx = createTestFieldContext(new String[] {"outer.inner"});
    SPath sp = (SPath) plan("source = t | spath input=outer output=result inner.data");
    Eval ev = (Eval) plan("source = t | eval result=json_extract(outer.inner, \"data\")");
    assertEquals(ev, sp.rewriteAsEval(ctx));
  }

  @Test
  public void testSpathExactFieldMatch() {
    // Test when the requested path exactly matches an existing field
    CalcitePlanContext ctx = createTestFieldContext(new String[] {"data.nested.field"});
    SPath sp = (SPath) plan("source = t | spath input=data output=output nested.field");
    Eval ev = (Eval) plan("source = t | eval output=data.nested.field");
    Eval act = sp.rewriteAsEval(ctx);
    assertEquals(ev, act);
  }

  @Test
  public void testSpathMultipleFieldOptions() {
    // Test when multiple field options are available, should choose longest match
    CalcitePlanContext ctx =
        createTestFieldContext(
            new String[] {"data", "data.nested", "data.nested.field", "data.other"});
    SPath sp = (SPath) plan("source = t | spath input=data output=output nested.field.value");
    Eval ev = (Eval) plan("source = t | eval output=json_extract(data.nested.field, \"value\")");
    assertEquals(ev, sp.rewriteAsEval(ctx));
  }

  @Test
  public void testSpathNoMatchingFields() {
    // Test when no matching fields are available
    CalcitePlanContext ctx = createTestFieldContext(new String[] {"other.field"});
    SPath sp = (SPath) plan("source = t | spath input=data output=output nested.field");
    Eval ev = (Eval) plan("source = t | eval output=json_extract(data, \"nested.field\")");
    assertEquals(ev, sp.rewriteAsEval(ctx));
  }

  @Test
  public void testSpathBacktickHandling() {
    // Test handling of backtick-quoted paths
    CalcitePlanContext ctx = createTestFieldContext(new String[] {"data"});
    SPath sp = (SPath) plan("source = t | spath input=data output=output `nested.field`");
    Eval ev = (Eval) plan("source = t | eval output=json_extract(data, \"nested.field\")");
    assertEquals(ev, sp.rewriteAsEval(ctx));
  }

  @Test
  public void testSpathSegmentBoundaryMatch() {
    // Test that we don't match partial segments
    CalcitePlanContext ctx = createTestFieldContext(new String[] {"data.inner_extended"});
    SPath sp = (SPath) plan("source = t | spath input=data output=output inner.field");
    Eval ev = (Eval) plan("source = t | eval output=json_extract(data, \"inner.field\")");
    assertEquals(ev, sp.rewriteAsEval(ctx));
  }
}
