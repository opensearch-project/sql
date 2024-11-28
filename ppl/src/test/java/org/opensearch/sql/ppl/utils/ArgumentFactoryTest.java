/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ppl.utils;

import static java.util.Collections.emptyList;
import static org.opensearch.sql.ast.dsl.AstDSL.agg;
import static org.opensearch.sql.ast.dsl.AstDSL.aggregate;
import static org.opensearch.sql.ast.dsl.AstDSL.alias;
import static org.opensearch.sql.ast.dsl.AstDSL.argument;
import static org.opensearch.sql.ast.dsl.AstDSL.booleanLiteral;
import static org.opensearch.sql.ast.dsl.AstDSL.dedupe;
import static org.opensearch.sql.ast.dsl.AstDSL.exprList;
import static org.opensearch.sql.ast.dsl.AstDSL.field;
import static org.opensearch.sql.ast.dsl.AstDSL.intLiteral;
import static org.opensearch.sql.ast.dsl.AstDSL.projectWithArg;
import static org.opensearch.sql.ast.dsl.AstDSL.relation;
import static org.opensearch.sql.ast.dsl.AstDSL.sort;
import static org.opensearch.sql.ast.dsl.AstDSL.stringLiteral;

import org.junit.Test;
import org.opensearch.sql.ppl.parser.AstBuilderTest;

public class ArgumentFactoryTest extends AstBuilderTest {

  @Test
  public void testFieldsCommandArgument() {
    assertEqual(
        "source=t | fields - a",
        projectWithArg(
            relation("t"), exprList(argument("exclude", booleanLiteral(true))), field("a")));
  }

  @Test
  public void testFieldsCommandDefaultArgument() {
    assertEqual("source=t | fields + a", "source=t | fields a");
  }

  @Test
  public void testStatsCommandArgument() {
    assertEqual(
        "source=t | stats partitions=1 allnum=false delim=',' avg(a) dedup_splitvalues=true",
        agg(
            relation("t"),
            exprList(alias("avg(a)", aggregate("avg", field("a")))),
            emptyList(),
            emptyList(),
            exprList(
                argument("partitions", intLiteral(1)),
                argument("allnum", booleanLiteral(false)),
                argument("delim", stringLiteral(",")),
                argument("dedupsplit", booleanLiteral(true)))));
  }

  @Test
  public void testStatsCommandDefaultArgument() {
    assertEqual(
        "source=t | stats partitions=1 allnum=false delim=' ' avg(a) dedup_splitvalues=false",
        "source=t | stats avg(a)");
  }

  @Test
  public void testDedupCommandArgument() {
    assertEqual(
        "source=t | dedup 3 field0 keepempty=false consecutive=true",
        dedupe(
            relation("t"),
            exprList(
                argument("number", intLiteral(3)),
                argument("keepempty", booleanLiteral(false)),
                argument("consecutive", booleanLiteral(true))),
            field("field0")));
  }

  @Test
  public void testDedupCommandDefaultArgument() {
    assertEqual(
        "source=t | dedup 1 field0 keepempty=false consecutive=false", "source=t | dedup field0");
  }

  @Test
  public void testSortCommandDefaultArgument() {
    assertEqual(
        "source=t | sort field0",
        sort(relation("t"), field("field0", exprList(argument("asc", booleanLiteral(true))))));

    assertEqual(
        "source=t | sort + field0",
        sort(relation("t"), field("field0", exprList(argument("asc", booleanLiteral(true))))));

    assertEqual(
        "source=t | sort - field0",
        sort(relation("t"), field("field0", exprList(argument("asc", booleanLiteral(false))))));
  }

  @Test
  public void testNoArgConstructorForArgumentFactoryShouldPass() {
    new ArgumentFactory();
  }
}
