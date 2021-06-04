/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

/*
 *   Copyright 2020 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 *   Licensed under the Apache License, Version 2.0 (the "License").
 *   You may not use this file except in compliance with the License.
 *   A copy of the License is located at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *   or in the "license" file accompanying this file. This file is distributed
 *   on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 *   express or implied. See the License for the specific language governing
 *   permissions and limitations under the License.
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
    assertEqual("source=t | fields - a",
        projectWithArg(
            relation("t"),
            exprList(argument("exclude", booleanLiteral(true))),
            field("a")
        ));
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
            exprList(
                alias(
                    "avg(a)",
                    aggregate("avg", field("a")))
                ),
            emptyList(),
            emptyList(),
            exprList(
                argument("partitions", intLiteral(1)),
                argument("allnum", booleanLiteral(false)),
                argument("delim", stringLiteral(",")),
                argument("dedupsplit", booleanLiteral(true))
            )
        ));
  }

  @Test
  public void testStatsCommandDefaultArgument() {
    assertEqual(
        "source=t | stats partitions=1 allnum=false delim=' ' avg(a) dedup_splitvalues=false",
        "source=t | stats avg(a)");
  }

  @Test
  public void testDedupCommandArgument() {
    assertEqual("source=t | dedup 3 field0 keepempty=false consecutive=true",
        dedupe(
            relation("t"),
            exprList(
                argument("number", intLiteral(3)),
                argument("keepempty", booleanLiteral(false)),
                argument("consecutive", booleanLiteral(true))
            ),
            field("field0")
        ));
  }

  @Test
  public void testDedupCommandDefaultArgument() {
    assertEqual(
        "source=t | dedup 1 field0 keepempty=false consecutive=false",
        "source=t | dedup field0"
    );
  }

  @Test
  public void testSortCommandDefaultArgument() {
    assertEqual(
        "source=t | sort field0",
        "source=t | sort field0"
    );
  }

  @Test
  public void testSortFieldArgument() {
    assertEqual("source=t | sort - auto(field0)",
        sort(
            relation("t"),
            field(
                "field0",
                exprList(
                    argument("asc", booleanLiteral(false)),
                    argument("type", stringLiteral("auto"))
                )
            )
        ));
  }

  @Test
  public void testNoArgConstructorForArgumentFactoryShouldPass() {
    new ArgumentFactory();
  }

}
