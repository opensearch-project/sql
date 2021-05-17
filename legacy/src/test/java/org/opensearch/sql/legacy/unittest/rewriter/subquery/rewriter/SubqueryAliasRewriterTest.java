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
 *   Copyright 2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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

package org.opensearch.sql.legacy.unittest.rewriter.subquery.rewriter;

import static org.junit.Assert.assertEquals;
import static org.opensearch.sql.legacy.util.SqlParserUtils.parse;

import com.alibaba.druid.sql.SQLUtils;
import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.expr.SQLQueryExpr;
import org.junit.Test;
import org.opensearch.sql.legacy.rewriter.subquery.rewriter.SubqueryAliasRewriter;

public class SubqueryAliasRewriterTest {

    @Test
    public void testWithoutAlias() {
        assertEquals(
                sqlString(parse(
                        "SELECT TbA_0.* " +
                                "FROM TbA as TbA_0 " +
                                "WHERE TbA_0.a IN (SELECT TbB_1.b FROM TbB as TbB_1) and TbA_0.c > 10")),
                sqlString(rewrite(parse(
                        "SELECT * " +
                                "FROM TbA " +
                                "WHERE a IN (SELECT b FROM TbB) and c > 10"))));
    }

    @Test
    public void testWithAlias() {
        assertEquals(
                sqlString(parse(
                        "SELECT A.* " +
                                "FROM TbA as A " +
                                "WHERE A.a IN (SELECT B.b FROM TbB as B) " +
                                "AND A.c > 10")),
                sqlString(rewrite(parse(
                        "SELECT A.* " +
                                "FROM TbA as A " +
                                "WHERE A.a IN (SELECT B.b FROM TbB as B) " +
                                "AND A.c > 10"))));
    }

    @Test
    public void testOuterWithoutAliasInnerWithAlias() {
        assertEquals(
                sqlString(parse(
                        "SELECT TbA_0.* " +
                                "FROM TbA as TbA_0 " +
                                "WHERE TbA_0.a IN (SELECT TbB.b FROM TbB as TbB) " +
                                "AND TbA_0.c > 10")),
                sqlString(rewrite(parse(
                        "SELECT * " +
                                "FROM TbA " +
                                "WHERE a IN (SELECT TbB.b FROM TbB as TbB) " +
                                "AND c > 10"))));
    }

    @Test
    public void testOuterWithoutAliasInnerMixAlias() {
        String expect =
                "SELECT TbA_0.* " +
                        "FROM TbA as TbA_0 " +
                        "WHERE TbA_0.a IN (SELECT B.b FROM TbB as B) " +
                        "AND TbA_0.c > 10";

        assertEquals(
                sqlString(parse(expect)),
                sqlString(rewrite(parse(
                        "SELECT * " +
                                "FROM TbA " +
                                "WHERE a IN (SELECT b FROM TbB as B) " +
                                "AND c > 10"))));

        assertEquals(
                sqlString(parse(expect)),
                sqlString(rewrite(parse(
                        "SELECT * " +
                                "FROM TbA " +
                                "WHERE a IN (SELECT TbB.b FROM TbB as B) " +
                                "AND c > 10"))));
    }

    @Test
    public void testOuterWithAliasInnerWithoutAlias() {
        assertEquals(
                sqlString(parse(
                        "SELECT TbA.* " +
                                "FROM TbA as TbA " +
                                "WHERE TbA.a IN (SELECT TbB_0.b FROM TbB as TbB_0) " +
                                "AND TbA.c > 10")),
                sqlString(rewrite(parse(
                        "SELECT TbA.* " +
                                "FROM TbA as TbA " +
                                "WHERE TbA.a IN (SELECT b FROM TbB ) " +
                                "AND TbA.c > 10"))));
    }

    @Test
    public void testOuterMixAliasInnerWithoutAlias() {
        String expect =
                "SELECT A.* " +
                        "FROM TbA as A " +
                        "WHERE A.a IN (SELECT TbB_0.b FROM TbB as TbB_0) " +
                        "AND A.c > 10";

        assertEquals(
                sqlString(parse(expect)),
                sqlString(rewrite(parse(
                        "SELECT TbA.* " +
                                "FROM TbA as A " +
                                "WHERE a IN (SELECT b FROM TbB ) " +
                                "AND TbA.c > 10"))));

        assertEquals(
                sqlString(parse(expect)),
                sqlString(rewrite(parse(
                        "SELECT * " +
                                "FROM TbA as A " +
                                "WHERE TbA.a IN (SELECT b FROM TbB ) " +
                                "AND TbA.c > 10"))));
    }


    private String sqlString(SQLExpr expr) {
        return SQLUtils.toMySqlString(expr);
    }

    private SQLQueryExpr rewrite(SQLQueryExpr expr) {
        expr.accept(new SubqueryAliasRewriter());
        return expr;
    }
}
