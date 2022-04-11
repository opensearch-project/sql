/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.opensearch.storage.script.filter;

import lombok.RequiredArgsConstructor;
import org.opensearch.sql.expression.Expression;
import org.opensearch.sql.expression.ExpressionNodeVisitor;
import org.opensearch.sql.expression.FunctionExpression;
import org.opensearch.sql.expression.function.FunctionDSL;


@RequiredArgsConstructor
public class PromQLFilterQueryBuilder extends ExpressionNodeVisitor<StringBuilder, Object> {

    /**
     * Build OpenSearch filter query from expression.
     *
     * @param expr expression
     * @return query
     */
    public StringBuilder build(Expression expr) {
        return expr.accept(this, null);
    }

    @Override
    public StringBuilder visitFunction(FunctionExpression func, Object context) {

        if(func.getFunctionName().getFunctionName().equals("and")) {
                return new StringBuilder()
                        .append("{")
                        .append(visitFunction((FunctionExpression) func.getArguments().get(0), context))
                        .append(" , ")
                        .append(visitFunction((FunctionExpression) func.getArguments().get(1), context))
                        .append("}");
        }
        else if(func.getFunctionName().getFunctionName().startsWith("=")) {
            return new StringBuilder()
                    .append(func.getArguments().get(0))
                    .append(func.getFunctionName().getFunctionName())
                    .append(func.getArguments().get(1));
        }
        else {
            throw new RuntimeException("Function Not supported");
        }
    }
}
