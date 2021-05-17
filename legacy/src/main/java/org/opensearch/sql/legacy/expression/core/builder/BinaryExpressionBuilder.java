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
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 *   or in the "license" file accompanying this file. This file is distributed
 *   on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 *   express or implied. See the License for the specific language governing
 *   permissions and limitations under the License.
 */

package org.opensearch.sql.legacy.expression.core.builder;

import java.util.Arrays;
import java.util.List;
import lombok.RequiredArgsConstructor;
import org.opensearch.sql.legacy.expression.core.Expression;
import org.opensearch.sql.legacy.expression.core.operator.ScalarOperator;
import org.opensearch.sql.legacy.expression.domain.BindingTuple;
import org.opensearch.sql.legacy.expression.model.ExprValue;

/**
 * The definition of the Expression Builder which has two arguments.
 */
@RequiredArgsConstructor
public class BinaryExpressionBuilder implements ExpressionBuilder {
    private final ScalarOperator op;

    /**
     * Build the expression with two {@link Expression} as arguments.
     * @param expressionList expression list.
     * @return expression.
     */
    @Override
    public Expression build(List<Expression> expressionList) {
        Expression e1 = expressionList.get(0);
        Expression e2 = expressionList.get(1);

        return new Expression() {
            @Override
            public ExprValue valueOf(BindingTuple tuple) {
                return op.apply(Arrays.asList(e1.valueOf(tuple), e2.valueOf(tuple)));
            }

            @Override
            public String toString() {
                return String.format("%s(%s,%s)", op.name(), e1, e2);
            }
        };
    }
}
