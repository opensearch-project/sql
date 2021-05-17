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

package org.opensearch.sql.legacy.expression.core.operator;

import java.util.List;
import org.opensearch.sql.legacy.expression.model.ExprValue;

/**
 * Scalar Operator is a function has one or more arguments and return a single value.
 */
public interface ScalarOperator {
    /**
     * Apply the operator to the input arguments.
     * @param valueList argument list.
     * @return result.
     */
    ExprValue apply(List<ExprValue> valueList);

    /**
     * The name of the operator.
     * @return name.
     */
    String name();
}
