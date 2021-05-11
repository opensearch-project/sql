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

package org.opensearch.sql.legacy.unittest.expression.core;

import static org.junit.Assert.assertEquals;
import static org.opensearch.sql.legacy.expression.core.ExpressionFactory.literal;
import static org.opensearch.sql.legacy.expression.model.ExprValueFactory.doubleValue;
import static org.opensearch.sql.legacy.expression.model.ExprValueFactory.integerValue;

import org.junit.Test;
import org.opensearch.sql.legacy.expression.core.operator.ScalarOperation;

public class CompoundExpressionTest extends ExpressionTest {

    @Test
    public void absAndAddShouldPass() {
        assertEquals(2.0d, apply(ScalarOperation.ABS, of(ScalarOperation.ADD,
                                                         literal(doubleValue(-1.0d)),
                                                         literal(integerValue(-1)))));
    }
}
