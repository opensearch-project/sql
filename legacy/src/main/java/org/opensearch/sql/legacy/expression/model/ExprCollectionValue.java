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

package org.opensearch.sql.legacy.expression.model;

import static org.opensearch.sql.legacy.expression.model.ExprValue.ExprValueKind.COLLECTION_VALUE;

import java.util.List;
import java.util.stream.Collectors;
import lombok.EqualsAndHashCode;
import lombok.RequiredArgsConstructor;

@EqualsAndHashCode
@RequiredArgsConstructor
public class ExprCollectionValue implements ExprValue {
    private final List<ExprValue> valueList;

    @Override
    public Object value() {
        return valueList;
    }

    @Override
    public ExprValueKind kind() {
        return COLLECTION_VALUE;
    }

    @Override
    public String toString() {
        return valueList.stream()
                .map(Object::toString)
                .collect(Collectors.joining(",", "[", "]"));
    }
}
