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

package org.opensearch.sql.legacy.antlr.semantic.types.operator;

import static org.opensearch.sql.legacy.antlr.semantic.types.base.OpenSearchDataType.TYPE_ERROR;

import java.util.List;
import java.util.Optional;
import org.opensearch.sql.legacy.antlr.semantic.types.Type;
import org.opensearch.sql.legacy.antlr.semantic.types.base.OpenSearchIndex;

/**
 * Join operator
 */
public enum JoinOperator implements Type {
    JOIN;

    @Override
    public String getName() {
        return name();
    }

    @Override
    public Type construct(List<Type> others) {
        Optional<Type> isAnyNonIndexType = others.stream().
                                                  filter(type -> !(type instanceof OpenSearchIndex)).
                                                  findAny();
        if (isAnyNonIndexType.isPresent()) {
            return TYPE_ERROR;
        }
        return others.get(0);
    }

    @Override
    public String usage() {
        return "Please join index with other index or its nested field.";
    }

    @Override
    public String toString() {
        return "Operator [" + getName() + "]";
    }
}
