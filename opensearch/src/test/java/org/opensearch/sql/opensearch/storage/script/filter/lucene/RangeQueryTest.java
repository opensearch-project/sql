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
 *    Copyright 2020 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 *    Licensed under the Apache License, Version 2.0 (the "License").
 *    You may not use this file except in compliance with the License.
 *    A copy of the License is located at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *    or in the "license" file accompanying this file. This file is distributed
 *    on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 *    express or implied. See the License for the specific language governing
 *    permissions and limitations under the License.
 *
 */

package org.opensearch.sql.opensearch.storage.script.filter.lucene;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.opensearch.sql.data.type.ExprCoreType.STRING;

import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator;
import org.junit.jupiter.api.Test;
import org.opensearch.sql.data.model.ExprValueUtils;
import org.opensearch.sql.opensearch.storage.script.filter.lucene.RangeQuery.Comparison;

@DisplayNameGeneration(DisplayNameGenerator.ReplaceUnderscores.class)
class RangeQueryTest {

  @Test
  void should_throw_exception_for_unsupported_comparison() {
    // Note that since we do switch check on enum comparison, this should'be impossible
    assertThrows(IllegalStateException.class, () ->
        new RangeQuery(Comparison.BETWEEN)
            .doBuild("name", STRING, ExprValueUtils.stringValue("John")));
  }

}
