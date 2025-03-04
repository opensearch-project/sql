/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.udf;

public interface UserDefinedAggFunction<S extends Accumulator> {
    S init();

    Object result(S accumulator);

    // Add values to the accumulator
    S add(S acc, Object... values);
}