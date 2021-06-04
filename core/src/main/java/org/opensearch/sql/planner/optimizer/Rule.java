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
 *
 *    Copyright 2020 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 *    Licensed under the Apache License, Version 2.0 (the "License").
 *    You may not use this file except in compliance with the License.
 *    A copy of the License is located at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    or in the "license" file accompanying this file. This file is distributed
 *    on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 *    express or implied. See the License for the specific language governing
 *    permissions and limitations under the License.
 *
 */

package org.opensearch.sql.planner.optimizer;

import com.facebook.presto.matching.Captures;
import com.facebook.presto.matching.Pattern;
import org.opensearch.sql.planner.logical.LogicalPlan;

/**
 * Optimization Rule.
 * @param <T> LogicalPlan.
 */
public interface Rule<T> {

  /**
   * Get the {@link Pattern}.
   */
  Pattern<T> pattern();

  /**
   * Apply the Rule to the LogicalPlan.
   * @param plan LogicalPlan which match the Pattern.
   * @param captures A list of LogicalPlan which are captured by the Pattern.
   * @return the transfromed LogicalPlan.
   */
  LogicalPlan apply(T plan, Captures captures);
}
