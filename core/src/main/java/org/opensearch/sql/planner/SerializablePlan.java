/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.planner;

import java.io.Externalizable;

/**
 * All subtypes of PhysicalPlan which needs to be serialized (in cursor, for pagination feature)
 * should follow one of the following options.
 * <ul>
 *   <li>Both:
 *     <ul>
 *       <li>Override both methods from {@link Externalizable}.</li>
 *       <li>Define a public no-arg constructor.</li>
 *     </ul>
 *   </li>
 *   <li>
 *     Overwrite {@link #getPlanForSerialization} to return
 *     another instance of {@link SerializablePlan}.
 *   </li>
 * </ul>
 */
public interface SerializablePlan extends Externalizable {

  /**
   * Override to return child or delegated plan, so parent plan should skip this one
   * for serialization, but it should try to serialize grandchild plan.
   * Imagine plan structure like this
   * <pre>
   *    A         -> this
   *    `- B      -> child
   *      `- C    -> this
   * </pre>
   * In that case only plans A and C should be attempted to serialize.
   * It is needed to skip a `ResourceMonitorPlan` instance only, actually.
   *
   *  <pre>{@code
   *    * A.writeObject(B.getPlanForSerialization());
   *  }</pre>
   * @return Next plan for serialization.
   */
  default SerializablePlan getPlanForSerialization() {
    return this;
  }
}
