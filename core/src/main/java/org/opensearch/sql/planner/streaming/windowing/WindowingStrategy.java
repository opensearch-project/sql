/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.planner.streaming.windowing;

import lombok.Getter;
import lombok.RequiredArgsConstructor;
import org.opensearch.sql.planner.streaming.WindowAccumulator;
import org.opensearch.sql.planner.streaming.windowing.assigner.WindowAssigner;
import org.opensearch.sql.planner.streaming.windowing.trigger.WindowTrigger;

/**
 * Window strategy that defines how to group elements, assign window and emit results.
 */
@Getter
@RequiredArgsConstructor
public class WindowingStrategy {

  private final WindowAssigner windowAssigner;

  private final WindowAccumulator windowAccumulator;

  private final WindowTrigger windowTrigger;

}
