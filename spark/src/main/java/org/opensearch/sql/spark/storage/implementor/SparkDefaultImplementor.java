/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.spark.storage.implementor;

import lombok.RequiredArgsConstructor;
import org.opensearch.sql.planner.DefaultImplementor;
import org.opensearch.sql.spark.storage.SparkMetricScan;

/**
 * Default Implementor of Logical plan for spark.
 */
@RequiredArgsConstructor
public class SparkDefaultImplementor
    extends DefaultImplementor<SparkMetricScan> {

}
