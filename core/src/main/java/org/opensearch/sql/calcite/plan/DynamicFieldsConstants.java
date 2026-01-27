/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.plan;

import lombok.experimental.UtilityClass;

@UtilityClass
public class DynamicFieldsConstants {
  /** Special field name for the map containing dynamic fields */
  public static final String DYNAMIC_FIELDS_MAP = "_MAP";
}
