/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ddl.view;

import lombok.RequiredArgsConstructor;
import org.opensearch.sql.ddl.DataDefinitionTask;
import org.opensearch.sql.ddl.QueryService;

/**
 * Base class for view DDL.
 */
@RequiredArgsConstructor
public abstract class ViewDataDefinitionTask implements DataDefinitionTask {

  /**
   * Query service to submit query.
   */
  protected final QueryService queryService;

}
