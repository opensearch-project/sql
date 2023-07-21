/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.expression.function;

import org.opensearch.sql.storage.Table;

/**
 * Interface for table function which returns Table when executed.
 */
public interface TableFunctionImplementation extends FunctionImplementation {

  Table applyArguments();

}
