/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.jdbc.protocol;

public interface ColumnDescriptor {
    /**
     * Column name
     * @return
     */
    String getName();

    /**
     * Label
     */
     String getLabel();

    /**
     * Column data type
     * @return
     */
    String getType();
}
