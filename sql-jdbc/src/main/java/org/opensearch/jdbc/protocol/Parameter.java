/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.jdbc.protocol;

public interface Parameter {

    Object getValue();

    String getType();
}
