/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.remote;

import org.opensearch.sql.ppl.ConvertCommandIT;

/**
 * Integration tests for the PPL convert command with Calcite enabled but pushdown disabled.
 *
 * <p>This test class extends ConvertCommandIT and runs all the same tests, but with pushdown
 * disabled to verify non-pushdown behavior.
 */
public class CalciteConvertCommandIT extends ConvertCommandIT {}
