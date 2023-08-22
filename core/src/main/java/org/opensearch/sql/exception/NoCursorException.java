/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.exception;

/**
 * This should be thrown on serialization of a PhysicalPlan tree if paging is finished. Processing
 * of such exception should outcome of responding no cursor to the user.
 */
public class NoCursorException extends RuntimeException {}
