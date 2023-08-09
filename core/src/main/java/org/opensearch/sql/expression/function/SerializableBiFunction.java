/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.expression.function;

import java.io.Serializable;
import java.util.function.BiFunction;

/** Serializable BiFunction. */
public interface SerializableBiFunction<T, U, R> extends BiFunction<T, U, R>, Serializable {}
