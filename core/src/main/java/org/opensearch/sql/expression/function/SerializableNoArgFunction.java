/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.expression.function;

import java.io.Serializable;
import java.util.function.Supplier;

/** Serializable no argument function. */
public interface SerializableNoArgFunction<T> extends Supplier<T>, Serializable {}
