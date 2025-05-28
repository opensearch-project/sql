/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ast.expression.subquery;

import lombok.EqualsAndHashCode;
import org.opensearch.sql.ast.expression.UnresolvedExpression;

/** Basic class of subquery expression */
@EqualsAndHashCode(callSuper = false)
public abstract class SubqueryExpression extends UnresolvedExpression {}
