/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ast.expression;

import com.google.common.collect.ImmutableList;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import org.opensearch.sql.ast.AbstractNodeVisitor;

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Expression node of scalar function. Params include function name (@funcName) and function
 * arguments (@funcArgs)
 */
@Getter
@EqualsAndHashCode(callSuper = false)
@RequiredArgsConstructor
public class Geoip extends UnresolvedExpression {
  private final UnresolvedExpression datasource;
  private final UnresolvedExpression ipAddress;
  private final String properties;

  @Override
  public List<UnresolvedExpression> getChild() {
    return ImmutableList.of(datasource, ipAddress);
  }

  @Override
  public <R, C> R accept(AbstractNodeVisitor<R, C> nodeVisitor, C context) {
    return nodeVisitor.visitGeoip(this, context);
  }
}
