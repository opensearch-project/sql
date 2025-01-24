/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.expression.window.frame;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;
import org.opensearch.sql.expression.parse.PatternsExpression;
import org.opensearch.sql.expression.window.WindowDefinition;

@Getter
@EqualsAndHashCode(callSuper = true)
@ToString
public class StreamPatternRowWindowFrame extends CurrentRowWindowFrame {

  final PatternsExpression patternsExpression;

  public StreamPatternRowWindowFrame(
      WindowDefinition windowDefinition, PatternsExpression patternsExpression) {
    super(windowDefinition);
    this.patternsExpression = patternsExpression;
  }
}
