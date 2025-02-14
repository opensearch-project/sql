/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.sql.expression.window.frame;

import com.google.common.collect.PeekingIterator;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;
import org.opensearch.sql.common.patterns.BrainLogParser;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.expression.Expression;
import org.opensearch.sql.expression.window.WindowDefinition;

@EqualsAndHashCode(callSuper = true)
@ToString
public class BufferPatternRowsWindowFrame extends PeerRowsWindowFrame {

  private final Expression sourceField;

  @Getter private final BrainLogParser logParser;

  private final List<List<String>> preprocessedMessages;

  public BufferPatternRowsWindowFrame(
      WindowDefinition windowDefinition, BrainLogParser logParser, Expression sourceField) {
    super(windowDefinition);
    this.logParser = logParser;
    this.sourceField = sourceField;
    this.preprocessedMessages = new ArrayList<>();
  }

  @Override
  public void load(PeekingIterator<ExprValue> it) {
    if (hasNext()) {
      return;
    }

    this.preprocessedMessages.clear();
    loadAllRows(it);

    List<String> logMessages =
        peers.stream()
            .map(
                exprValue -> {
                  ExprValue value = sourceField.valueOf(exprValue.bindingTuples());
                  return value.stringValue();
                })
            .collect(Collectors.toList());
    this.preprocessedMessages.addAll(logParser.preprocessAllLogs(logMessages));
  }

  public List<String> currentPreprocessedMessage() {
    return this.preprocessedMessages.get(position);
  }
}
