/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.legacy.query.join;

import java.util.List;
import org.opensearch.sql.legacy.domain.Condition;
import org.opensearch.sql.legacy.domain.JoinSelect;
import org.opensearch.sql.legacy.domain.hints.Hint;
import org.opensearch.sql.legacy.domain.hints.HintType;
import org.opensearch.sql.legacy.query.QueryAction;
import org.opensearch.transport.client.Client;

/** Created by Eliran on 15/9/2015. */
public class OpenSearchJoinQueryActionFactory {
  public static QueryAction createJoinAction(Client client, JoinSelect joinSelect) {
    List<Condition> connectedConditions = joinSelect.getConnectedConditions();
    boolean allEqual = true;
    for (Condition condition : connectedConditions) {
      if (condition.getOPERATOR() != Condition.OPERATOR.EQ) {
        allEqual = false;
        break;
      }
    }
    if (!allEqual) {
      return new OpenSearchNestedLoopsQueryAction(client, joinSelect);
    }

    boolean useNestedLoopsHintExist = false;
    for (Hint hint : joinSelect.getHints()) {
      if (hint.getType() == HintType.USE_NESTED_LOOPS) {
        useNestedLoopsHintExist = true;
        break;
      }
    }
    if (useNestedLoopsHintExist) {
      return new OpenSearchNestedLoopsQueryAction(client, joinSelect);
    }

    return new OpenSearchHashJoinQueryAction(client, joinSelect);
  }
}
