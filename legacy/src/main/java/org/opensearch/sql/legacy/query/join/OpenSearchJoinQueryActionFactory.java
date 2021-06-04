/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

/*
 *   Copyright 2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 *   Licensed under the Apache License, Version 2.0 (the "License").
 *   You may not use this file except in compliance with the License.
 *   A copy of the License is located at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *   or in the "license" file accompanying this file. This file is distributed
 *   on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 *   express or implied. See the License for the specific language governing
 *   permissions and limitations under the License.
 */

package org.opensearch.sql.legacy.query.join;

import java.util.List;
import org.opensearch.client.Client;
import org.opensearch.sql.legacy.domain.Condition;
import org.opensearch.sql.legacy.domain.JoinSelect;
import org.opensearch.sql.legacy.domain.hints.Hint;
import org.opensearch.sql.legacy.domain.hints.HintType;
import org.opensearch.sql.legacy.query.QueryAction;

/**
 * Created by Eliran on 15/9/2015.
 */
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
