/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.legacy.query.join;

import org.opensearch.client.Client;
import org.opensearch.sql.legacy.domain.JoinSelect;
import org.opensearch.sql.legacy.domain.Where;
import org.opensearch.sql.legacy.domain.hints.Hint;
import org.opensearch.sql.legacy.domain.hints.HintType;
import org.opensearch.sql.legacy.exception.SqlParseException;

/** Created by Eliran on 15/9/2015. */
public class OpenSearchNestedLoopsQueryAction extends OpenSearchJoinQueryAction {

  public OpenSearchNestedLoopsQueryAction(Client client, JoinSelect joinSelect) {
    super(client, joinSelect);
  }

  @Override
  protected void fillSpecificRequestBuilder(JoinRequestBuilder requestBuilder)
      throws SqlParseException {
    NestedLoopsElasticRequestBuilder nestedBuilder =
        (NestedLoopsElasticRequestBuilder) requestBuilder;
    Where where = joinSelect.getConnectedWhere();
    nestedBuilder.setConnectedWhere(where);
  }

  @Override
  protected JoinRequestBuilder createSpecificBuilder() {
    return new NestedLoopsElasticRequestBuilder();
  }

  @Override
  protected void updateRequestWithHints(JoinRequestBuilder requestBuilder) {
    super.updateRequestWithHints(requestBuilder);
    for (Hint hint : this.joinSelect.getHints()) {
      if (hint.getType() == HintType.NL_MULTISEARCH_SIZE) {
        Integer multiSearchMaxSize = (Integer) hint.getParams()[0];
        ((NestedLoopsElasticRequestBuilder) requestBuilder)
            .setMultiSearchMaxSize(multiSearchMaxSize);
      }
    }
  }

  private String removeAlias(String field) {
    String alias = joinSelect.getFirstTable().getAlias();
    if (!field.startsWith(alias + ".")) {
      alias = joinSelect.getSecondTable().getAlias();
    }
    return field.replace(alias + ".", "");
  }
}
