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
 *   Copyright 2020 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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

package org.opensearch.sql.ppl.antlr;

import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.Lexer;
import org.antlr.v4.runtime.tree.ParseTree;
import org.opensearch.sql.common.antlr.CaseInsensitiveCharStream;
import org.opensearch.sql.common.antlr.SyntaxAnalysisErrorListener;
import org.opensearch.sql.ppl.antlr.parser.OpenSearchPPLLexer;
import org.opensearch.sql.ppl.antlr.parser.OpenSearchPPLParser;

/**
 * PPL Syntax Parser.
 */
public class PPLSyntaxParser {
  /**
   * Analyze the query syntax.
   */
  public ParseTree analyzeSyntax(String query) {
    OpenSearchPPLParser parser = createParser(createLexer(query));
    parser.addErrorListener(new SyntaxAnalysisErrorListener());
    return parser.root();
  }

  private OpenSearchPPLParser createParser(Lexer lexer) {
    return new OpenSearchPPLParser(
        new CommonTokenStream(lexer));
  }

  private OpenSearchPPLLexer createLexer(String query) {
    return new OpenSearchPPLLexer(
        new CaseInsensitiveCharStream(query));
  }
}
