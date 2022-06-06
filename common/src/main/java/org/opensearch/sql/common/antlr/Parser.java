package org.opensearch.sql.common.antlr;

import org.antlr.v4.runtime.tree.ParseTree;

public interface Parser {
  ParseTree parse(String query);
}
