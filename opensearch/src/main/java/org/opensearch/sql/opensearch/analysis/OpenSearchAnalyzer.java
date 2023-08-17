package org.opensearch.sql.opensearch.analysis;

import org.opensearch.sql.analysis.Analyzer;
import org.opensearch.sql.analysis.ExpressionAnalyzer;
import org.opensearch.sql.analysis.NamedExpressionAnalyzer;
import org.opensearch.sql.datasource.DataSourceService;
import org.opensearch.sql.expression.function.BuiltinFunctionRepository;

public class OpenSearchAnalyzer extends Analyzer {
  /**
   * Constructor.
   *
   * @param dataSourceService
   * @param repository
   */
  public OpenSearchAnalyzer(DataSourceService dataSourceService,
                            BuiltinFunctionRepository repository) {
    super(
        new OpenSearchExpressionAnalyzer(repository),
        new OpenSearchSelectExpressionAnalyzer(new OpenSearchExpressionAnalyzer(repository)),
        new NamedExpressionAnalyzer(new OpenSearchExpressionAnalyzer(repository)),
        dataSourceService,
        repository);
  }
}
