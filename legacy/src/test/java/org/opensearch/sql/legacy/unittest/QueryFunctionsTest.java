/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.legacy.unittest;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.junit.Assert.assertTrue;
import static org.opensearch.index.query.QueryBuilders.constantScoreQuery;
import static org.opensearch.index.query.QueryBuilders.matchPhraseQuery;
import static org.opensearch.index.query.QueryBuilders.matchQuery;
import static org.opensearch.index.query.QueryBuilders.multiMatchQuery;
import static org.opensearch.index.query.QueryBuilders.nestedQuery;
import static org.opensearch.index.query.QueryBuilders.queryStringQuery;
import static org.opensearch.index.query.QueryBuilders.wildcardQuery;
import static org.opensearch.sql.legacy.util.SqlExplainUtils.explain;

import java.sql.SQLFeatureNotSupportedException;
import org.apache.lucene.search.join.ScoreMode;
import org.hamcrest.Matcher;
import org.junit.Test;
import org.mockito.Mockito;
import org.opensearch.client.Client;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.core.common.Strings;
import org.opensearch.index.query.AbstractQueryBuilder;
import org.opensearch.index.query.MultiMatchQueryBuilder;
import org.opensearch.search.builder.SearchSourceBuilder.ScriptField;
import org.opensearch.sql.legacy.exception.SQLFeatureDisabledException;
import org.opensearch.sql.legacy.exception.SqlParseException;
import org.opensearch.sql.legacy.query.OpenSearchActionFactory;
import org.opensearch.sql.legacy.util.CheckScriptContents;
import org.opensearch.sql.legacy.util.TestsConstants;

public class QueryFunctionsTest {

    private static final String SELECT_ALL = "SELECT *";
    private static final String FROM_ACCOUNTS = "FROM " + TestsConstants.TEST_INDEX_ACCOUNT + "/account";
    private static final String FROM_NESTED = "FROM " + TestsConstants.TEST_INDEX_NESTED_TYPE + "/nestedType";
    private static final String FROM_PHRASE = "FROM " + TestsConstants.TEST_INDEX_PHRASE + "/phrase";

    @Test
    public void query() {
        assertThat(
            query(
                FROM_ACCOUNTS,
                "WHERE QUERY('CA')"
            ),
            contains(
                queryStringQuery("CA")
            )
        );
    }

    @Test
    public void matchQueryRegularField() {
        assertThat(
            query(
                FROM_ACCOUNTS,
                "WHERE MATCH_QUERY(firstname, 'Ayers')"
            ),
            contains(
                matchQuery("firstname", "Ayers")
            )
        );
    }

    @Test
    public void matchQueryNestedField() {
        assertThat(
            query(
                FROM_NESTED,
                "WHERE MATCH_QUERY(NESTED(comment.data), 'aa')"
            ),
            contains(
                nestedQuery("comment", matchQuery("comment.data", "aa"), ScoreMode.None)
            )
        );
    }

    @Test
    public void scoreQuery() {
        assertThat(
            query(
                FROM_ACCOUNTS,
                "WHERE SCORE(MATCH_QUERY(firstname, 'Ayers'), 10)"
            ),
            contains(
                constantScoreQuery(
                    matchQuery("firstname", "Ayers")
                ).boost(10)
            )
        );
    }

    @Test
    public void scoreQueryWithNestedField() {
        assertThat(
            query(
                FROM_NESTED,
                "WHERE SCORE(MATCH_QUERY(NESTED(comment.data), 'ab'), 10)"
            ),
            contains(
                constantScoreQuery(
                    nestedQuery("comment", matchQuery("comment.data", "ab"), ScoreMode.None)
                ).boost(10)
            )
        );
    }

    @Test
    public void wildcardQueryRegularField() {
        assertThat(
            query(
                FROM_ACCOUNTS,
                "WHERE WILDCARD_QUERY(city.keyword, 'B*')"
            ),
            contains(
                wildcardQuery("city.keyword", "B*")
            )
        );
    }

    @Test
    public void wildcardQueryNestedField() {
        assertThat(
            query(
                FROM_NESTED,
                "WHERE WILDCARD_QUERY(nested(comment.data), 'a*')"
            ),
            contains(
                nestedQuery("comment", wildcardQuery("comment.data", "a*"), ScoreMode.None)
            )
        );
    }

    @Test
    public void matchPhraseQueryDefault() {
        assertThat(
            query(
                FROM_PHRASE,
                "WHERE MATCH_PHRASE(phrase, 'brown fox')"
            ),
            contains(
                matchPhraseQuery("phrase", "brown fox")
            )
        );
    }

    @Test
    public void matchPhraseQueryWithSlop() {
        assertThat(
            query(
                FROM_PHRASE,
                "WHERE MATCH_PHRASE(phrase, 'brown fox', slop=2)"
            ),
            contains(
                matchPhraseQuery("phrase", "brown fox").slop(2)
            )
        );
    }

    @Test
    public void multiMatchQuerySingleField() {
        assertThat(
            query(
                FROM_ACCOUNTS,
                "WHERE MULTI_MATCH(query='Ayers', fields='firstname')"
            ),
            contains(
                multiMatchQuery("Ayers").field("firstname")
            )
        );
    }

    @Test
    public void multiMatchQueryWildcardField() {
        assertThat(
            query(
                FROM_ACCOUNTS,
                "WHERE MULTI_MATCH(query='Ay', fields='*name', type='phrase_prefix')"
            ),
            contains(
                multiMatchQuery("Ay").
                                field("*name").
                                type(MultiMatchQueryBuilder.Type.PHRASE_PREFIX)
            )
        );
    }

    @Test
    public void numberLiteralInSelectField() {
        String query = "SELECT 2 AS number FROM bank WHERE age > 20";
        ScriptField scriptField = CheckScriptContents.getScriptFieldFromQuery(query);
        assertTrue(
                CheckScriptContents.scriptContainsString(
                     scriptField,
                     "def assign"
                )
        );
    }

    @Test
    public void ifFunctionWithConditionStatement() {
        String query = "SELECT IF(age > 35, 'elastic', 'search') AS Ages FROM accounts";
        ScriptField scriptField = CheckScriptContents.getScriptFieldFromQuery(query);
        assertTrue(
                CheckScriptContents.scriptContainsString(
                        scriptField,
                        "boolean cond = doc['age'].value > 35;"
                )
        );
    }

    @Test
    public void ifFunctionWithEquationConditionStatement() {
        String query = "SELECT IF(age = 35, 'elastic', 'search') AS Ages FROM accounts";
        ScriptField scriptField = CheckScriptContents.getScriptFieldFromQuery(query);
        assertTrue(
                CheckScriptContents.scriptContainsString(
                        scriptField,
                        "boolean cond = doc['age'].value == 35;"
                )
        );
    }

    @Test
    public void ifFunctionWithConstantConditionStatement() {
        String query = "SELECT IF(1 = 2, 'elastic', 'search') FROM accounts";
        ScriptField scriptField = CheckScriptContents.getScriptFieldFromQuery(query);
        assertTrue(
                CheckScriptContents.scriptContainsString(
                        scriptField,
                        "boolean cond = 1 == 2;"
                )
        );
    }

    @Test
    public void ifNull() {
        String query = "SELECT IFNULL(lastname, 'Unknown') FROM accounts";
        ScriptField scriptField = CheckScriptContents.getScriptFieldFromQuery(query);
        assertTrue(
                CheckScriptContents.scriptContainsString(
                        scriptField,
                        "doc['lastname'].size()==0"
                )
        );
    }

    @Test
    public void isNullWithMathExpr() {
        String query = "SELECT ISNULL(1+1) FROM accounts";
        ScriptField scriptField = CheckScriptContents.getScriptFieldFromQuery(query);
        assertTrue(
                CheckScriptContents.scriptContainsString(
                        scriptField,
                        "catch(ArithmeticException e)"
                )
        );

    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void emptyQueryShouldThrowSQLFeatureNotSupportedException()
        throws SQLFeatureNotSupportedException, SqlParseException, SQLFeatureDisabledException {
        OpenSearchActionFactory.create(Mockito.mock(Client.class), "");
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void emptyNewLineQueryShouldThrowSQLFeatureNotSupportedException()
        throws SQLFeatureNotSupportedException, SqlParseException, SQLFeatureDisabledException {
        OpenSearchActionFactory.create(Mockito.mock(Client.class), "\n");
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void emptyNewLineQueryShouldThrowSQLFeatureNotSupportedException2()
        throws SQLFeatureNotSupportedException, SqlParseException, SQLFeatureDisabledException {
        OpenSearchActionFactory.create(Mockito.mock(Client.class), "\r\n");
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void queryWithoutSpaceShouldSQLFeatureNotSupportedException()
        throws SQLFeatureNotSupportedException, SqlParseException, SQLFeatureDisabledException {
        OpenSearchActionFactory.create(Mockito.mock(Client.class), "SELE");
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void spacesOnlyQueryShouldThrowSQLFeatureNotSupportedException()
        throws SQLFeatureNotSupportedException, SqlParseException, SQLFeatureDisabledException {
        OpenSearchActionFactory.create(Mockito.mock(Client.class), "      ");
    }

    private String query(String from, String... statements) {
        return explain(SELECT_ALL + " " + from + " " + String.join(" ", statements));
    }

    private String query(String sql) {
        return explain(sql);
    }

    private Matcher<String> contains(AbstractQueryBuilder queryBuilder) {
        return containsString(Strings.toString(XContentType.JSON, queryBuilder, false, false));
    }
}
