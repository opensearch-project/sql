package org.opensearch.sql.utils;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * The SQL (textual) query wrapper and fluent query builder
 */
public class Builder implements QueryBuilder<String> {
    private List<String> parameters = new LinkedList<>();
    private String name;
    private StringBuilder sqlBuilder;

    public static Builder instance() {
        return new Builder();
    }

    @Override
    public String build() {
        return this.getSQL();
    }

    @Override
    public QueryBuilder withName(String name) {
        this.name = name;
        return this;
    }

    public Builder() {
        this(new StringBuilder());
    }

    /**
     * Constructs a query builder from an existing SQL query.
     *
     * @param sql The existing SQL query.
     */
    public Builder(String sql) {
        if (sql == null) {
            throw new IllegalArgumentException();
        }

        sqlBuilder = new StringBuilder();

        append(sql);
    }

    private Builder(StringBuilder sqlBuilder) {
        this.sqlBuilder = sqlBuilder;
    }

    /**
     * Creates a "select" query.
     *
     * @param columns The column names.
     * @return The new {@link Builder} instance.
     */
    public static Builder select(String... columns) {
        if (columns == null || columns.length == 0) {
            throw new IllegalArgumentException();
        }

        var sqlBuilder = new StringBuilder();

        sqlBuilder.append("select ");

        var SQLQueryBuilder = new Builder(sqlBuilder);

        for (var i = 0; i < columns.length; i++) {
            if (i > 0) {
                SQLQueryBuilder.sqlBuilder.append(", ");
            }

            SQLQueryBuilder.append(columns[i]);
        }

        return SQLQueryBuilder;
    }

    /**
     * Appends a "from" clause to a query.
     *
     * @param tables The table names.
     * @return The {@link Builder} instance.
     */
    public Builder from(String... tables) {
        if (tables == null || tables.length == 0) {
            throw new IllegalArgumentException();
        }

        sqlBuilder.append(" from ");
        sqlBuilder.append(String.join(", ", tables));

        return this;
    }

    /**
     * Appends a "from" clause to a query.
     *
     * @param Builder A "select" subquery.
     * @param alias   The subquery's alias.
     * @return The {@link Builder} instance.
     */
    public Builder from(Builder Builder, String alias) {
        if (Builder == null || alias == null) {
            throw new IllegalArgumentException();
        }

        sqlBuilder.append(" from (");
        sqlBuilder.append(Builder.getSQL());
        sqlBuilder.append(") ");
        sqlBuilder.append(alias);

        parameters.addAll(Builder.parameters);

        return this;
    }

    /**
     * Appends a "join" clause to a query.
     *
     * @param table The table name.
     * @return The {@link Builder} instance.
     */
    public Builder join(String table) {
        if (table == null) {
            throw new IllegalArgumentException();
        }

        sqlBuilder.append(" join ");
        sqlBuilder.append(table);

        return this;
    }

    /**
     * Appends a "join" clause to a query.
     *
     * @param Builder A "select" subquery.
     * @param alias   The subquery's alias.
     * @return The {@link Builder} instance.
     */
    public Builder join(Builder Builder, String alias) {
        if (Builder == null || alias == null) {
            throw new IllegalArgumentException();
        }

        sqlBuilder.append(" join (");
        sqlBuilder.append(Builder.getSQL());
        sqlBuilder.append(") ");
        sqlBuilder.append(alias);

        parameters.addAll(Builder.parameters);

        return this;
    }

    /**
     * Appends a "left join" clause to a query.
     *
     * @param table The table name.
     * @return The {@link Builder} instance.
     */
    public Builder leftJoin(String table) {
        if (table == null) {
            throw new IllegalArgumentException();
        }

        sqlBuilder.append(" left join ");
        sqlBuilder.append(table);

        return this;
    }

    /**
     * Appends a "right join" clause to a query.
     *
     * @param table The table name.
     * @return The {@link Builder} instance.
     */
    public Builder rightJoin(String table) {
        if (table == null) {
            throw new IllegalArgumentException();
        }

        sqlBuilder.append(" right join ");
        sqlBuilder.append(table);

        return this;
    }

    /**
     * Appends an "on" clause to a query.
     *
     * @param predicates The clause predicates.
     * @return The {@link Builder} instance.
     */
    public Builder on(String... predicates) {
        return filter("on", predicates);
    }

    /**
     * Appends a "where" clause to a query.
     *
     * @param predicates The clause predicates.
     * @return The {@link Builder} instance.
     */
    public Builder where(String... predicates) {
        return filter("where", predicates);
    }

    private Builder filter(String clause, String... predicates) {
        if (predicates == null) {
            throw new IllegalArgumentException();
        }

        sqlBuilder.append(" ");
        sqlBuilder.append(clause);
        sqlBuilder.append(" ");

        for (var i = 0; i < predicates.length; i++) {
            if (i > 0) {
                sqlBuilder.append(" ");
            }

            append(predicates[i]);
        }

        return this;
    }

    /**
     * Creates an "and" conditional.
     *
     * @param predicates The conditional's predicates.
     * @return The conditional text.
     */
    public static String and(String... predicates) {
        return conditional("and", predicates);
    }

    /**
     * Creates an "or" conditional.
     *
     * @param predicates The conditional's predicates.
     * @return The conditional text.
     */
    public static String or(String... predicates) {
        return conditional("or", predicates);
    }

    private static String conditional(String operator, String... predicates) {
        if (predicates == null || predicates.length == 0) {
            throw new IllegalArgumentException();
        }

        var stringBuilder = new StringBuilder();

        stringBuilder.append(operator);
        stringBuilder.append(" ");

        if (predicates.length > 1) {
            stringBuilder.append("(");
        }

        for (var i = 0; i < predicates.length; i++) {
            if (i > 0) {
                stringBuilder.append(" ");
            }

            stringBuilder.append(predicates[i]);
        }

        if (predicates.length > 1) {
            stringBuilder.append(")");
        }

        return stringBuilder.toString();
    }

    /**
     * Creates an "and" conditional group.
     *
     * @param predicates The group's predicates.
     * @return The conditional text.
     */
    public static String allOf(String... predicates) {
        if (predicates == null || predicates.length == 0) {
            throw new IllegalArgumentException();
        }

        return conditionalGroup("and", predicates);
    }

    /**
     * Creates an "or" conditional group.
     *
     * @param predicates The group's predicates.
     * @return The conditional text.
     */
    public static String anyOf(String... predicates) {
        if (predicates == null || predicates.length == 0) {
            throw new IllegalArgumentException();
        }

        return conditionalGroup("or", predicates);
    }

    private static String conditionalGroup(String operator, String... predicates) {
        if (predicates == null || predicates.length == 0) {
            throw new IllegalArgumentException();
        }

        var stringBuilder = new StringBuilder();

        stringBuilder.append("(");

        for (var i = 0; i < predicates.length; i++) {
            if (i > 0) {
                stringBuilder.append(" ");
                stringBuilder.append(operator);
                stringBuilder.append(" ");
            }

            stringBuilder.append(predicates[i]);
        }

        stringBuilder.append(")");

        return stringBuilder.toString();
    }

    /**
     * Creates an "equal to" conditional.
     *
     * @param Builder The conditional's subquery.
     * @return The conditional text.
     */
    public static String equalTo(Builder Builder) {
        if (Builder == null) {
            throw new IllegalArgumentException();
        }

        return String.format("= (%s)", Builder);
    }

    /**
     * Creates a "not equal to" conditional.
     *
     * @param Builder The conditional's subquery.
     * @return The conditional text.
     */
    public static String notEqualTo(Builder Builder) {
        if (Builder == null) {
            throw new IllegalArgumentException();
        }

        return String.format("!= (%s)", Builder);
    }

    /**
     * Creates an "in" conditional.
     *
     * @param Builder The conditional's subquery.
     * @return The conditional text.
     */
    public static String in(Builder Builder) {
        if (Builder == null) {
            throw new IllegalArgumentException();
        }

        return String.format("in (%s)", Builder);
    }

    /**
     * Creates a "not in" conditional.
     *
     * @param Builder The conditional's subquery.
     * @return The conditional text.
     */
    public static String notIn(Builder Builder) {
        if (Builder == null) {
            throw new IllegalArgumentException();
        }

        return String.format("not in (%s)", Builder);
    }

    /**
     * Creates an "exists" conditional.
     *
     * @param Builder The conditional's subquery.
     * @return The conditional text.
     */
    public static String exists(Builder Builder) {
        if (Builder == null) {
            throw new IllegalArgumentException();
        }

        return String.format("exists (%s)", Builder);
    }

    /**
     * Creates a "not exists" conditional.
     *
     * @param Builder The conditional's subquery.
     * @return The conditional text.
     */
    public static String notExists(Builder Builder) {
        if (Builder == null) {
            throw new IllegalArgumentException();
        }

        return String.format("not exists (%s)", Builder);
    }

    /**
     * Appends an "order by" clause to a query.
     *
     * @param columns The column names.
     * @return The {@link Builder} instance.
     */
    public Builder orderBy(String... columns) {
        if (columns == null || columns.length == 0) {
            throw new IllegalArgumentException();
        }

        sqlBuilder.append(" order by ");
        sqlBuilder.append(String.join(", ", columns));

        return this;
    }

    /**
     * Appends a "limit" clause to a query.
     *
     * @param count The limit count.
     * @return The {@link Builder} instance.
     */
    public Builder limit(int count) {
        if (count < 0) {
            throw new IllegalArgumentException();
        }

        sqlBuilder.append(" limit ");
        sqlBuilder.append(count);

        return this;
    }

    /**
     * Appends a "for update" clause to a query.
     *
     * @return The {@link Builder} instance.
     */
    public Builder forUpdate() {
        sqlBuilder.append(" for update");

        return this;
    }

    /**
     * Appends a "union" clause to a query.
     *
     * @param Builder The query builder to append.
     * @return The {@link Builder} instance.
     */
    public Builder union(Builder Builder) {
        if (Builder == null) {
            throw new IllegalArgumentException();
        }

        sqlBuilder.append(" union ");
        sqlBuilder.append(Builder.getSQL());

        parameters.addAll(Builder.parameters);

        return this;
    }

    /**
     * Creates an "insert into" query.
     *
     * @param table The table name.
     * @return The new {@link Builder} instance.
     */
    public static Builder insertInto(String table) {
        if (table == null) {
            throw new IllegalArgumentException();
        }

        var sqlBuilder = new StringBuilder();

        sqlBuilder.append("insert into ");
        sqlBuilder.append(table);

        return new Builder(sqlBuilder);
    }

    /**
     * Appends column values to an "insert into" query.
     *
     * @param values The values to insert.
     * @return The {@link Builder} instance.
     */
    public Builder values(Map<String, ?> values) {
        if (values == null) {
            throw new IllegalArgumentException();
        }

        sqlBuilder.append(" (");

        List<String> columns = new ArrayList<>(values.keySet());

        var n = columns.size();

        for (var i = 0; i < n; i++) {
            if (i > 0) {
                sqlBuilder.append(", ");
            }

            sqlBuilder.append(columns.get(i));
        }

        sqlBuilder.append(") values (");

        for (var i = 0; i < n; i++) {
            if (i > 0) {
                sqlBuilder.append(", ");
            }

            encode(values.get(columns.get(i)));
        }

        sqlBuilder.append(")");

        return this;
    }

    /**
     * Creates an "update" query.
     *
     * @param table The table name.
     * @return The new {@link Builder} instance.
     */
    public static Builder update(String table) {
        if (table == null) {
            throw new IllegalArgumentException();
        }

        var sqlBuilder = new StringBuilder();

        sqlBuilder.append("update ");
        sqlBuilder.append(table);

        return new Builder(sqlBuilder);
    }

    /**
     * Appends column values to an "update" query.
     *
     * @param values The values to update.
     * @return The {@link Builder} instance.
     */
    public Builder set(Map<String, ?> values) {
        if (values == null) {
            throw new IllegalArgumentException();
        }

        sqlBuilder.append(" set ");

        var i = 0;

        for (Map.Entry<String, ?> entry : values.entrySet()) {
            if (i > 0) {
                sqlBuilder.append(", ");
            }

            sqlBuilder.append(entry.getKey());
            sqlBuilder.append(" = ");

            encode(entry.getValue());

            i++;
        }

        return this;
    }

    /**
     * Creates a "delete from" query.
     *
     * @param table The table name.
     * @return The new {@link Builder} instance.
     */
    public static Builder deleteFrom(String table) {
        if (table == null) {
            throw new IllegalArgumentException();
        }

        var sqlBuilder = new StringBuilder();

        sqlBuilder.append("delete from ");
        sqlBuilder.append(table);

        return new Builder(sqlBuilder);
    }


    /**
     * Returns the parameters parsed by the query builder.
     *
     * @return The parameters parsed by the query builder.
     */
    public Collection<String> getParameters() {
        return Collections.unmodifiableList(parameters);
    }

    /**
     * Returns the generated SQL.
     *
     * @return The generated SQL.
     */
    public String getSQL() {
        return sqlBuilder.toString();
    }

    private void append(String sql) {
        var quoted = false;

        var n = sql.length();
        var i = 0;

        while (i < n) {
            var c = sql.charAt(i++);

            if (c == ':' && !quoted) {
                var parameterBuilder = new StringBuilder();

                while (i < n) {
                    c = sql.charAt(i);

                    if (!Character.isJavaIdentifierPart(c)) {
                        break;
                    }

                    parameterBuilder.append(c);

                    i++;
                }

                if (parameterBuilder.length() == 0) {
                    throw new IllegalArgumentException("Missing parameter name.");
                }

                parameters.add(parameterBuilder.toString());

                sqlBuilder.append("?");
            } else if (c == '?' && !quoted) {
                parameters.add(null);

                sqlBuilder.append(c);
            } else {
                if (c == '\'') {
                    quoted = !quoted;
                }

                sqlBuilder.append(c);
            }
        }
    }

    private void encode(Object value) {
        if (value instanceof String) {
            var string = (String) value;

            if (string.startsWith(":") || string.equals("?")) {
                append(string);
            } else {
                sqlBuilder.append("'");

                for (int i = 0, n = string.length(); i < n; i++) {
                    var c = string.charAt(i);

                    if (c == '\'') {
                        sqlBuilder.append(c);
                    }

                    sqlBuilder.append(c);
                }

                sqlBuilder.append("'");
            }
        } else if (value instanceof Builder) {
            var SQLQueryBuilder = (Builder) value;

            sqlBuilder.append("(");
            sqlBuilder.append(SQLQueryBuilder.getSQL());
            sqlBuilder.append(")");

            parameters.addAll(SQLQueryBuilder.parameters);
        } else {
            sqlBuilder.append(value);
        }
    }

    /**
     * Returns the query as a string.
     * {@inheritDoc}
     */
    @Override
    public String toString() {
        var stringBuilder = new StringBuilder();

        var parameterIterator = parameters.iterator();

        for (int i = 0, n = sqlBuilder.length(); i < n; i++) {
            var c = sqlBuilder.charAt(i);

            if (c == '?') {
                var parameter = parameterIterator.next();

                if (parameter == null) {
                    stringBuilder.append(c);
                } else {
                    stringBuilder.append(':');
                    stringBuilder.append(parameter);
                }
            } else {
                stringBuilder.append(c);
            }
        }

        return stringBuilder.toString();
    }
}
