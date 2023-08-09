/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.legacy.parser;

import com.alibaba.druid.sql.ast.expr.SQLCharExpr;
import com.alibaba.druid.sql.visitor.SQLASTVisitor;


/**
 * Created by jheimbouch on 3/17/15.
 */
public class SQLOdbcExpr extends SQLCharExpr {

    private static final long serialVersionUID = 1L;

    public SQLOdbcExpr() {

    }

    public SQLOdbcExpr(String text) {
        super(text);
    }

    @Override
    public void output(StringBuffer buf) {
        if ((this.text == null) || (this.text.length() == 0)) {
            buf.append("NULL");
        } else {
            buf.append("{ts '");
            buf.append(this.text.replaceAll("'", "''"));
            buf.append("'}");
        }
    }

    @Override
    public String getText() {
        StringBuilder sb = new StringBuilder();
        sb.append("{ts '");
        sb.append(this.text);
        sb.append("'}");
        return sb.toString();
    }

    protected void accept0(SQLASTVisitor visitor) {
        visitor.visit(this);
        visitor.endVisit(this);
    }
}
