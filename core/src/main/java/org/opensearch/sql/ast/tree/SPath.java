package org.opensearch.sql.ast.tree;

import com.google.common.collect.ImmutableList;
import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.RequiredArgsConstructor;
import lombok.ToString;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.opensearch.sql.ast.AbstractNodeVisitor;
import org.opensearch.sql.ast.dsl.AstDSL;
import org.opensearch.sql.ast.expression.Argument;

import java.util.List;

@ToString
@EqualsAndHashCode(callSuper = false)
@RequiredArgsConstructor
@AllArgsConstructor
public class SPath extends UnresolvedPlan {
    private UnresolvedPlan child;

    private final Argument inField;

    @Nullable
    private final Argument outField;

    private final Argument path;

    @Override
    public UnresolvedPlan attach(UnresolvedPlan child) {
        this.child = child;
        return this;
    }

    @Override
    public List<UnresolvedPlan> getChild() {
        return this.child == null ? ImmutableList.of() : ImmutableList.of(this.child);
    }

    @Override
    public <T, C> T accept(AbstractNodeVisitor<T, C> nodeVisitor, C context) {
        return nodeVisitor.visitSpath(this, context);
    }

    public Eval rewriteAsEval() {
        Argument outField = this.outField;
        if (outField == null) {
            outField = new Argument("output", this.path.getValue());
        }

        return AstDSL.eval(
                this.child,
                AstDSL.let(AstDSL.field(outField.getValue()), AstDSL.function(
                        "json_extract", AstDSL.field(inField.getValue()), this.path.getValue()))
        );
    }
}
