package org.opensearch.sql.ast.tree;

import com.google.common.collect.ImmutableList;
import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.Setter;
import lombok.ToString;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.opensearch.sql.ast.expression.UnresolvedExpression;

import java.util.List;

@Getter
@Setter
@ToString
@EqualsAndHashCode(callSuper = false)
@RequiredArgsConstructor
@AllArgsConstructor
public class SPath extends UnresolvedPlan {
    private UnresolvedPlan child;

    private final UnresolvedExpression inField;

    @Nullable
    private final UnresolvedExpression outField;

    private final String path;

    @Override
    public UnresolvedPlan attach(UnresolvedPlan child) {
        this.child = child;
        return this;
    }

    @Override
    public List<UnresolvedPlan> getChild() {
        return this.child == null ? ImmutableList.of() : ImmutableList.of(this.child);
    }
}
