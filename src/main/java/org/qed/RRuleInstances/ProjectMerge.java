package org.qed.RRuleInstances;

import kala.collection.Map;
import kala.collection.Seq;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.qed.RelRN;
import org.qed.RexRN;
import org.qed.RRule;
import org.qed.RuleBuilder;

public record ProjectMerge() implements RRule {
    static final RelRN source = RelRN.scan("Source", "Source_Type");
    static final RexRN inner = source.proj("inner", "Inner_Type");
    static final String outer = "outer";
    static final String outerType = "Outer_Type";

    @Override
    public RelRN before() {
        return source.project(inner).project(outer, outerType);
    }

    @Override
    public RelRN after() {
        return source.project(inner.proj(outer, outerType));
    }
}
