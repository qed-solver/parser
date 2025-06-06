package org.qed.Generated.RRuleInstances;

import kala.collection.Map;
import kala.collection.Seq;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.qed.RelRN;
import org.qed.RexRN;
import org.qed.RRule;
import org.qed.RuleBuilder;

public record MinusMerge() implements RRule {
    static final RelRN a = RelRN.scan("A", "Common_Type");
    static final RelRN b = RelRN.scan("B", "Common_Type");
    static final RelRN c = RelRN.scan("C", "Common_Type");

    @Override
    public RelRN before() {
        return a.minus(false, b).minus(false, c);
    }

    @Override
    public RelRN after() {
        return a.minus(false, b.union(false, c));
    }
}
