package org.qed.RRuleInstances;

import org.qed.RelRN;
import org.qed.RRule;

public record UnionMerge() implements RRule {
    static final RelRN a = RelRN.scan("A", "Common_Type");
    static final RelRN b = RelRN.scan("B", "Common_Type");
    static final RelRN c = RelRN.scan("C", "Common_Type");

    @Override
    public RelRN before() {
        return a.union(false, b).union(false, c);
    }

    @Override
    public RelRN after() {
        return a.union(false, b, c);
    }
}