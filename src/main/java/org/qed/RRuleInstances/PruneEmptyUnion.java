package org.qed.RRuleInstances;

import org.qed.RRule;
import org.qed.RelRN;
import org.qed.RexRN;

public record PruneEmptyUnion() implements RRule {
    static final RelRN a = RelRN.scan("A", "Common_Type");
    static final RelRN b = RelRN.scan("B", "Common_Type");

    @Override
    public RelRN before() {
        return a.empty().union(false, b.empty());
    }

    @Override
    public RelRN after() {
        return a.empty();
    }
}
