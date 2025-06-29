package org.qed.Generated.RRuleInstances;

import org.qed.RRule;
import org.qed.RelRN;
import org.qed.RexRN;

public record PruneEmptyMinus() implements RRule {
    static final RelRN a = RelRN.scan("A", "Common_Type");
    static final RelRN b = RelRN.scan("B", "Common_Type");

    @Override
    public RelRN before() {
        return a.empty().minus(false, b);
    }

    @Override
    public RelRN after() {
        return a.empty();
    }
}
