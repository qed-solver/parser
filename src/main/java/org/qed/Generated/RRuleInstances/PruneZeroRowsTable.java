package org.qed.Generated.RRuleInstances;

import org.qed.RRule;
import org.qed.RelRN;
import org.qed.RexRN;

public record PruneZeroRowsTable() implements RRule {
    static final RelRN a = RelRN.scan("A", "Common_Type");

    @Override
    public RelRN before() {
        return a;
    }

    @Override
    public RelRN after() {
        return a;
    }
}
