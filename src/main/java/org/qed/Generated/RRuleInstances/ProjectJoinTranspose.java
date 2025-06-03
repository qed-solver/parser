package org.qed.Generated.RRuleInstances;

import org.apache.calcite.rel.core.JoinRelType;
import org.qed.RelRN;
import org.qed.RexRN;
import org.qed.RRule;

/** Project ↔ Join transpose (ordering-only version). */
public record ProjectJoinTranspose() implements RRule {

    /* Base relations */
    static final RelRN left  = RelRN.scan("Left",  "left_type");
    static final RelRN right = RelRN.scan("Right", "right_type");

    /* A projection that keeps (or derives) one column from the left input. */
    static final RexRN proj = left.proj("proj", "proj_type");

    /* Generic join predicate (could be equality, TRUE, etc.). */
    static final RexRN cond = left.joinPred("cond", right);

    /** BEFORE:  Project applied to the left input **before** the Join. */
    @Override
    public RelRN before() {
        return left                // 1️⃣ project first
                .join(JoinRelType.INNER, cond, right).project(proj); // 2️⃣ then join
    }

    /** AFTER:  The same join with the projected left input already embedded;
     *          no extra top-level Project, so the row-type is unchanged. */
    @Override
    public RelRN after() {
        RelRN newLeft = left.project(proj);           // left side already trimmed
        return newLeft.join(JoinRelType.INNER,        // join stays the same
                newLeft.joinPred("cond", right.project(proj)),
                right.project(proj)); // right side also trimmed
    }
}
