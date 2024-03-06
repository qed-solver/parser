package org.cosette;

import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rex.RexNode;

public interface RRule {

    RelRN before();

    RelRN after();

    default String explain() {
        return STR."\{getClass().getName()}\n\{before().semantics().explain()}=>\n\{after().semantics().explain()}";
    }

    record FilterMerge() implements RRule {
        static final RelRN source = RelRN.scan("Source", "Source_Type");
        static final RexRN inner = source.pred("inner");
        static final RexRN outer = source.pred("outer");

        @Override
        public RelRN before() {
            return source.filter(inner).filter(outer);
        }

        @Override
        public RelRN after() {
            return source.filter(RexRN.and(inner, outer));
        }
    }

    record FilterIntoJoin() implements RRule {
        static final RelRN left = RelRN.scan("Left", "Left_Type");
        static final RelRN right = RelRN.scan("Right", "Right_Type");
        static final RexRN joinCond = left.joinPred("join", right);

        @Override
        public RelRN before() {
            var join = left.join(JoinRelType.INNER, joinCond, right);
            return join.filter("outer");
        }

        @Override
        public RelRN after() {
            return left.join(JoinRelType.INNER, RexRN.and(joinCond, left.joinPred("outer", right)), right);
        }
    }

    record FilterProjectTranspose() implements RRule {
        static final RelRN source = RelRN.scan("Source", "Source_Type");
        static final RexRN proj = source.proj("proj", "Project_Type");

        @Override
        public RelRN before() {
            return source.filter(proj.pred("pred")).project(proj);
        }

        @Override
        public RelRN after() {
            return source.project(proj).filter("pred");
        }
    }

    record JoinConditionPush() implements RRule {
        record JoinPred(RelRN left, RelRN right) implements RexRN {

            @Override
            public RexNode semantics() {
                return RexRN.and(left.joinPred(bothPred(), right), left.joinField(0, right).pred(leftPred()), left.joinField(1, right).pred(rightPred())).semantics();
            }

            public String bothPred() { return "both"; }
            public String leftPred() { return "left"; }
            public String rightPred() { return "right"; }

        }

        static final RelRN left = RelRN.scan("Left", "Left_Type");
        static final RelRN right = RelRN.scan("Right", "Right_Type");
        static final JoinPred joinPred = new JoinPred(left, right);

        @Override
        public RelRN before() {
            return left.join(JoinRelType.INNER, joinPred, right);
        }

        @Override
        public RelRN after() {
            var leftRN = left.filter(joinPred.leftPred());
            var rightRN = right.filter(joinPred.rightPred());
            return leftRN.join(JoinRelType.INNER, joinPred.bothPred(), rightRN);
        }
    }
}
