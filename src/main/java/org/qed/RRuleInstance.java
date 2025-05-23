package org.qed;

import kala.collection.Map;
import kala.collection.Seq;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.qed.RRuleInstance.JoinAssociate;
// import org.qed.RRuleInstance.JoinConditionPush.JoinPred;

public interface RRuleInstance {
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

    record FilterReduceFalse() implements RRule {
        static final RelRN source = RelRN.scan("Source", "Source_Type");

        @Override
        public RelRN before() {
            return source.filter(RexRN.falseLiteral());
        }

        @Override
        public RelRN after() {
            return source.empty();
        }
    }

    record FilterReduceTrue() implements RRule {
        static final RelRN source = RelRN.scan("Source", "Source_Type");

        @Override
        public RelRN before() {
            return source.filter(RexRN.trueLiteral());
        }

        @Override
        public RelRN after() {
            return source;
        }
    }

    // TBD: include intersect to make it a rule familiy
    record FilterSetOpTranspose() implements RRule {
        static final RelRN left = RelRN.scan("Left", "Common_Type");
        static final RelRN right = RelRN.scan("Right", "Common_Type");
        
        @Override
        public RelRN before() {
            RelRN projTmp = left.union(false, right);
            return projTmp.filter(projTmp.pred("filter"));
        }
        
        @Override
        public RelRN after() {
            RexRN leftPred = left.pred("filter");
            RexRN rightPred = right.pred("filter");
            return left.filter(leftPred).union(false, right.filter(rightPred));
        }
    }

    record IntersectMerge() implements RRule {
        // Use a common type for all relations to make them compatible
        static final RelRN a = RelRN.scan("A", "Common_Type");
        static final RelRN b = RelRN.scan("B", "Common_Type");
        static final RelRN c = RelRN.scan("C", "Common_Type");
    
        @Override
        public RelRN before() {
            // Nested INTERSECT: (A INTERSECT B) INTERSECT C
            return a.intersect(false, b).intersect(false, c);
        }
    
        @Override
        public RelRN after() {
            // Flattened INTERSECT: A INTERSECT B INTERSECT C
            return a.intersect(false, b, c);
        }
    }

    // record JoinConditionPush() implements RRule {
    //     static final RelRN left = RelRN.scan("Left", "Left_Type");
    //     static final RelRN right = RelRN.scan("Right", "Right_Type");
    //     static final JoinPred joinPred = new JoinPred(left, right);

    //     @Override
    //     public RelRN before() {
    //         return left.join(JoinRelType.INNER, joinPred, right);
    //     }

    //     @Override
    //     public RelRN after() {
    //         var leftRN = left.filter(joinPred.leftPred());
    //         var rightRN = right.filter(joinPred.rightPred());
    //         return leftRN.join(JoinRelType.INNER, joinPred.bothPred(), rightRN);
    //     }

    //     public record JoinPred(RelRN left, RelRN right) implements RexRN {

    //         @Override
    //         public RexNode semantics() {
    //             return RexRN.and(left.joinPred(bothPred(), right), left.joinField(0, right).pred(leftPred()),
    //                     left.joinField(1, right).pred(rightPred())).semantics();
    //         }

    //         public String bothPred() {return "both";}

    //         public String leftPred() {return "left";}

    //         public String rightPred() {return "right";}

    //     }
    // }

    record JoinAddRedundantSemiJoin() implements RRule {
        static final RelRN left = RelRN.scan("Left", "Left_Type");
        static final RelRN right = RelRN.scan("Right", "Right_Type");
        static final String pred = "pred";

        @Override
        public RelRN before() {
            return left.join(JoinRelType.INNER, pred, right);
        }

        @Override
        public RelRN after() {
            return left.join(JoinRelType.SEMI, pred, right).join(JoinRelType.INNER, pred, right);
        }
    }

    // Todo: explore join types, see line 102 of JoinAssociateRule
    record JoinAssociate() implements RRule.RRuleFamily {
        static final RelRN a = RelRN.scan("A", "A_Type");
        static final RelRN b = RelRN.scan("B", "B_Type");
        static final RelRN c = RelRN.scan("C", "C_Type");
        static final String pred_ab = "pred_ab";
        static final String pred_bc = "pred_bc";
        static final RelRN.Join.JoinType.MetaJoinType mjt_0 = new RelRN.Join.JoinType.MetaJoinType("mjt_0");
        static final RelRN.Join.JoinType.MetaJoinType mjt_1 = new RelRN.Join.JoinType.MetaJoinType("mjt_1");
        static final RelRN.Join.JoinType.MetaJoinType mjt_2 = new RelRN.Join.JoinType.MetaJoinType("mjt_2");
        static final RelRN.Join.JoinType.MetaJoinType mjt_3 = new RelRN.Join.JoinType.MetaJoinType("mjt_3");

        static final RelRN before_ab = a.join(mjt_0, RexRN.and(
                a.joinPred(pred_ab, b),
                new RexRN.JoinField(1, a, b).pred(SqlStdOperatorTable.IS_NOT_NULL)
        ), b);

        static final RelRN before = before_ab.join(mjt_1, RexRN.and(
                new RexRN.Pred(RuleBuilder.create().genericPredicateOp(pred_bc, true), before_ab.joinFields(c, 1, 2)),
                new RexRN.JoinField(1, before_ab, c).pred(SqlStdOperatorTable.IS_NOT_NULL)
        ), c);

        static final RelRN after_bc = b.join(mjt_2, RexRN.and(
                b.joinPred(pred_bc, c),
                new RexRN.JoinField(0, b, c).pred(SqlStdOperatorTable.IS_NOT_NULL)
        ), c);

        static final RelRN after = a.join(mjt_3, RexRN.and(
                new RexRN.Pred(RuleBuilder.create().genericPredicateOp(pred_ab, true), a.joinFields(after_bc, 0, 1)),
                new RexRN.JoinField(1, a, after_bc).pred(SqlStdOperatorTable.IS_NOT_NULL)
        ), after_bc);

        static final RRule template = new RRule() {
            @Override
            public RelRN before() {
                return before;
            }

            @Override
            public RelRN after() {
                return after;
            }

            @Override
            public String name() {
                return JoinAssociate.class.getSimpleName();
            }
        };

        static Seq<RRule.RRuleGenerator.MetaAssignment> assignments() {
            var joinTypes = Seq.of(JoinRelType.INNER, JoinRelType.LEFT, JoinRelType.RIGHT, JoinRelType.FULL).map(RelRN.Join.JoinType.ConcreteJoinType::new);
            return joinTypes.flatMap(jt0 -> joinTypes.flatMap(jt1 -> joinTypes.flatMap(jt2 -> joinTypes.map(jt3 -> new RRule.RRuleGenerator.MetaAssignment(Map.of(mjt_0, jt0, mjt_1, jt1, mjt_2, jt2, mjt_3, jt3))))));
        }

        @Override
        public Seq<RRule> family() {
            return new RRule.RRuleGenerator(template, assignments()).family();
        }
    }

    record JoinCommute() implements RRule {
        static final RelRN left = RelRN.scan("Left", "Left_Type");
        static final RelRN right = RelRN.scan("Right", "Right_Type");
        static final RexRN joinCond = left.joinPred("pred", right);
    
        @Override
        public RelRN before() {
            return left.join(JoinRelType.INNER, joinCond, right);
        }
    
        @Override
        public RelRN after() {
            // We need to swap the join fields in the condition
            RexRN commutedJoinCond = right.joinPred("pred", left);
            return right.join(JoinRelType.INNER, commutedJoinCond, left);
        }
    }

    record JoinExtractFilter() implements RRule {
        static final RelRN left = RelRN.scan("Left", "Left_Type");
        static final RelRN right = RelRN.scan("Right", "Right_Type");
        static final RexRN joinCond = left.joinPred("join", right);
    
        @Override
        public RelRN before() {
            return left.join(JoinRelType.INNER, joinCond, right);
        }
    
        @Override
        public RelRN after() {
            return left.join(JoinRelType.INNER, RexRN.trueLiteral(), right).filter(joinCond);
        }
    }

//    record JoinProjectTranspose() implements RRule {
//        
//    }

    // JoinConditionPush?
//    record JoinPushExpressions() implements RRule {
//
//    }

    // JoinConditionPush?
//    record JoinPushTransitivePredicates() implements RRule {
//
//    }

//    record JoinToSemiJoin() implements RRule {
//    
//    }

//    record JoinLeftUnionTranspose() implements RRule {
//    
//    }

//    record JoinRightUnionTranspose() implements RRule {
//
//    }

//    record ProjectJoinRemove() implements RRule {
//
//        @Override
//        public RelRN before() {
//            return null;
//        }
//
//        @Override
//        public RelRN after() {
//            return null;
//        }
//    }

//    record ProjectJoinJoinRemove() implements RRule {
//
//    }

   record ProjectJoinTranspose() implements RRule {
        static final RelRN left = RelRN.scan("Left", "Left_Type");
        static final RelRN right = RelRN.scan("Right", "Right_Type");
        static final RexRN proj = left.proj("proj", "Project_Type");
        static final String joinCond = left.joinPred("join", right);

        @Override
        public RelRN before() {
            return left.join(JoinRelType.INNER, joinCond, right).project(proj);
        }

        @Override
        public RelRN after() {
            return left.project(proj).join(JoinRelType.INNER, joinCond, right);
        }
   }

    record ProjectMerge() implements RRule {
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

    //TBD: currently provable for UNION ALL while unprovable for UNION
    // record ProjectSetOpTranspose() implements RRule {
    //     static final RelRN left = RelRN.scan("Left", "Common_Type");
    //     static final RelRN right = RelRN.scan("Right", "Common_Type");

    //     @Override
    //     public RelRN before() {
    //         RelRN projTmp = left.union(true, right);
    //         return projTmp.project(projTmp.proj("proj", "Proj_Type"));
    //     }
        
    //     @Override
    //     public RelRN after() {
    //         RelRN projA = left.project(left.proj("proj", "Proj_Type"));
    //         RelRN projB = right.project(right.proj("proj", "Proj_Type"));
    //         return projA.union(true, projB);
    //     }
    // }


    /* TBD: Already optimized by calcite? */
    // record ProjectRemove() implements RRule {
    //     static final RelRN source = RelRN.scan("Source", "Source_Type");

    //     @Override
    //     public RelRN before() {
    //         return source.project(source.field(0));
    //     }

    //     @Override
    //     public RelRN after() {
    //         return source;
    //     }
    // }

    record UnionMerge() implements RRule {
        static final RelRN a = RelRN.scan("A", "Common_Type");
        static final RelRN b = RelRN.scan("B", "Common_Type");
        static final RelRN c = RelRN.scan("C", "Common_Type");
    
        @Override
        public RelRN before() {
            return a.union(false, b).union(false, c);
        }
    
        @Override
        public RelRN after() {
            return null;
        }
    }

   record SemiJoinFilterTranspose() implements RRule {
        static final RelRN left = RelRN.scan("left", "Left_Type");
        static final RelRN right = RelRN.scan("right", "Right_Type");
        static final RexRN pred = left.pred("pred");
        static final RexRN joinCond = left.joinPred("join", right);
        
        @Override
        public RelRN before() {
            return left.join(JoinRelType.SEMI, joinCond, right).filter(pred);
        }
        
        @Override
        public RelRN after() {
            return left.filter(pred).join(JoinRelType.SEMI, joinCond, right);
        }
   }

   record SemiJoinJoinTranspose() implements RRule {
        static final RelRN r = RelRN.scan("R", "R_Type");
        static final RelRN s = RelRN.scan("S", "S_Type");
        static final RelRN t = RelRN.scan("T", "T_Type");
        static final RexRN semiCond = r.joinPred("semi", s);
        static final RexRN joinCond = r.joinPred("join", t);

        @Override
        public RelRN before() {
            return r.join(JoinRelType.INNER, joinCond, t).join(JoinRelType.SEMI, semiCond, s);
        }

        @Override
        public RelRN after() {
            return r.join(JoinRelType.SEMI, semiCond, s).join(JoinRelType.INNER, joinCond, t);
        }
   }

   record SemiJoinProjectTranspose() implements RRule {
        static final RelRN left = RelRN.scan("Left", "Left_Type");
        static final RelRN right = RelRN.scan("Right", "Right_Type");
        static final RexRN proj = left.proj("proj", "Project_Type");
        static final RexRN semiCond = left.joinPred("semi", right);

        @Override
        public RelRN before() {
            return left.join(JoinRelType.SEMI, semiCond, right).project(proj);
        }

        @Override
        public RelRN after() {
            return left.project(proj).join(JoinRelType.SEMI, semiCond, right);
        }
   }

   record SemiJoinRemove() implements RRule {
        static final RelRN left = RelRN.scan("Left", "Left_Type");
        static final RelRN right = RelRN.scan("Right", "Right_Type");

        @Override
        public RelRN before() {
            return left.join(JoinRelType.SEMI, RexRN.trueLiteral(), right);
        }

        @Override
        public RelRN after() {
            return left;
        }
   }

//    record UnionMerge() implements RRule {
//
//    }

//    record UnionRemove() implements RRule {
//
//    }
}

/*
 * Semantically identical cases:
 * FilterExpandIsNotDistinctFrom
 * FilterScan
 * JoinReduceExpression
 * ProjectReduceExpression
 * ProjectTableScan
 */
