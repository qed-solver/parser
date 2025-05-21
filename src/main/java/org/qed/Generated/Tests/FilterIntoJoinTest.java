
package org.qed.Generated.Tests;

import kala.collection.Seq;
import kala.tuple.Tuple;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.qed.Generated.CalciteTester;
import org.qed.RelType;
import org.qed.Generated.RRuleInstances.FilterIntoJoin;
import org.qed.RuleBuilder;

/**
 * Test for the FilterIntoJoin rule.
 */
public class FilterIntoJoinTest {

    /**
     * Run test for FilterIntoJoin rule.
     */
    public static void runTest() {
        var tester = new CalciteTester();
        var builder = RuleBuilder.create();
        var table = builder.createQedTable(Seq.of(Tuple.of(RelType.fromString("INTEGER", true), false)));
        builder.addTable(table);
        
        var before = builder.scan(table.getName())
                .scan(table.getName())
                .join(JoinRelType.INNER, builder.call(builder.genericPredicateOp("join", true), builder.joinFields()))
                .filter(builder.call(builder.genericPredicateOp("pred", true), builder.fields()))
                .build();
                
        var after = builder.scan(table.getName())
                .scan(table.getName())
                .join(JoinRelType.INNER, builder.call(SqlStdOperatorTable.AND,
                        builder.call(builder.genericPredicateOp("join", true), builder.joinFields()),
                        builder.call(builder.genericPredicateOp("pred", true), builder.joinFields())))
                .build();
                
        var runner = CalciteTester.loadRule(org.qed.Generated.FilterIntoJoin.Config.DEFAULT.toRule());
        tester.verify(runner, before, after);
    }
    
    /**
     * Main method to run this test independently.
     */
    public static void main(String[] args) {
        System.out.println("Running FilterIntoJoin test...");
        runTest();
    }
}