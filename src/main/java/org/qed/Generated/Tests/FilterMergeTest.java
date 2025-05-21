package org.qed.Generated.Tests;

import kala.collection.Seq;
import kala.tuple.Tuple;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.qed.Generated.CalciteTester;
import org.qed.RelType;
import org.qed.Generated.RRuleInstances.FilterMerge;
import org.qed.RuleBuilder;

/**
 * Test for the FilterMerge rule.
 */
public class FilterMergeTest {

    /**
     * Run test for FilterMerge rule.
     */
    public static void runTest() {
        var tester = new CalciteTester();
        var builder = RuleBuilder.create();
        var table = builder.createQedTable(Seq.of(Tuple.of(RelType.fromString("INTEGER", true), false)));
        builder.addTable(table);
        
        var before = builder.scan(table.getName())
                .filter(builder.call(builder.genericPredicateOp("inner", true), builder.fields()))
                .filter(builder.call(builder.genericPredicateOp("outer", true), builder.fields()))
                .build();
                
        var after = builder.scan(table.getName()).filter(builder.call(SqlStdOperatorTable.AND,
                        builder.call(builder.genericPredicateOp("inner", true), builder.fields()),
                        builder.call(builder.genericPredicateOp("outer", true), builder.fields())))
                .build();
                
        var runner = CalciteTester.loadRule(org.qed.Generated.FilterMerge.Config.DEFAULT.toRule());
        tester.verify(runner, before, after);
    }
    
    /**
     * Main method to run this test independently.
     */
    public static void main(String[] args) {
        System.out.println("Running FilterMerge test...");
        runTest();
    }
}