package org.qed.Generated.Tests;

import kala.collection.Seq;
import kala.tuple.Tuple;
import org.apache.calcite.rel.core.JoinRelType;
import org.qed.Generated.CalciteTester;
import org.qed.RelType;
import org.qed.Generated.RRuleInstances.JoinExtractFilter;
import org.qed.RuleBuilder;

/**
 * Test for the JoinExtractFilter rule.
 */
public class JoinExtractFilterTest {

    /**
     * Run test for JoinExtractFilter rule.
     */
    public static void runTest() {
        var tester = new CalciteTester();
        var builder = RuleBuilder.create();
        var leftTable = builder.createQedTable(Seq.of(Tuple.of(RelType.fromString("INTEGER", true), false)));
        var rightTable = builder.createQedTable(Seq.of(Tuple.of(RelType.fromString("VARCHAR", true), false)));
        builder.addTable(leftTable);
        builder.addTable(rightTable);
        
        var leftScan = builder.scan(leftTable.getName()).build();
        var rightScan = builder.scan(rightTable.getName()).build();
        
        var before = builder.push(leftScan)
                .push(rightScan)
                .join(JoinRelType.INNER, builder.call(builder.genericPredicateOp("join", true), 
                        builder.field(2, 0, 0), builder.field(2, 1, 0)))
                .build();
        
        var trueJoin = builder.push(leftScan)
                .push(rightScan)
                .join(JoinRelType.INNER, builder.literal(true))
                .build();
                
        var after = builder.push(trueJoin)
                .filter(builder.call(builder.genericPredicateOp("join", true), builder.field(0), builder.field(1)))
                .build();
        
        var runner = CalciteTester.loadRule(org.qed.Generated.JoinExtractFilter.Config.DEFAULT.toRule());
        tester.verify(runner, before, after);
    }
    
    /**
     * Main method to run this test independently.
     */
    public static void main(String[] args) {
        System.out.println("Running JoinExtractFilter test...");
        runTest();
    }
}