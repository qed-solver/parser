package org.qed.Generated.Tests;

import kala.collection.Seq;
import kala.tuple.Tuple;
import org.apache.calcite.rel.core.JoinRelType;
import org.qed.Generated.CalciteTester;
import org.qed.RelType;
import org.qed.Generated.RRuleInstances.SemiJoinFilterTranspose;
import org.qed.RuleBuilder;

/**
 * Test for the SemiJoinFilterTranspose rule.
 */
public class SemiJoinFilterTransposeTest {

    /**
     * Run test for SemiJoinFilterTranspose rule.
     */
    public static void runTest() {
        var tester = new CalciteTester();
        var builder = RuleBuilder.create();
        
        var leftTable = builder.createQedTable(Seq.of(Tuple.of(RelType.fromString("INTEGER", true), false)));
        var rightTable = builder.createQedTable(Seq.of(Tuple.of(RelType.fromString("INTEGER", true), false)));
        builder.addTable(leftTable);
        builder.addTable(rightTable);
        
        var leftScan = builder.scan(leftTable.getName()).build();
        var rightScan = builder.scan(rightTable.getName()).build();
        
        // Build the "before" relation
        builder.push(leftScan);
        builder.push(rightScan);
        var joinPredicate = builder.equals(builder.field(2, 0, 0), builder.field(2, 1, 0));
        var semiJoin = builder.join(JoinRelType.SEMI, joinPredicate).build();
        builder.push(semiJoin);
        var filterPredicate = builder.call(builder.genericPredicateOp("filter", true), builder.field(0));
        var before = builder.filter(filterPredicate).build();
        
        // Build the expected "after" relation
        builder.push(leftScan);
        var leftFilterPredicate = builder.call(builder.genericPredicateOp("filter", true), builder.field(0));
        var filteredLeft = builder.filter(leftFilterPredicate).build();
        builder.push(filteredLeft);
        builder.push(rightScan);
        var afterJoinPredicate = builder.equals(builder.field(2, 0, 0), builder.field(2, 1, 0));
        var after = builder.join(JoinRelType.SEMI, afterJoinPredicate).build();
        
        var runner = CalciteTester.loadRule(org.qed.Generated.SemiJoinFilterTranspose.Config.DEFAULT.toRule());
        tester.verify(runner, before, after);
    }
    
    /**
     * Main method to run this test independently.
     */
    public static void main(String[] args) {
        System.out.println("Running SemiJoinFilterTranspose test...");
        runTest();
    }
}