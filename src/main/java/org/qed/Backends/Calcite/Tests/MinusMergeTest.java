package org.qed.Backends.Calcite.Tests;

import kala.collection.Seq;
import kala.tuple.Tuple;
import org.qed.Backends.Calcite.CalciteTester;
import org.qed.RelType;
import org.qed.RuleBuilder;

/**
 * Test for the MinusMerge rule.
 */
public class MinusMergeTest {

    public static void runTest() {
        var tester = new CalciteTester();
        var builder = RuleBuilder.create();
        var table = builder.createQedTable(Seq.of(
            Tuple.of(RelType.fromString("INTEGER", true), false)
        ));
        builder.addTable(table);
        
        var scan1 = builder.scan(table.getName()).build();
        var scan2 = builder.scan(table.getName()).build();
        var scan3 = builder.scan(table.getName()).build();
        
        // (A − B) − C
        var before = builder.push(scan1).push(scan2).minus(false, 2).push(scan3).minus(false, 2).build();

        // A − (B ∪ C)
        var union = builder.push(scan2).push(scan3).union(false).build();
        var after = builder.push(scan1).push(union).minus(false, 2).build();
        
        var runner = CalciteTester.loadRule(org.qed.Backends.Calcite.Generated.MinusMerge.Config.DEFAULT.toRule());
        tester.verify(runner, before, after);
    }
    
    /**
     * Main method to run this test independently.
     */
    public static void main(String[] args) {
        System.out.println("Running MinusMerge test...");
        runTest();
    }
}