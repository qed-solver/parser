package org.qed.Generated.Tests;

import kala.collection.Seq;
import kala.tuple.Tuple;
import org.qed.Generated.CalciteTester;
import org.qed.RelType;
import org.qed.RuleBuilder;

public class IntersectMergeTest {

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
        
        var firstIntersect = builder.push(scan1).push(scan2).intersect(false).build();
        var before = builder.push(firstIntersect).push(scan3).intersect(false).build();
        
        var after = builder.push(scan1).push(scan2).push(scan3).intersect(false, 3).build();
        
        var runner = CalciteTester.loadRule(org.qed.Generated.IntersectMerge.Config.DEFAULT.toRule());
        tester.verify(runner, before, after);
    }

    public static void main(String[] args) {
        System.out.println("Running IntersectMerge test...");
        runTest();
    }
}