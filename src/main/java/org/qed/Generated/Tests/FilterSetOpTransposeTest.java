package org.qed.Generated.Tests;

import kala.collection.Seq;
import kala.tuple.Tuple;
import org.qed.Generated.CalciteTester;
import org.qed.RelType;
import org.qed.RuleBuilder;

public class FilterSetOpTransposeTest {

    public static void runTest() {
        var tester = new CalciteTester();
        var builder = RuleBuilder.create();
        var table = builder.createQedTable(Seq.of(
            Tuple.of(RelType.fromString("INTEGER", true), false)
        ));
        builder.addTable(table);
        
        var scan1 = builder.scan(table.getName()).build();
        var scan2 = builder.scan(table.getName()).build();
        
        var union = builder.push(scan1).push(scan2).union(false).build();
        var before = builder.push(union).filter(builder.call(builder.genericPredicateOp("filter", true), builder.fields())).build();
        
        var filteredScan1 = builder.push(scan1).filter(builder.call(builder.genericPredicateOp("filter", true), builder.fields())).build();
        var filteredScan2 = builder.push(scan2).filter(builder.call(builder.genericPredicateOp("filter", true), builder.fields())).build();
        var after = builder.push(filteredScan1).push(filteredScan2).union(false).build();
        
        var runner = CalciteTester.loadRule(org.qed.Generated.FilterSetOpTranspose.Config.DEFAULT.toRule());
        tester.verify(runner, before, after);
    }

    public static void main(String[] args) {
        System.out.println("Running FilterSetOpTranspose test...");
        runTest();
    }
}