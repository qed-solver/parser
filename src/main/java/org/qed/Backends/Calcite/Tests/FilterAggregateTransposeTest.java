package org.qed.Backends.Calcite.Tests;

import kala.collection.Seq;
import kala.tuple.Tuple;
import org.qed.Backends.Calcite.CalciteTester;
import org.qed.RelType;
import org.qed.RuleBuilder;

public class FilterAggregateTransposeTest {

    public static void runTest() {
        var tester = new CalciteTester();
        var builder = RuleBuilder.create();
        
        var sourceTable = builder.createQedTable(Seq.of(
            Tuple.of(RelType.fromString("INTEGER", true), false),
            Tuple.of(RelType.fromString("VARCHAR", true), false),
            Tuple.of(RelType.fromString("VARCHAR", true), false),
            Tuple.of(RelType.fromString("DECIMAL", true), false)
        ));
        builder.addTable(sourceTable);
        
        var sourceScan = builder.scan(sourceTable.getName()).build();

        var before = builder
            .push(sourceScan)
            .aggregate(
                builder.groupKey(builder.field(1), builder.field(2)),  
                builder.sum(builder.field(3))         
            )
            .filter(builder.call(
                builder.genericPredicateOp("pred", true),
                builder.field(0), builder.field(1)
            ))
            .build();
        var after = builder
            .push(sourceScan)
            .filter(builder.call(
                builder.genericPredicateOp("pred", true),
                builder.field(1), builder.field(2)
            ))
            .aggregate(
                builder.groupKey(builder.field(1), builder.field(2)),  
                builder.sum(builder.field(3))
            )
            .build();
            
        var runner = CalciteTester.loadRules(
            org.qed.Backends.Calcite.Generated.FilterAggregateTranspose.Config.DEFAULT.toRule()
        );
        tester.verify(runner, before, after);
    }

    public static void main(String[] args) {
        System.out.println("Running FilterAggregateTranspose test...");
        runTest();
    }
}