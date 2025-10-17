package org.qed.Backends.Calcite.Tests;

import kala.collection.Seq;
import kala.tuple.Tuple;
import org.qed.Backends.Calcite.CalciteTester;
import org.qed.RelType;
import org.qed.RuleBuilder;

public class FilterProjectTransposeTest {

    public static void runTest() {
        var tester = new CalciteTester();
        var builder = RuleBuilder.create();
        var table = builder.createQedTable(Seq.of(
            Tuple.of(RelType.fromString("INTEGER", true), false),
            Tuple.of(RelType.fromString("INTEGER", true), false),
            Tuple.of(RelType.fromString("INTEGER", true), false)
        ));
        builder.addTable(table);
        
        var scan = builder.scan(table.getName()).build();

        var before = builder
            .push(scan)
            .project(
                builder.field(1),
                builder.field(2)
            )
            .filter(
                builder.and(
                    builder.greaterThan(builder.field(0), builder.literal(50000)),
                    builder.equals(builder.field(1), builder.literal(5))
                )
            )
            .build();

        var after = builder
            .push(scan)
            .filter(
                builder.and(
                    builder.greaterThan(builder.field(1), builder.literal(50000)),
                    builder.equals(builder.field(2), builder.literal(5))
                )
            )
            .project(
                builder.field(1),
                builder.field(2)
            )
            .build();
            
        var runner = CalciteTester.loadRule(org.qed.Backends.Calcite.Generated.FilterProjectTranspose.Config.DEFAULT.toRule());
        tester.verify(runner, before, after);
    }

    public static void main(String[] args) {
        System.out.println("Running FilterProjectTranspose test...");
        runTest();
    }
}