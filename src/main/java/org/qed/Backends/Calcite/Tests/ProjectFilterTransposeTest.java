package org.qed.Backends.Calcite.Tests;

import kala.collection.Seq;
import kala.tuple.Tuple;
import org.qed.Backends.Calcite.CalciteTester;
import org.qed.RelType;
import org.qed.RuleBuilder;

public class ProjectFilterTransposeTest {
    public static void runTest() {
        var tester = new CalciteTester();
        var builder = RuleBuilder.create();
        
        // Create table with 3 columns: id, salary, dept_id
        var table = builder.createQedTable(Seq.of(
            Tuple.of(RelType.fromString("INTEGER", true), false),  // id (col 0)
            Tuple.of(RelType.fromString("INTEGER", true), false),  // salary (col 1)
            Tuple.of(RelType.fromString("INTEGER", true), false)   // dept_id (col 2)
        ));
        builder.addTable(table);
        
        var scan = builder.scan(table.getName()).build();

        var before = builder
            .push(scan)
            .filter(
                builder.and(
                    builder.greaterThan(builder.field(1), builder.literal(50000)), // table salary > 50000
                    builder.equals(builder.field(2), builder.literal(5))           // table dept_id = 5
                )
            )
            .project(
                builder.field(1),  // salary
                builder.field(2)   // dept_id
            )
            .build();

        var after = builder
            .push(scan)
            .project(
                builder.field(1),  // salary -> position 0 in projection
                builder.field(2)   // dept_id -> position 1 in projection
            )
            .filter(
                builder.and(
                    builder.greaterThan(builder.field(0), builder.literal(50000)), // projected salary > 50000
                    builder.equals(builder.field(1), builder.literal(5))           // projected dept_id = 5
                )
            )
            .build();
            
        var runner = CalciteTester.loadRule(org.qed.Backends.Calcite.Generated.ProjectFilterTranspose.Config.DEFAULT.toRule());
        tester.verify(runner, before, after);
    }

    public static void main(String[] args) {
        System.out.println("Running ProjectFilterTranspose test...");
        runTest();
    }
}
