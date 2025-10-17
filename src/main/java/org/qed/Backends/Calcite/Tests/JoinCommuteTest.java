package org.qed.Backends.Calcite.Tests;

import kala.collection.Seq;
import kala.tuple.Tuple;
import org.apache.calcite.rel.core.JoinRelType;
import org.qed.Backends.Calcite.CalciteTester;
import org.qed.RelType;
import org.qed.RuleBuilder;

public class JoinCommuteTest {

    public static void runTest() {
        var tester = new CalciteTester();
        var builder = RuleBuilder.create();

        var empTable = builder.createQedTable(Seq.of(
            Tuple.of(RelType.fromString("INTEGER", true), false),
            Tuple.of(RelType.fromString("VARCHAR", true), false),
            Tuple.of(RelType.fromString("VARCHAR", true), false),
            Tuple.of(RelType.fromString("INTEGER", true), false),
            Tuple.of(RelType.fromString("DATE", true), false),
            Tuple.of(RelType.fromString("DECIMAL", true), false),
            Tuple.of(RelType.fromString("DECIMAL", true), false),
            Tuple.of(RelType.fromString("INTEGER", true), false)
        ));
        builder.addTable(empTable);

        var deptTable = builder.createQedTable(Seq.of(
            Tuple.of(RelType.fromString("INTEGER", true), false),
            Tuple.of(RelType.fromString("VARCHAR", true), false),
            Tuple.of(RelType.fromString("VARCHAR", true), false)
        ));
        builder.addTable(deptTable);

        var empScan = builder.scan(empTable.getName()).build();
        var deptScan = builder.scan(deptTable.getName()).build();
        
        var before = builder
            .push(empScan)
            .push(deptScan)
            .join(JoinRelType.INNER, builder.call(builder.genericPredicateOp("equals", true), 
                  builder.field(2, 0, 7),
                  builder.field(2, 1, 0)
            ))
            .build();

        var after = builder
            .push(deptScan)
            .push(empScan)
            .join(JoinRelType.INNER, builder.call(builder.genericPredicateOp("equals", true),
                  builder.field(2, 1, 7),
                  builder.field(2, 0, 0)
            ))
            .project(
                builder.field(3),
                builder.field(4),
                builder.field(5),
                builder.field(6),
                builder.field(7),
                builder.field(8),
                builder.field(9),
                builder.field(10),
                builder.field(0),
                builder.field(1),
                builder.field(2)
            )
            .build();
            
        var runner = CalciteTester.loadRule(org.qed.Backends.Calcite.Generated.JoinCommute.Config.DEFAULT.toRule(), 1);
        tester.verify(runner, before, after);
    }

    public static void main(String[] args) {
        System.out.println("Running JoinCommute test...");
        runTest();
    }
}