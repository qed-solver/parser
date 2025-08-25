package org.qed.Generated.Tests;

import kala.collection.Seq;
import kala.tuple.Tuple;
import org.qed.Generated.CalciteTester;
import org.qed.RelType;
import org.qed.RuleBuilder;

public class AggregateExtractProjectTest {

    public static void runTest() {
        var tester = new CalciteTester();
        var builder = RuleBuilder.create();
        
        var empTable = builder.createQedTable(Seq.of(
            Tuple.of(RelType.fromString("INTEGER", true), false),  // EMPNO (index 0)
            Tuple.of(RelType.fromString("VARCHAR", true), false),  // ENAME (index 1)
            Tuple.of(RelType.fromString("VARCHAR", true), false),  // JOB (index 2)
            Tuple.of(RelType.fromString("INTEGER", true), false),  // MGR (index 3)
            Tuple.of(RelType.fromString("DATE", true), false),     // HIREDATE (index 4)
            Tuple.of(RelType.fromString("DECIMAL", true), false),  // SAL (index 5)
            Tuple.of(RelType.fromString("DECIMAL", true), false),  // COMM (index 6)
            Tuple.of(RelType.fromString("INTEGER", true), false)   // DEPTNO (index 7)
        ));
        builder.addTable(empTable);
        
        var empScan = builder.scan(empTable.getName()).build();

        var before = builder
            .push(empScan)
            .aggregate(
                builder.groupKey(builder.field(0), builder.field(7)), // Group by EMPNO, DEPTNO
                builder.sum(builder.field(5))  // SUM(SAL)
            )
            .build();

        var after = builder
            .push(empScan)
            .project(
                builder.field(0),  // DEPTNO -> X (position 0 in projection)
                builder.field(5),  // EMPNO -> Y (position 1 in projection)  
                builder.field(7)   // SAL -> Z (position 2 in projection)
            )
            .aggregate(
                builder.groupKey(builder.field(0), builder.field(2)), // Group by X, Y
                builder.sum(builder.field(1))  // SUM(Z)
            )
            .build();
            
        var runner = CalciteTester.loadRule(org.qed.Generated.AggregateExtractProject.Config.DEFAULT.toRule());
        tester.verify(runner, before, after);
    }

    public static void main(String[] args) {
        System.out.println("Running AggregateExtractProject comprehensive test...");
        runTest();
    }
}