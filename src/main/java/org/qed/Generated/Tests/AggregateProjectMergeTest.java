package org.qed.Generated.Tests;

import kala.collection.Seq;
import kala.tuple.Tuple;
import org.qed.Generated.CalciteTester;
import org.qed.RelType;
import org.qed.RuleBuilder;

public class AggregateProjectMergeTest {

    public static void runTest() {
        var tester = new CalciteTester();
        var builder = RuleBuilder.create();
        
        // Create EMP table matching the Apache Calcite test structure
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
        
        // Build the "before" pattern matching the Apache Calcite test:
        // select x, sum(z), y from (
        //   select deptno as x, empno as y, sal as z, sal * 2 as zz
        //   from emp)
        // group by x, y
        var before = builder
            .push(empScan)
            // Inner project: select deptno as x, empno as y, sal as z
            // This corresponds to: X=[$7], Y=[$0], Z=[$5]
            .project(
                builder.field(7),  // DEPTNO -> X (position 0 in projection)
                builder.field(0),  // EMPNO -> Y (position 1 in projection)  
                builder.field(5)   // SAL -> Z (position 2 in projection)
                // Note: sal * 2 as zz is omitted since it's not used by the aggregate
            )
            // Aggregate: group by x, y and sum(z)
            // This corresponds to: group=[{0, 1}], EXPR$1=[SUM($2)]
            .aggregate(
                builder.groupKey(builder.field(0), builder.field(1)), // Group by X, Y
                builder.sum(builder.field(2))  // SUM(Z)
            )
            // Outer project to reorder columns: X, EXPR$1, Y
            // This corresponds to: LogicalProject(X=[$0], EXPR$1=[$2], Y=[$1])
            .project(
                builder.field(0),  // X
                builder.field(2),  // EXPR$1 (SUM result)
                builder.field(1)   // Y
            )
            .build();
            
        // Build the "after" pattern (optimized):
        // The inner project is merged with the aggregate
        var after = builder
            .push(empScan)
            // Direct aggregate on base table: group=[{0, 7}], EXPR$1=[SUM($5)]
            // This means: GROUP BY EMPNO (0), DEPTNO (7), SUM(SAL) (5)
            .aggregate(
                builder.groupKey(builder.field(0), builder.field(7)), // Group by EMPNO, DEPTNO
                builder.sum(builder.field(5))  // SUM(SAL)
            )
            // Intermediate project to match field names: DEPTNO=[$1], EMPNO=[$0], EXPR$1=[$2]
            .project(
                builder.field(1),  // DEPTNO (was at position 1 in aggregate result)
                builder.field(0),  // EMPNO (was at position 0 in aggregate result)
                builder.field(2)   // EXPR$1 (SUM result at position 2)
            )
            // Final project to match original output order: X=[$0], EXPR$1=[$2], Y=[$1]
            .project(
                builder.field(0),  // X (DEPTNO)
                builder.field(2),  // EXPR$1 (SUM result)
                builder.field(1)   // Y (EMPNO)
            )
            .build();
            
        var runner = CalciteTester.loadRules(org.qed.Generated.AggregateProjectMerge.Config.DEFAULT.toRule(), org.qed.Generated.ProjectMerge.Config.DEFAULT.toRule());
        tester.verify(runner, before, after);
    }

    public static void main(String[] args) {
        System.out.println("Running AggregateProjectMerge comprehensive test...");
        runTest();
    }
}