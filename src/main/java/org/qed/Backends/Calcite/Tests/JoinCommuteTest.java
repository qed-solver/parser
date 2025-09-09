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
        
        // Create EMP table (8 columns like the real Calcite test)
        var empTable = builder.createQedTable(Seq.of(
            Tuple.of(RelType.fromString("INTEGER", true), false),  // EMPNO
            Tuple.of(RelType.fromString("VARCHAR", true), false),  // ENAME  
            Tuple.of(RelType.fromString("VARCHAR", true), false),  // JOB
            Tuple.of(RelType.fromString("INTEGER", true), false),  // MGR
            Tuple.of(RelType.fromString("DATE", true), false),     // HIREDATE
            Tuple.of(RelType.fromString("DECIMAL", true), false),  // SAL
            Tuple.of(RelType.fromString("DECIMAL", true), false),  // COMM
            Tuple.of(RelType.fromString("INTEGER", true), false)   // DEPTNO
        ));
        builder.addTable(empTable);
        
        // Create DEPT table (3 columns like the real Calcite test)
        var deptTable = builder.createQedTable(Seq.of(
            Tuple.of(RelType.fromString("INTEGER", true), false),  // DEPTNO
            Tuple.of(RelType.fromString("VARCHAR", true), false),  // DNAME
            Tuple.of(RelType.fromString("VARCHAR", true), false)   // LOC
        ));
        builder.addTable(deptTable);
        
        // Build the "before" pattern: EMP JOIN DEPT ON EMP.DEPTNO = DEPT.DEPTNO
        var empScan = builder.scan(empTable.getName()).build();
        var deptScan = builder.scan(deptTable.getName()).build();
        
        var before = builder
            .push(empScan)
            .push(deptScan)
            .join(JoinRelType.INNER, builder.call(builder.genericPredicateOp("equals", true), 
                  builder.field(2, 0, 7), // EMP.DEPTNO (8th column, index 7)
                  builder.field(2, 1, 0)  // DEPT.DEPTNO (1st column, index 0)
            ))
            .build();
            
        // Build the "after" pattern: DEPT JOIN EMP ON DEPT.DEPTNO = EMP.DEPTNO
        // followed by projection to restore original column order
        var after = builder
            .push(deptScan)
            .push(empScan)
            .join(JoinRelType.INNER, builder.call(builder.genericPredicateOp("equals", true),
                  builder.field(2, 1, 7), // EMP.DEPTNO (now at input 1, field 7)
                  builder.field(2, 0, 0)  // DEPT.DEPTNO (now at input 0, field 0)
            ))
            .project(
                // EMP columns first (now at positions 3-10 in the swapped join)
                builder.field(3),  // EMPNO
                builder.field(4),  // ENAME
                builder.field(5),  // JOB
                builder.field(6),  // MGR
                builder.field(7),  // HIREDATE
                builder.field(8),  // SAL
                builder.field(9),  // COMM
                builder.field(10), // DEPTNO
                // DEPT columns second (now at positions 0-2 in the swapped join)
                builder.field(0),  // DEPTNO0
                builder.field(1),  // DNAME
                builder.field(2)   // LOC
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