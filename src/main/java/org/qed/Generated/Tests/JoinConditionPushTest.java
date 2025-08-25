package org.qed.Generated.Tests;

import kala.collection.Seq;
import kala.tuple.Tuple;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.qed.Generated.CalciteTester;
import org.qed.RelType;
import org.qed.RuleBuilder;

public class JoinConditionPushTest {

    public static void runTest() {
        var tester = new CalciteTester();
        var builder = RuleBuilder.create();
        
        // Create EMP table (simplified version with key fields)
        var empTable = builder.createQedTable(Seq.of(
            Tuple.of(RelType.fromString("INTEGER", true), false),  // EMPNO
            Tuple.of(RelType.fromString("VARCHAR", true), false),  // ENAME  
            Tuple.of(RelType.fromString("INTEGER", true), false)   // DEPTNO
        ));
        builder.addTable(empTable);
        
        // Create DEPT table
        var deptTable = builder.createQedTable(Seq.of(
            Tuple.of(RelType.fromString("INTEGER", true), false),  // DEPTNO
            Tuple.of(RelType.fromString("VARCHAR", true), false)   // DNAME
        ));
        builder.addTable(deptTable);
        
        var empScan = builder.scan(empTable.getName()).build();
        var deptScan = builder.scan(deptTable.getName()).build();
        
        // Build the "before" pattern: 
        // JOIN with complex condition: joinCond AND leftCond AND rightCond
        var before = builder
            .push(empScan)
            .push(deptScan)
            .join(JoinRelType.INNER, builder.call(SqlStdOperatorTable.AND,
                // Cross-table join condition: emp.deptno = dept.deptno
                builder.call(builder.genericPredicateOp("joinCond", true),
                    builder.field(2, 0, 2), // EMP.DEPTNO (field 2 of left input)
                    builder.field(2, 1, 0)  // DEPT.DEPTNO (field 0 of right input)
                ),
                // Left-only condition: emp.empno condition
                builder.call(builder.genericPredicateOp("leftCond", true),
                    builder.field(2, 0, 0)  // EMP.EMPNO (field 0 of left input)
                ),
                // Right-only condition: dept.deptno condition  
                builder.call(builder.genericPredicateOp("rightCond", true),
                    builder.field(2, 1, 0)  // DEPT.DEPTNO (field 0 of right input)
                )
            ))
            .build();
            
        // Build the "after" pattern:
        // JOIN with only cross-table condition, filters pushed down
        var after = builder
            .push(
                // Left side: filtered EMP table
                builder.push(empScan)
                    .filter(builder.call(builder.genericPredicateOp("leftCond", true),
                        builder.field(0)  // EMP.EMPNO (field 0 in filter context)
                    ))
                    .build()
            )
            .push(
                // Right side: filtered DEPT table
                builder.push(deptScan)
                    .filter(builder.call(builder.genericPredicateOp("rightCond", true),
                        builder.field(0)  // DEPT.DEPTNO (field 0 in filter context)
                    ))
                    .build()
            )
            .join(JoinRelType.INNER, 
                // Only the cross-table join condition remains
                builder.call(builder.genericPredicateOp("joinCond", true),
                    builder.field(2, 0, 2), // EMP.DEPTNO (field 2 of left input)
                    builder.field(2, 1, 0)  // DEPT.DEPTNO (field 0 of right input)
                )
            )
            .build();
            
        var runner = CalciteTester.loadRule(org.qed.Generated.JoinConditionPush.Config.DEFAULT.toRule(), 1);
        tester.verify(runner, before, after);
    }

    public static void main(String[] args) {
        System.out.println("Running JoinConditionPush test...");
        runTest();
    }
}