package org.qed.Backends.Calcite.Tests;

import kala.collection.Seq;
import kala.tuple.Tuple;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.qed.Backends.Calcite.CalciteTester;
import org.qed.RelType;
import org.qed.RuleBuilder;

public class JoinConditionPushTest {

    public static void runTest() {
        var tester = new CalciteTester();
        var builder = RuleBuilder.create();

        var empTable = builder.createQedTable(Seq.of(
            Tuple.of(RelType.fromString("INTEGER", true), false),
            Tuple.of(RelType.fromString("VARCHAR", true), false),
            Tuple.of(RelType.fromString("INTEGER", true), false)
        ));
        builder.addTable(empTable);

        var deptTable = builder.createQedTable(Seq.of(
            Tuple.of(RelType.fromString("INTEGER", true), false),
            Tuple.of(RelType.fromString("VARCHAR", true), false)
        ));
        builder.addTable(deptTable);
        
        var empScan = builder.scan(empTable.getName()).build();
        var deptScan = builder.scan(deptTable.getName()).build();

        var before = builder
            .push(empScan)
            .push(deptScan)
            .join(JoinRelType.INNER, builder.call(SqlStdOperatorTable.AND,
                builder.call(builder.genericPredicateOp("joinCond", true),
                    builder.field(2, 0, 2),
                    builder.field(2, 1, 0)
                ),
                builder.call(builder.genericPredicateOp("leftCond", true),
                    builder.field(2, 0, 0)
                ),
                builder.call(builder.genericPredicateOp("rightCond", true),
                    builder.field(2, 1, 0)
                )
            ))
            .build();

        var after = builder
            .push(
                builder.push(empScan)
                    .filter(builder.call(builder.genericPredicateOp("leftCond", true),
                        builder.field(0)
                    ))
                    .build()
            )
            .push(
                builder.push(deptScan)
                    .filter(builder.call(builder.genericPredicateOp("rightCond", true),
                        builder.field(0)
                    ))
                    .build()
            )
            .join(JoinRelType.INNER,
                builder.call(builder.genericPredicateOp("joinCond", true),
                    builder.field(2, 0, 2),
                    builder.field(2, 1, 0)
                )
            )
            .build();
            
        var runner = CalciteTester.loadRule(org.qed.Backends.Calcite.Generated.JoinConditionPush.Config.DEFAULT.toRule(), 1);
        tester.verify(runner, before, after);
    }

    public static void main(String[] args) {
        System.out.println("Running JoinConditionPush test...");
        runTest();
    }
}