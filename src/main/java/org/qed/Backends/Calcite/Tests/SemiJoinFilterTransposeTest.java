package org.qed.Backends.Calcite.Tests;

import kala.collection.Seq;
import kala.tuple.Tuple;
import org.apache.calcite.rel.core.JoinRelType;
import org.qed.Backends.Calcite.CalciteTester;
import org.qed.RelType;
import org.qed.RuleBuilder;

public class SemiJoinFilterTransposeTest {

    public static void runTest() {
        var tester = new CalciteTester();
        var builder = RuleBuilder.create();
        
        var leftTable = builder.createQedTable(Seq.of(Tuple.of(RelType.fromString("INTEGER", true), false)));
        var rightTable = builder.createQedTable(Seq.of(Tuple.of(RelType.fromString("INTEGER", true), false)));
        builder.addTable(leftTable);
        builder.addTable(rightTable);
        
        var leftScan = builder.scan(leftTable.getName()).build();
        var rightScan = builder.scan(rightTable.getName()).build();

        builder.push(leftScan);
        builder.push(rightScan);
        var joinPredicate = builder.equals(builder.field(2, 0, 0), builder.field(2, 1, 0));
        var semiJoin = builder.join(JoinRelType.SEMI, joinPredicate).build();
        builder.push(semiJoin);
        var filterPredicate = builder.call(builder.genericPredicateOp("filter", true), builder.field(0));
        var before = builder.filter(filterPredicate).build();

        builder.push(leftScan);
        var leftFilterPredicate = builder.call(builder.genericPredicateOp("filter", true), builder.field(0));
        var filteredLeft = builder.filter(leftFilterPredicate).build();
        builder.push(filteredLeft);
        builder.push(rightScan);
        var afterJoinPredicate = builder.equals(builder.field(2, 0, 0), builder.field(2, 1, 0));
        var after = builder.join(JoinRelType.SEMI, afterJoinPredicate).build();
        
        var runner = CalciteTester.loadRule(org.qed.Backends.Calcite.Generated.SemiJoinFilterTranspose.Config.DEFAULT.toRule());
        tester.verify(runner, before, after);
    }

    public static void main(String[] args) {
        System.out.println("Running SemiJoinFilterTranspose test...");
        runTest();
    }
}