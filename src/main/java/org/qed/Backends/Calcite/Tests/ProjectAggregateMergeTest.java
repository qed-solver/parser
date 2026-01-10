package org.qed.Backends.Calcite.Tests;

import kala.collection.Seq;
import kala.tuple.Tuple;
import org.qed.Backends.Calcite.CalciteTester;
import org.qed.RelType;
import org.qed.RuleBuilder;

public class ProjectAggregateMergeTest {

    public static void runTest() {
        var tester = new CalciteTester();
        var builder = RuleBuilder.create();
        
        // Create source table with 4 fields
        var sourceTable = builder.createQedTable(Seq.of(
            Tuple.of(RelType.fromString("INTEGER", true), false),  // field 0 - group key
            Tuple.of(RelType.fromString("INTEGER", true), false),  // field 1 - for sum/max
            Tuple.of(RelType.fromString("INTEGER", true), false),  // field 2 - for avg
            Tuple.of(RelType.fromString("INTEGER", true), false)   // field 3 - for count
        ));
        builder.addTable(sourceTable);
        
        var scan = builder.scan(sourceTable.getName()).build();
        
        // Before: Aggregate with 4 agg calls, then project uses only 2 of them
        // Aggregate: group by field(0), sum(field(1)), avg(field(2)), count(field(3)), max(field(1))
        // Result fields: [0=group, 1=sum, 2=avg, 3=count, 4=max]
        // Project: uses only field(0), field(1), field(3) - skips avg and max
        var before = builder
            .push(scan)
            .aggregate(
                builder.groupKey(builder.field(0)),
                builder.sum(false, "agg1", builder.field(1)),    // Will be used - becomes field 1
                builder.avg(builder.field(2)),                    // Will be unused - field 2
                builder.count(false, "agg3", builder.field(3)),  // Will be used - becomes field 3
                builder.max(builder.field(1))                     // Will be unused - field 4
            )
            .project(
                builder.field(0),  // group key
                builder.field(1),  // agg1 (sum)
                builder.field(3)   // agg3 (count) - skips field 2 (avg) and field 4 (max)
            )
            .build();
        
        // After: Aggregate with only 2 used agg calls, project adjusted
        // Aggregate: group by field(0), sum(field(1)), count(field(3))
        // Result fields: [0=group, 1=sum, 2=count]
        // Project: field(0), field(1), field(2)
        var after = builder
            .push(scan)
            .aggregate(
                builder.groupKey(builder.field(0)),
                builder.sum(false, "agg1", builder.field(1)),   // field 1
                builder.count(false, "agg3", builder.field(3))  // field 2 (was 3)
            )
            .project(
                builder.field(0),  // group key
                builder.field(1),  // agg1 (sum)
                builder.field(2)   // agg3 (count) - adjusted index
            )
            .build();
            
        var runner = CalciteTester.loadRule(
            org.qed.Backends.Calcite.Generated.ProjectAggregateMerge.Config.DEFAULT.toRule()
        );
        tester.verify(runner, before, after);
    }

    public static void main(String[] args) {
        System.out.println("Running ProjectAggregateMerge test...");
        runTest();
    }
}