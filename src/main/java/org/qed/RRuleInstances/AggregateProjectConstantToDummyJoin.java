package org.qed.RRuleInstances;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.JoinRelType;
import org.qed.RelRN;
import org.qed.RexRN;
import org.qed.RRule;
import org.qed.RuleBuilder;
import org.qed.RelType;
import kala.collection.Seq;
import kala.tuple.Tuple;

/**
 * AggregateProjectConstantToDummyJoinRule: Replaces constant literals in GROUP BY 
 * with a dummy table join.
 * 
 * Pattern:
 * Aggregate(group=[constant_literal, regular_field])
 *   Project(constant_literal, regular_field)
 *     Scan
 * 
 * =>
 * 
 * Aggregate(group=[dummy.constant, regular_field])
 *   Project(dummy.constant, regular_field)
 *     Join(Scan, DummyValues(constant_literal))
 * 
 * This optimization can help with certain database engines that handle 
 * joins more efficiently than literal constants in GROUP BY clauses.
 */
public record AggregateProjectConstantToDummyJoin() implements RRule {
    
    // Base table for the pattern
    static final RelRN baseTable = new BaseEmployeeTable();
    
    @Override
    public RelRN before() {
        // Aggregate over project with constant literals
        var projectWithConstants = new ProjectWithConstantLiterals(baseTable);
        return new AggregateGroupingByConstants(projectWithConstants);
    }
    
    @Override
    public RelRN after() {
        // Optimized: join with dummy table containing constants
        var dummyTable = new DummyConstantsTable();
        var joinWithDummy = new JoinWithDummyTable(baseTable, dummyTable);
        var projectWithDummyFields = new ProjectWithDummyFields(joinWithDummy);
        return new AggregateGroupingByDummyFields(projectWithDummyFields);
    }

    /**
     * Base employee table
     */
    public static record BaseEmployeeTable() implements RelRN {
        @Override
        public RelNode semantics() {
            var builder = RuleBuilder.create();
            
            var table = builder.createQedTable(Seq.of(
                Tuple.of(RelType.fromString("INTEGER", true), false),   // emp_id
                Tuple.of(RelType.fromString("DECIMAL", true), false),   // salary
                Tuple.of(RelType.fromString("INTEGER", true), false)    // dept_id
            ));
            
            builder.addTable(table);
            return builder.scan(table.getName()).build();
        }
    }
    
    /**
     * Project with constant literals: SELECT emp_id, TRUE as active_flag, '2024' as year_label, salary
     */
    public static record ProjectWithConstantLiterals(RelRN input) implements RelRN {
        @Override
        public RelNode semantics() {
            var builder = RuleBuilder.create();
            builder.push(input.semantics());
            
            builder.project(
                builder.field(0),                                          // emp_id
                builder.alias(builder.literal(true), "active_flag"),       // constant: TRUE
                builder.alias(builder.literal("2024"), "year_label"),      // constant: '2024'
                builder.field(1)                                           // salary
            );
            
            return builder.build();
        }
    }
    
    /**
     * Aggregate grouping by constant literals: GROUP BY active_flag, year_label, emp_id
     */
    public static record AggregateGroupingByConstants(RelRN input) implements RelRN {
        @Override
        public RelNode semantics() {
            var builder = RuleBuilder.create();
            builder.push(input.semantics());
            
            // Group by the constant fields and emp_id
            var groupKey = builder.groupKey(
                builder.field(1),   // active_flag (constant)
                builder.field(2),   // year_label (constant)
                builder.field(0)    // emp_id (regular field)
            );
            
            // Aggregate: AVG(salary)
            var avgSalary = builder.avg(builder.field(3));
            
            builder.aggregate(groupKey, avgSalary);
            return builder.build();
        }
    }
    
    /**
     * Dummy table containing the constant values: VALUES (TRUE, '2024')
     */
    public static record DummyConstantsTable() implements RelRN {
        @Override
        public RelNode semantics() {
            var builder = RuleBuilder.create();
            
            // Create a values table with the constants
            // Using the correct values() method signature
            builder.values(
                new String[]{"active_flag", "year_label"},  // Column names
                true,                                       // TRUE constant value
                "2024"                                      // '2024' constant value
            );
            
            return builder.build();
        }
    }
    
    /**
     * Join base table with dummy constants table
     */
    public static record JoinWithDummyTable(RelRN baseTable, RelRN dummyTable) implements RelRN {
        @Override
        public RelNode semantics() {
            var builder = RuleBuilder.create();
            
            builder.push(baseTable.semantics());
            builder.push(dummyTable.semantics());
            
            // Cross join (INNER JOIN with TRUE condition)
            builder.join(JoinRelType.INNER, builder.literal(true));
            
            return builder.build();
        }
    }
    
    /**
     * Project using dummy fields instead of constants: SELECT emp_id, dummy.active_flag, dummy.year_label, salary
     */
    public static record ProjectWithDummyFields(RelRN input) implements RelRN {
        @Override
        public RelNode semantics() {
            var builder = RuleBuilder.create();
            builder.push(input.semantics());
            
            // After join: base table fields are 0,1,2 and dummy fields are 3,4
            builder.project(
                builder.field(0),   // emp_id (from base table)
                builder.field(3),   // active_flag (from dummy table)
                builder.field(4),   // year_label (from dummy table)
                builder.field(1)    // salary (from base table)
            );
            
            return builder.build();
        }
    }
    
    /**
     * Aggregate grouping by dummy fields: GROUP BY dummy.active_flag, dummy.year_label, emp_id
     */
    public static record AggregateGroupingByDummyFields(RelRN input) implements RelRN {
        @Override
        public RelNode semantics() {
            var builder = RuleBuilder.create();
            builder.push(input.semantics());
            
            // Group by the dummy fields and emp_id
            var groupKey = builder.groupKey(
                builder.field(1),   // active_flag (from dummy)
                builder.field(2),   // year_label (from dummy)
                builder.field(0)    // emp_id (regular field)
            );
            
            // Same aggregate: AVG(salary)
            var avgSalary = builder.avg(builder.field(3));
            
            builder.aggregate(groupKey, avgSalary);
            return builder.build();
        }
    }
}