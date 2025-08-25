package org.qed.Generated.RRuleInstances;

import org.apache.calcite.rel.RelNode;
import org.qed.RelRN;
import org.qed.RRule;
import org.qed.RuleBuilder;
import org.qed.RelType;
import kala.collection.Seq;
import kala.tuple.Tuple;

/**
 * UnionPullUpConstantsRule: Pulls up constant expressions through Union operators
 * 
 * Pattern:
 * Union(
 *   Project(col1, constant_value, col3),
 *   Project(col1, constant_value, col3)
 * )
 * =>
 * Project(col1, constant_value, col3,
 *   Union(
 *     Project(col1, col3),
 *     Project(col1, col3)
 *   )
 * )
 * 
 * This optimization reduces the Union to only non-constant columns,
 * then adds back the constants in a top-level projection.
 */
public record UnionPullUpConstants() implements RRule {
    
    // Base tables for demonstrating the pattern
    static final RelRN leftTable = new LeftTableWithConstants();
    static final RelRN rightTable = new RightTableWithConstants();
    
    @Override
    public RelRN before() {
        // Union of two projections that both have constants
        var leftProjection = new LeftProjectionWithConstants(leftTable);
        var rightProjection = new RightProjectionWithConstants(rightTable);
        return new UnionWithConstantColumns(leftProjection, rightProjection);
    }
    
    @Override
    public RelRN after() {
        // Optimized: constants pulled up, union reduced to non-constant columns
        var leftProjectionReduced = new LeftProjectionNonConstants(leftTable);
        var rightProjectionReduced = new RightProjectionNonConstants(rightTable);
        var reducedUnion = new UnionReducedColumns(leftProjectionReduced, rightProjectionReduced);
        return new TopProjectionWithConstants(reducedUnion);
    }

    /**
     * Left source table
     */
    public static record LeftTableWithConstants() implements RelRN {
        @Override
        public RelNode semantics() {
            var builder = RuleBuilder.create();
            
            var table = builder.createQedTable(Seq.of(
                Tuple.of(RelType.fromString("INTEGER", true), false),   // emp_id
                Tuple.of(RelType.fromString("VARCHAR", true), false),   // emp_name
                Tuple.of(RelType.fromString("INTEGER", true), false)    // dept_id
            ));
            
            builder.addTable(table);
            return builder.scan(table.getName()).build();
        }
    }
    
    /**
     * Right source table
     */
    public static record RightTableWithConstants() implements RelRN {
        @Override
        public RelNode semantics() {
            var builder = RuleBuilder.create();
            
            var table = builder.createQedTable(Seq.of(
                Tuple.of(RelType.fromString("INTEGER", true), false),   // emp_id
                Tuple.of(RelType.fromString("VARCHAR", true), false),   // emp_name
                Tuple.of(RelType.fromString("INTEGER", true), false)    // dept_id
            ));
            
            builder.addTable(table);
            return builder.scan(table.getName()).build();
        }
    }
    
    /**
     * Left projection with constants: SELECT emp_id, 'ACTIVE' as status, dept_id
     */
    public static record LeftProjectionWithConstants(RelRN input) implements RelRN {
        @Override
        public RelNode semantics() {
            var builder = RuleBuilder.create();
            builder.push(input.semantics());
            
            builder.project(
                builder.field(0),                                      // emp_id
                builder.alias(builder.literal("ACTIVE"), "status"),    // constant: 'ACTIVE'
                builder.field(2)                                       // dept_id
            );
            
            return builder.build();
        }
    }
    
    /**
     * Right projection with SAME constants: SELECT emp_id, 'ACTIVE' as status, dept_id
     */
    public static record RightProjectionWithConstants(RelRN input) implements RelRN {
        @Override
        public RelNode semantics() {
            var builder = RuleBuilder.create();
            builder.push(input.semantics());
            
            builder.project(
                builder.field(0),                                      // emp_id
                builder.alias(builder.literal("ACTIVE"), "status"),    // same constant: 'ACTIVE'
                builder.field(2)                                       // dept_id
            );
            
            return builder.build();
        }
    }
    
    /**
     * Union with constant columns (before optimization)
     */
    public static record UnionWithConstantColumns(RelRN left, RelRN right) implements RelRN {
        @Override
        public RelNode semantics() {
            var builder = RuleBuilder.create();
            
            builder.push(left.semantics());
            builder.push(right.semantics());
            
            builder.union(true, 2);  // UNION ALL
            
            return builder.build();
        }
    }
    
    /**
     * Left projection with constants removed: SELECT emp_id, dept_id
     */
    public static record LeftProjectionNonConstants(RelRN input) implements RelRN {
        @Override
        public RelNode semantics() {
            var builder = RuleBuilder.create();
            builder.push(input.semantics());
            
            // Project only non-constant columns
            builder.project(
                builder.field(0),   // emp_id
                builder.field(2)    // dept_id
                // status constant removed
            );
            
            return builder.build();
        }
    }
    
    /**
     * Right projection with constants removed: SELECT emp_id, dept_id
     */
    public static record RightProjectionNonConstants(RelRN input) implements RelRN {
        @Override
        public RelNode semantics() {
            var builder = RuleBuilder.create();
            builder.push(input.semantics());
            
            // Project only non-constant columns
            builder.project(
                builder.field(0),   // emp_id
                builder.field(2)    // dept_id
                // status constant removed
            );
            
            return builder.build();
        }
    }
    
    /**
     * Union of reduced columns (constants removed)
     */
    public static record UnionReducedColumns(RelRN left, RelRN right) implements RelRN {
        @Override
        public RelNode semantics() {
            var builder = RuleBuilder.create();
            
            builder.push(left.semantics());
            builder.push(right.semantics());
            
            builder.union(true, 2);  // UNION ALL on reduced columns
            
            return builder.build();
        }
    }
    
    /**
     * Top projection that adds back the constants: SELECT emp_id, 'ACTIVE' as status, dept_id
     */
    public static record TopProjectionWithConstants(RelRN input) implements RelRN {
        @Override
        public RelNode semantics() {
            var builder = RuleBuilder.create();
            builder.push(input.semantics());
            
            // Add back the constant in the final projection
            builder.project(
                builder.field(0),                                      // emp_id (from union)
                builder.alias(builder.literal("ACTIVE"), "status"),    // constant added back
                builder.field(1)                                       // dept_id (from union)
            );
            
            return builder.build();
        }
    }
}