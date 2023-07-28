package org.cosette;

import io.github.cvc5.Kind;
import io.github.cvc5.Solver;
import io.github.cvc5.Term;
import kala.collection.Seq;
import kala.collection.Set;
import kala.collection.immutable.ImmutableSeq;
import kala.control.Result;
import kala.tuple.Tuple;
import kala.tuple.Tuple2;
import kala.tuple.Tuple3;
import org.apache.calcite.plan.Context;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptSchema;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.RelBuilder;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

public class RuleBuilder extends RelBuilder {

    private final AtomicInteger TABLE_ID_GENERATOR = new AtomicInteger();

    private final SchemaPlus root;

    protected RuleBuilder(@Nullable Context context, RelOptCluster cluster, RelOptSchema relOptSchema,
                          SchemaPlus schema) {
        super(context, cluster, relOptSchema);
        root = schema;
    }

    public static RuleBuilder create() {
        var emptySchema = Frameworks.createRootSchema(true);
        var config = Frameworks.newConfigBuilder().defaultSchema(emptySchema).build();
        return Frameworks.withPrepare(config,
                (cluster, relOptSchema, rootSchema, statement) -> new RuleBuilder(config.getContext(), cluster,
                        relOptSchema, emptySchema));
    }

    public RuleBuilder addTable(CosetteTable table) {
        root.add(table.getName(), table);
        return this;
    }

    /**
     * Create a cosette table given the column types and whether they are unique (i.e. can be key)
     *
     * @param schema the list of column types and they are unique
     * @return the table created from the given schema
     */
    public CosetteTable createCosetteTable(Seq<Tuple2<RelType, Boolean>> schema) {
        var identifier = "Table_" + TABLE_ID_GENERATOR.getAndIncrement();
        var cols = schema.mapIndexed(
                (idx, tuple) -> Tuple.of(identifier + "_Column_" + idx, tuple.component1(), tuple.component2()));
        return new CosetteTable(identifier,
                cols.map(tuple -> Map.entry(tuple.component1(), tuple.component2())).toImmutableMap(),
                Set.from(cols.filter(Tuple3::component3).map(tuple -> Set.of(tuple.component1()))), Set.of());
    }

    /**
     * Create and return the names of the created simple tables after registering them to the builder
     *
     * @param typeIds the absolute value represents type id, while the sign indicates the uniqueness
     * @return the names for the created tables
     */
    public Seq<String> sourceSimpleTables(Seq<Integer> typeIds) {
        return typeIds.map(id -> {
            var identifier = "Table_" + TABLE_ID_GENERATOR.getAndIncrement();
            var colName = identifier + "_Column";
            var colType = new RelType.VarType("Type_" + (id < 0 ? -id : id), true);
            var table = new CosetteTable(identifier, kala.collection.Map.of(colName, colType),
                    id < 0 ? Set.of(Set.of(colName)) : Set.of(), Set.empty());
            addTable(table);
            return table.getName();
        });
    }

    public Seq<RexNode> joinFields() {
        return Seq.from(fields(2, 0)).concat(fields(2, 1));
    }

    public SqlOperator genericPredicateOp(String name, boolean nullable) {
        return new CosetteFunction(name, new RelType.BaseType(SqlTypeName.BOOLEAN, nullable));
    }

    public SqlOperator genericProjectionOp(String name, RelType projection) {
        return new CosetteFunction(name, projection);
    }

    public static class CosetteFunction extends SqlFunction {

        private final RelType codomain;

        public CosetteFunction(String name, RelType returnType) {
            super(name, SqlKind.OTHER_FUNCTION, opBinding -> {
                var factory = opBinding.getTypeFactory();
                return factory.createTypeWithNullability(returnType, returnType.isNullable());
            }, null, null, SqlFunctionCategory.USER_DEFINED_FUNCTION);
            codomain = returnType;
        }

        public RelType getReturnType() {
            return codomain;
        }
    }

    public interface CosetteRule {

        RelNode getPattern();

        RelNode getTransformation();

        default ImmutableSeq<Term> deriveAdditionalConstraints(Solver solver, RexTranslator.Declarations declarations) {
            return ImmutableSeq.empty();
        }

        default Result<RexTranslator, String> match(RelNode target) {
            return RelMatcher.check(getPattern(), target).map(translator -> translator.addConstraints(
                    deriveAdditionalConstraints(translator.solver(), translator.declaredFunctions())));
        }

    }

    public static class JoinConditionPush implements CosetteRule {

        private final RelNode pattern;
        private final RelNode transform;
        private final String bothPredicate = "joinBoth";
        private final String leftPredicate = "joinLeft";
        private final String rightPredicate = "joinRight";


        public JoinConditionPush() {
            var builder = RuleBuilder.create();
            var tableNames = builder.sourceSimpleTables(Seq.of(1, 2));
            tableNames.forEach(builder::scan);
            var joinBoth = builder.genericPredicateOp(bothPredicate, true);
            var joinLeft = builder.genericPredicateOp(leftPredicate, true);
            var joinRight = builder.genericPredicateOp(rightPredicate, true);
            var joinCond = builder.and(builder.call(joinBoth, builder.joinFields()),
                    builder.call(joinLeft, builder.fields(2, 0)), builder.call(joinRight, builder.fields(2, 1)));
            builder.join(JoinRelType.INNER, joinCond);
            pattern = builder.build();
            builder.scan(tableNames.get(0)).filter(builder.call(joinLeft, builder.fields()));
            builder.scan(tableNames.get(1)).filter(builder.call(joinRight, builder.fields()));
            joinCond = builder.call(joinBoth, builder.joinFields());
            builder.join(JoinRelType.INNER, joinCond);
            transform = builder.build();
        }

        @Override
        public RelNode getPattern() {
            return pattern;
        }

        @Override
        public RelNode getTransformation() {
            return transform;
        }

        @Override
        public ImmutableSeq<Term> deriveAdditionalConstraints(Solver solver, RexTranslator.Declarations declarations) {
            var bp = declarations.store().get(bothPredicate);
            var lp = declarations.store().get(leftPredicate);
            var rp = declarations.store().get(rightPredicate);
            var lvs = lp.component2().mapIndexed((i, s) -> solver.declareSygusVar(leftPredicate + "-V" + i, s));
            var rvs = rp.component2().mapIndexed((i, s) -> solver.declareSygusVar(rightPredicate + "-V" + i, s));
            var bpc = solver.mkTerm(Kind.APPLY_UF,
                    bp.component1().map(Tuple2::component1).appendedAll(lvs).appendedAll(rvs).toArray(new Term[]{}));
            var lpc = solver.mkTerm(Kind.APPLY_UF,
                    lp.component1().map(Tuple2::component1).appendedAll(lvs).toArray(new Term[]{}));
            var rpc = solver.mkTerm(Kind.APPLY_UF,
                    rp.component1().map(Tuple2::component1).appendedAll(rvs).toArray(new Term[]{}));
            var acl = solver.mkTerm(Kind.IMPLIES, lpc,
                    solver.mkTerm(Kind.EXISTS, solver.mkTerm(Kind.VARIABLE_LIST, rvs.toArray(new Term[]{})),
                            solver.mkTerm(Kind.AND, bpc, rpc)));
            var acr = solver.mkTerm(Kind.IMPLIES, rpc,
                    solver.mkTerm(Kind.EXISTS, solver.mkTerm(Kind.VARIABLE_LIST, lvs.toArray(new Term[]{})),
                            solver.mkTerm(Kind.AND, bpc, lpc)));
            return ImmutableSeq.of(acl, acr);
        }
    }

}