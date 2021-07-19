package org.cosette;

import com.google.common.collect.ImmutableList;
import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.config.CalciteConnectionConfig;
import org.apache.calcite.config.CalciteConnectionConfigImpl;
import org.apache.calcite.config.CalciteConnectionProperty;
import org.apache.calcite.config.CalciteSystemProperty;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.plan.*;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexExecutor;
import org.apache.calcite.runtime.Hook;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.SqlInsert;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperatorTable;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.util.SqlOperatorTables;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorImpl;
import org.apache.calcite.sql2rel.RelDecorrelator;
import org.apache.calcite.sql2rel.SqlRexConvertletTable;
import org.apache.calcite.sql2rel.SqlToRelConverter;
import org.apache.calcite.tools.*;
import org.apache.calcite.util.SourceStringReader;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.io.Reader;
import java.util.List;

import static java.util.Objects.requireNonNull;

/**
 * A copy of the PlannerImpl that disables all rewrite rules.
 */

public class RawPlanner implements RelOptTable.ViewExpander {
    private final SqlOperatorTable operatorTable;
    private final ImmutableList<Program> programs;
    private final @Nullable RelOptCostFactory costFactory;
    private final Context context;
    private final CalciteConnectionConfig connectionConfig;

    /** Holds the trait definitions to be registered with planner. May be null. */
    private final @Nullable ImmutableList<RelTraitDef> traitDefs;

    private final SqlParser.Config parserConfig;
    private final SqlValidator.Config sqlValidatorConfig;
    private final SqlToRelConverter.Config sqlToRelConverterConfig;
    private final SqlRexConvertletTable convertletTable;

    // set in STATE_1_RESET
    @SuppressWarnings("unused")
    private boolean open;

    // set in STATE_2_READY
    private @Nullable SchemaPlus defaultSchema;
    private @Nullable JavaTypeFactory typeFactory;
    private @Nullable RelOptPlanner planner;
    private @Nullable RexExecutor executor;

    // set in STATE_4_VALIDATE
    private @Nullable SqlValidator validator;
    private @Nullable SqlNode validatedSqlNode;

    public RawPlanner(SchemaPlus schema) {
        SqlToRelConverter.Config converterConfig = SqlToRelConverter.config()
                .withRelBuilderConfigTransform(c -> c.withPushJoinCondition(false)
                        .withSimplify(false)
                        .withSimplifyValues(false)
                        .withBloat(-1)
                        .withDedupAggregateCalls(false)
                        .withPruneInputOfAggregate(false))
                .withDecorrelationEnabled(false)
                .withExpand(false)
                .withTrimUnusedFields(false);
        FrameworkConfig config = Frameworks.newConfigBuilder()
                .defaultSchema(schema)
                .sqlToRelConverterConfig(converterConfig)
                .build();
        this.costFactory = config.getCostFactory();
        this.defaultSchema = config.getDefaultSchema();
        this.operatorTable = config.getOperatorTable();
        this.programs = config.getPrograms();
        this.parserConfig = config.getParserConfig();
        this.sqlValidatorConfig = config.getSqlValidatorConfig();
        this.sqlToRelConverterConfig = config.getSqlToRelConverterConfig();
        this.traitDefs = config.getTraitDefs();
        this.convertletTable = config.getConvertletTable();
        this.executor = config.getExecutor();
        this.context = config.getContext();
        this.connectionConfig = connConfig(context, parserConfig);
    }

    private static CalciteConnectionConfig connConfig(Context context,
                                                      SqlParser.Config parserConfig) {
        CalciteConnectionConfigImpl config =
                context.maybeUnwrap(CalciteConnectionConfigImpl.class)
                        .orElse(CalciteConnectionConfig.DEFAULT);
        if (!config.isSet(CalciteConnectionProperty.CASE_SENSITIVE)) {
            config = config.set(CalciteConnectionProperty.CASE_SENSITIVE,
                    String.valueOf(parserConfig.caseSensitive()));
        }
        if (!config.isSet(CalciteConnectionProperty.CONFORMANCE)) {
            config = config.set(CalciteConnectionProperty.CONFORMANCE,
                    String.valueOf(parserConfig.conformance()));
        }
        return config;
    }

    private void ready() {
        RelDataTypeSystem typeSystem =
                connectionConfig.typeSystem(RelDataTypeSystem.class,
                        RelDataTypeSystem.DEFAULT);
        typeFactory = new JavaTypeFactoryImpl(typeSystem);
        RelOptPlanner planner = this.planner = new VolcanoPlanner(costFactory, context);
        RelOptUtil.registerDefaultRules(planner,
                connectionConfig.materializationsEnabled(),
                Hook.ENABLE_BINDABLE.get(false));
        planner.setExecutor(executor);

        // If user specify own traitDef, instead of default default trait,
        // register the trait def specified in traitDefs.
        if (this.traitDefs == null) {
            planner.addRelTraitDef(ConventionTraitDef.INSTANCE);
            if (CalciteSystemProperty.ENABLE_COLLATION_TRAIT.value()) {
                planner.addRelTraitDef(RelCollationTraitDef.INSTANCE);
            }
        } else {
            for (RelTraitDef def : this.traitDefs) {
                planner.addRelTraitDef(def);
            }
        }
    }

    public SqlNode parse(String sql) throws SqlParseException, ValidationException {
        ready();
        Reader reader = new SourceStringReader(sql);
        SqlParser parser = SqlParser.create(reader, parserConfig);
        SqlNode sqlNode =  parser.parseStmt();
        this.validator = createSqlValidator(createCatalogReader());
        try {
            validatedSqlNode = validator.validate(sqlNode);
        } catch (RuntimeException e) {
            throw new ValidationException(e);
        }
        return validatedSqlNode;
    }

    private SqlValidator createSqlValidator(CalciteCatalogReader catalogReader) {
        final SqlOperatorTable opTab =
                SqlOperatorTables.chain(operatorTable, catalogReader);
        return new RawSqlValidator(opTab,
                catalogReader,
                getTypeFactory(),
                sqlValidatorConfig
                        .withDefaultNullCollation(connectionConfig.defaultNullCollation())
                        .withLenientOperatorLookup(connectionConfig.lenientOperatorLookup())
                        .withSqlConformance(connectionConfig.conformance())
                        .withIdentifierExpansion(true));
    }

    private CalciteCatalogReader createCatalogReader() {
        SchemaPlus defaultSchema = requireNonNull(this.defaultSchema, "defaultSchema");
        final SchemaPlus rootSchema = rootSchema(defaultSchema);

        return new CalciteCatalogReader(
                CalciteSchema.from(rootSchema),
                CalciteSchema.from(defaultSchema).path(null),
                getTypeFactory(), connectionConfig);
    }

    private static SchemaPlus rootSchema(SchemaPlus schema) {
        for (;;) {
            SchemaPlus parentSchema = schema.getParentSchema();
            if (parentSchema == null) {
                return schema;
            }
            schema = parentSchema;
        }
    }

    public JavaTypeFactory getTypeFactory() {
        return requireNonNull(typeFactory, "typeFactory");
    }

    public RelRoot rel(SqlNode sql) {
        SqlNode validatedSqlNode = requireNonNull(this.validatedSqlNode,
                "validatedSqlNode is null. Need to call #validate() first");
        final RexBuilder rexBuilder = createRexBuilder();
        final RelOptCluster cluster = RelOptCluster.create(
                requireNonNull(planner, "planner"),
                rexBuilder);
        final SqlToRelConverter.Config config =
                sqlToRelConverterConfig.withTrimUnusedFields(false);
        final SqlToRelConverter sqlToRelConverter =
                new SqlToRelConverter(this, validator,
                        createCatalogReader(), cluster, convertletTable, config);
        return sqlToRelConverter.convertQuery(validatedSqlNode, false, true);
    }

    private RexBuilder createRexBuilder() {
        return new RexBuilder(getTypeFactory());
    }

    @Override
    public RelRoot expandView(RelDataType rowType, String queryString, List<String> schemaPath, @Nullable List<String> viewPath) {
        RelOptPlanner planner = this.planner;
        if (planner == null) {
            ready();
            planner = requireNonNull(this.planner, "planner");
        }
        SqlParser parser = SqlParser.create(queryString, parserConfig);
        SqlNode sqlNode;
        try {
            sqlNode = parser.parseQuery();
        } catch (SqlParseException e) {
            throw new RuntimeException("parse failed", e);
        }

        final CalciteCatalogReader catalogReader =
                createCatalogReader().withSchemaPath(schemaPath);
        final SqlValidator validator = createSqlValidator(catalogReader);

        final RexBuilder rexBuilder = createRexBuilder();
        final RelOptCluster cluster = RelOptCluster.create(planner, rexBuilder);
        final SqlToRelConverter.Config config =
                sqlToRelConverterConfig.withTrimUnusedFields(false);
        final SqlToRelConverter sqlToRelConverter =
                new SqlToRelConverter(this, validator,
                        catalogReader, cluster, convertletTable, config);

        final RelRoot root =
                sqlToRelConverter.convertQuery(sqlNode, true, false);
        final RelRoot root2 =
                root.withRel(sqlToRelConverter.flattenTypes(root.rel, true));
        final RelBuilder relBuilder =
                config.getRelBuilderFactory().create(cluster, null);
        return root2.withRel(
                RelDecorrelator.decorrelateQuery(root.rel, relBuilder));
    }
}

class RawSqlValidator extends SqlValidatorImpl {

    RawSqlValidator(SqlOperatorTable opTab,
                        CalciteCatalogReader catalogReader, JavaTypeFactory typeFactory,
                        Config config) {
        super(opTab, catalogReader, typeFactory, config);
    }

    @Override protected RelDataType getLogicalSourceRowType(
            RelDataType sourceRowType, SqlInsert insert) {
        final RelDataType superType =
                super.getLogicalSourceRowType(sourceRowType, insert);
        return ((JavaTypeFactory) typeFactory).toSql(superType);
    }

    @Override protected RelDataType getLogicalTargetRowType(
            RelDataType targetRowType, SqlInsert insert) {
        final RelDataType superType =
                super.getLogicalTargetRowType(targetRowType, insert);
        return ((JavaTypeFactory) typeFactory).toSql(superType);
    }
}