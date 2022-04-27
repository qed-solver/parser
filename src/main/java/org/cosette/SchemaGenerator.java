package org.cosette;

import com.google.common.collect.ImmutableMap;
import org.apache.calcite.config.Lex;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.schema.*;
import org.apache.calcite.schema.impl.AbstractSchema;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.schema.impl.ScalarFunctionImpl;
import org.apache.calcite.sql.*;
import org.apache.calcite.sql.ddl.SqlCheckConstraint;
import org.apache.calcite.sql.ddl.SqlColumnDeclaration;
import org.apache.calcite.sql.ddl.SqlCreateTable;
import org.apache.calcite.sql.ddl.SqlKeyConstraint;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.parser.ddl.SqlDdlParserImpl;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.ImmutableBitSet;

import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * A SchemaGenerator instance can execute DDL statements and generate schemas in the process.
 */
public class SchemaGenerator {

    private static final Map<String, Class<?>> toPrimitive = ImmutableMap.<String, Class<?>>builder()
            .put("BINARY", String.class)
            .put("CHAR", String.class)
            .put("VARBINARY", String.class)
            .put("VARCHAR", String.class)
            .put("BLOB", String.class)
            .put("TINYBLOB", String.class)
            .put("MEDIUMBLOB", String.class)
            .put("LONGBLOB", String.class)
            .put("TEXT", String.class)
            .put("TINYTEXT", String.class)
            .put("MEDIUMTEXT", String.class)
            .put("LONGTEXT", String.class)
            .put("ENUM", String.class)
            .put("SET", String.class)
            .put("BOOL", boolean.class)
            .put("BOOLEAN", boolean.class)
            .put("DEC", double.class)
            .put("DECIMAL", double.class)
            .put("DOUBLE", double.class)
            .put("DOUBLE PRECISION", double.class)
            .put("FLOAT", float.class)
            .put("DATE", int.class)
            .put("DATETIME", int.class)
            .put("TIMESTAMP", int.class)
            .put("TIME", int.class)
            .put("YEAR", int.class)
            .put("INT", int.class)
            .put("TINYINT", int.class)
            .put("SMALLINT", int.class)
            .put("MEDIUMINT", int.class)
            .put("BIGINT", int.class)
            .put("INTEGER", int.class)
            .build();
    private static final Pattern scalarFunctionPattern = Pattern.compile("(?i)DECLARE\\s+FUNCTION\\s+(?<identifier>\\w+)\\s*\\((?<source>.*)\\)\\s+RETURNS\\s+(?<target>.+)");
    private static final SqlParser.Config schemaParserConfig = SqlParser.Config.DEFAULT
            .withParserFactory(SqlDdlParserImpl.FACTORY)
            .withLex(Lex.MYSQL);
    private final CosetteSchema schema;
    private final Map<String, ScalarFunction> declaredFunctions = new HashMap<>();

    /**
     * Create a SchemaGenerator instance by setting up a connection to JDBC.
     */
    public SchemaGenerator() {
        schema = new CosetteSchema(declaredFunctions);
    }

    /**
     * Execute a CREATE TABLE statement.
     *
     * @param createTable The given CREATE TABLE statement.
     */
    public void applyCreateTable(String createTable) throws Exception {
        SqlParser schemaParser = SqlParser.create(createTable, schemaParserConfig);
        SqlNode schemaNode = schemaParser.parseStmt();
        schema.addTable((SqlCreateTable) schemaNode);
    }

    /**
     * Execute a DECLARE FUNCTION statement.
     *
     * @param declareFunction The given DECLARE FUNCTION statement.
     */
    public void applyDeclareFunction(String declareFunction) throws Exception {
        Matcher matcher = scalarFunctionPattern.matcher(declareFunction);
        if (!matcher.find()) {
            throw new RuntimeException("Broken function declaration:\n" + declareFunction);
        }
        String identifier = matcher.group("identifier");
        String[] source = matcher.group("source").split(",");
        String target = matcher.group("target").split("\\(")[0].trim().toUpperCase();
        Class<?>[] parameters = new Class[source.length];
        if (!toPrimitive.containsKey(target)) {
            throw new RuntimeException("Invalid return type: " + target);
        }
        for (int i = 0; i < source.length; i += 1) {
            String arg = source[i].split("\\(")[0].trim().toUpperCase();
            if (!toPrimitive.containsKey(arg)) {
                throw new RuntimeException("Invalid argument type: " + arg);
            }
            parameters[i] = toPrimitive.get(arg);
        }
        Constructor<Method> methodConstructor = Method.class.getDeclaredConstructor(Class.class, String.class, Class[].class, Class.class, Class[].class, int.class, int.class, String.class, byte[].class, byte[].class, byte[].class);
        methodConstructor.setAccessible(true);
        Method scalarFunction = methodConstructor.newInstance(SchemaGenerator.class, "cosetteFunction", parameters, toPrimitive.get(target), null, 0, 0, "", null, null, null);
        declaredFunctions.put(identifier, ScalarFunctionImpl.createUnsafe(scalarFunction));
    }

    /**
     * @return The current schema.
     */
    public SchemaPlus extractSchema() {
        return schema.plus();
    }

}

class CosetteTable extends AbstractTable {

    final CosetteSchema owner;
    final List<Boolean> columnNullabilities = new ArrayList<>();
    final List<String> columnNames = new ArrayList<>();
    final List<SqlTypeName> columnTypeNames = new ArrayList<>();
    final List<SqlBasicCall> checkConstraints = new ArrayList<>();
    final Set<ImmutableBitSet> columnKeys = new HashSet<>();
    final SqlIdentifier id;

    public CosetteTable(CosetteSchema schema, SqlIdentifier name) {
        owner = schema;
        id = name;
    }

    @Override
    public RelDataType getRowType(RelDataTypeFactory typeFactory) {
        List<RelDataType> fields = new ArrayList<>();
        for (int index = 0; index < columnNames.size(); index += 1) {
            fields.add(typeFactory.createTypeWithNullability(typeFactory.createSqlType(columnTypeNames.get(index)), columnNullabilities.get(index)));
        }
        return typeFactory.createStructType(fields, columnNames);
    }

    @Override
    public Statistic getStatistic() {
        return Statistics.of(0, new ArrayList<>(columnKeys));
    }

    public List<RexNode> deriveCheckConstraint() {
        List<RexNode> derivedConstraints = new ArrayList<>();
        RawPlanner planner = new RawPlanner(owner.plus());
        for (SqlBasicCall check : checkConstraints) {
            SqlSelect wrapper = new SqlSelect(SqlParserPos.ZERO, SqlNodeList.EMPTY, SqlNodeList.SINGLETON_STAR,
                    this.id, check, null, null, SqlNodeList.EMPTY, null, null, null, null);
            try {
                planner.parse(wrapper.toString());
                LogicalFilter filter = (LogicalFilter) planner.rel(check).project().getInput(0);
                derivedConstraints.add(filter.getCondition());
            } catch (Exception ignore) {

            }
        }
        return derivedConstraints;
    }

}

class CosetteSchema extends AbstractSchema {

    final HashMap<String, Table> tables = new HashMap<>();
    final Map<String, ScalarFunction> context;

    public CosetteSchema(Map<String, ScalarFunction> declaredFunctions) {
        context = declaredFunctions;
    }

    public void addTable(SqlCreateTable createTable) throws Exception {
        if (createTable.columnList == null) {
            throw new Exception("No column in table " + createTable.name);
        }
        CosetteTable cosetteTable = new CosetteTable(this, createTable.name);

        for (SqlNode column : createTable.columnList) {
            switch (column.getKind()) {
                case CHECK:
                    cosetteTable.checkConstraints.add((SqlBasicCall) ((SqlCheckConstraint) column).getOperandList().get(1));
                    break;
                case COLUMN_DECL:
                    SqlColumnDeclaration decl = (SqlColumnDeclaration) column;
                    cosetteTable.columnNames.add(decl.name.toString());
                    cosetteTable.columnTypeNames.add(SqlTypeName.get(decl.dataType.getTypeName().toString()));
                    cosetteTable.columnNullabilities.add(decl.strategy != ColumnStrategy.NOT_NULLABLE);
                    break;
                case FOREIGN_KEY:
                    System.err.println("Foreign key constraint is not implemented in cosette yet.");
                    break;
                case PRIMARY_KEY:
                case UNIQUE:
                    SqlKeyConstraint cons = (SqlKeyConstraint) column;
                    List<Integer> keys = new ArrayList<>();
                    for (SqlNode id : (SqlNodeList) cons.getOperandList().get(1)) {
                        int index = cosetteTable.columnNames.indexOf(id.toString());
                        keys.add(index);
                        if (column.getKind() == SqlKind.PRIMARY_KEY) {
                            cosetteTable.columnNullabilities.set(index, false);
                        }
                    }
                    cosetteTable.columnKeys.add(ImmutableBitSet.of(keys));
                    break;
                default:
                    throw new Exception("Unsupported declaration type " + column.getKind() + " in table " + createTable.name);
            }
        }
        tables.put(createTable.name.toString(), cosetteTable);
    }

    protected Map<String, Table> getTableMap() {
        return tables;
    }

    public SchemaPlus plus() {
        SchemaPlus raw = CalciteSchema.createRootSchema(true, false, "Cosette", this).plus();
        for (String fn : context.keySet()) {
            raw.add(fn, context.get(fn));
        }
        return raw;
    }

}
