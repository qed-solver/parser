package org.cosette;

import org.apache.calcite.config.Lex;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.*;
import org.apache.calcite.schema.impl.AbstractSchema;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.ddl.SqlColumnDeclaration;
import org.apache.calcite.sql.ddl.SqlCreateTable;
import org.apache.calcite.sql.ddl.SqlKeyConstraint;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.parser.ddl.SqlDdlParserImpl;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.ImmutableBitSet;

import java.util.*;

/**
 * A SchemaGenerator instance can execute DDL statements and generate schemas in the process.
 */
public class SchemaGenerator {

    private final SqlParser.Config schemaParserConfig = SqlParser.Config.DEFAULT
            .withParserFactory(SqlDdlParserImpl.FACTORY)
            .withLex(Lex.MYSQL);
    private final CosetteSchema schema = new CosetteSchema();

    /**
     * Create a SchemaGenerator instance by setting up a connection to JDBC.
     */
    public SchemaGenerator() {

    }

    /**
     * Execute a DDL statement.
     *
     * @param ddl The given DDL statement.
     */
    public void applyDDL(String ddl) throws Exception {
        SqlParser schemaParser = SqlParser.create(ddl, schemaParserConfig);
        SqlNode schemaNode = schemaParser.parseStmt();
        schema.addTable((SqlCreateTable) schemaNode);
    }

    /**
     * @return The current schema.
     */
    public SchemaPlus extractSchema() {
        return CalciteSchema.createRootSchema(true, false, "Cosette", schema).plus();
    }

    /**
     * @return A RawPlanner instance based on the extracted schema.
     */
    public RawPlanner createPlanner() {
        return new RawPlanner(extractSchema());
    }

}

class CosetteTable extends AbstractTable {

    final List<Boolean> columnNullabilities = new ArrayList<>();
    final List<String> columnNames = new ArrayList<>();
    final List<SqlTypeName> columnTypeNames = new ArrayList<>();
    final Set<ImmutableBitSet> columnKeys = new HashSet<>();

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
}

class CosetteSchema extends AbstractSchema {

    final HashMap<String, Table> tables = new HashMap<>();

    public void addTable(SqlCreateTable createTable) throws Exception {
        if (createTable.columnList == null) {
            throw new Exception("No column in table " + createTable.name);
        }
        CosetteTable cosetteTable = new CosetteTable();
        for (SqlNode column : createTable.columnList) {
            switch (column.getKind()) {
                case CHECK:
                    System.err.println("Check constraint is not implemented in cosette yet.");
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
                    throw new Exception("Unsupported declaration type" + column.getKind() + " in table " + createTable.name);
            }
        }
        tables.put(createTable.name.toString(), cosetteTable);
    }

    protected Map<String, Table> getTableMap() {
        return tables;
    }

}
