package org.cosette;

import org.apache.calcite.config.CalciteConnectionProperty;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.server.ServerDdlExecutor;
import org.apache.calcite.sql.*;
//import org.apache.calcite.sql.ddl.SqlCreateTable;
//import org.apache.calcite.sql.ddl.SqlKeyConstraint;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.parser.ddl.SqlDdlParserImpl;
//import org.apache.calcite.sql.util.SqlShuttle;
//import org.checkerframework.checker.nullness.qual.Nullable;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.*;
//import java.util.stream.Collectors;

/**
 * A SchemaGenerator instance can execute DDL statements and generate schemas in the process.
 */
public class SchemaGenerator {

    private final SqlParser.Config parserConfig;
    private final CalciteConnection calciteConnection;
//    private final Map<String, Map<SqlKind, List<String>>> constraints;

    /**
     * Create a SchemaGenerator instance by setting up a connection to JDBC.
     */
    public SchemaGenerator() throws SQLException {
        Properties info = new Properties();
        info.setProperty(CalciteConnectionProperty.LEX.camelName(), "mysql");
        info.setProperty(CalciteConnectionProperty.FUN.camelName(), "standard");
        info.setProperty(CalciteConnectionProperty.FORCE_DECORRELATE.camelName(), "false");
        info.setProperty(CalciteConnectionProperty.MATERIALIZATIONS_ENABLED.camelName(), "false");
        info.setProperty(CalciteConnectionProperty.QUOTING.camelName(), "back_tick");
        info.setProperty(CalciteConnectionProperty.PARSER_FACTORY.camelName(), ServerDdlExecutor.class.getName() + "#PARSER_FACTORY");
        Connection connection = DriverManager.getConnection("jdbc:calcite:", info);
        calciteConnection = connection.unwrap(CalciteConnection.class);
        parserConfig = SqlParser.Config.DEFAULT.withParserFactory(SqlDdlParserImpl.FACTORY);
//        constraints = new HashMap<>();
    }

    /**
     * Extract constraints from the given DDL statement.
     *
     * @param ddl The given DDL statement.
     * @return The DDL statement without the constraints.
     */
    private String extractConstraints(String ddl) throws SqlParseException {
        SqlParser constraintParser = SqlParser.create(ddl, parserConfig);
        SqlNode statement = constraintParser.parseQuery();
//        ConstraintExtractor constraintExtractor = new ConstraintExtractor();
//        statement.accept(constraintExtractor);
//        constraints.put(constraintExtractor.getName().getSimple(), constraintExtractor.getConstraints());
        return statement.toString();
    }

    /**
     * Execute a DDL statement.
     *
     * @param ddl The given DDL statement.
     */
    public void applyDDL(String ddl) throws SQLException, SqlParseException {
        Statement statement = calciteConnection.createStatement();
        statement.execute(extractConstraints(ddl));
        statement.close();
    }

//    /**
//     * @return The map from table to constraints.
//     */
//    public Map<String, Map<SqlKind, List<String>>> getConstraints() {
//        return constraints;
//    }

    /**
     * @return The current schema.
     */
    public SchemaPlus extractSchema() {
        return calciteConnection.getRootSchema();
    }

    /**
     * @return A RawPlanner instance based on the extracted schema.
     */
    public RawPlanner createPlanner() {
        return new RawPlanner(extractSchema());
    }

    /**
     * Close the connection.
     */
    public void close() throws SQLException {
        calciteConnection.close();
    }

}

// TODO: Resolve constraints.
///**
// * A ConstraintExtractor instance can help to extract and delete the constraint from a CREATE_TABLE statement.
// */
//class ConstraintExtractor extends SqlShuttle {
//
//    private final Map<SqlKind, List<String>> constraints;
//    private SqlIdentifier name;
//
//    /**
//     * Create a ConstraintExtractor instance by initializing the constraint map.
//     */
//    public ConstraintExtractor() {
//        super();
//        name = null;
//        constraints = new HashMap<>();
//        constraints.put(SqlKind.PRIMARY_KEY, new ArrayList<>());
//        constraints.put(SqlKind.FOREIGN_KEY, new ArrayList<>());
//        constraints.put(SqlKind.UNIQUE, new ArrayList<>());
//    }
//
//    /**
//     * @return The name of the table.
//     */
//    public SqlIdentifier getName() {
//        return name;
//    }
//
//    /**
//     * @return The constraints of the table.
//     */
//    public Map<SqlKind, List<String>> getConstraints() {
//        return constraints;
//    }
//
//    /**
//     * Extract table name and constraints from the call, if the call is a instance of corresponding type.
//     *
//     * @param call The input SqlCall.
//     * @return The result of SqlCall.
//     */
//    @Override
//    public @Nullable SqlNode visit(final SqlCall call) {
//        if (call instanceof SqlCreateTable createTable) {
//            name = createTable.name;
//        } else if (call instanceof SqlKeyConstraint constraint) {
//            ArrayList<SqlNode> operands = new ArrayList<>(constraint.getOperandList());
//            operands.removeIf(Objects::isNull);
//            SqlKind sqlKind = constraint.getOperator().getKind();
//            if (!constraints.containsKey(sqlKind)) {
//                constraints.put(sqlKind, new ArrayList<>());
//            }
//            constraints.get(sqlKind).addAll(operands.stream().map(SqlNode::toString).collect(Collectors.toList()));
//        }
//
//        CallCopyingArgHandler argHandler = new CallCopyingArgHandler(call, false);
//        call.getOperator().acceptCall(this, call, false, argHandler);
//        return argHandler.result();
//    }
//
//    /**
//     * Delete all the constraints from the nodeList.
//     *
//     * @param nodeList The input nodeList.
//     * @return The modified nodeList.
//     */
//
//    @Override
//    public @Nullable SqlNode visit(SqlNodeList nodeList) {
//        SqlNode operand;
//        final List<SqlNode> removed = new ArrayList<>();
//        for (int index = 0; index < nodeList.size(); index++) {
//            operand = nodeList.get(index);
//            if (operand != null) {
//                nodeList.set(index, operand.accept(this));
//                if (operand instanceof SqlKeyConstraint) {
//                    removed.add(operand);
//                }
//            }
//        }
//        nodeList.removeAll(removed);
//        return nodeList;
//    }
//}
