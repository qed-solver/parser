package org.cosette;

import org.apache.calcite.config.CalciteConnectionProperty;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.server.ServerDdlExecutor;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.parser.ddl.SqlDdlParserImpl;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

/**
 * A SchemaGenerator instance can execute DDL statements and generate schemas in the process.
 */
public class SchemaGenerator {

    private final SqlParser.Config parserConfig;
    private final CalciteConnection calciteConnection;

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
