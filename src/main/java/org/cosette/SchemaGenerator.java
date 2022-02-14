package org.cosette;

import com.mysql.cj.jdbc.MysqlDataSource;
import org.apache.calcite.DataContext;
import org.apache.calcite.adapter.jdbc.JdbcConvention;
import org.apache.calcite.adapter.jdbc.JdbcSchema;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.dialect.MysqlSqlDialect;
import org.apache.calcite.util.BuiltInMethod;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * A SchemaGenerator instance can execute DDL statements and generate schemas in the process.
 */
public class SchemaGenerator {

    private final MysqlDataSource dataSource = new MysqlDataSource();

    /**
     * Create a SchemaGenerator instance by setting up a connection to JDBC.
     */
    public SchemaGenerator() throws SQLException {
        dataSource.setUser("cosette");
        dataSource.setPassword("cosette");
        dataSource.setUrl("jdbc:mysql://localhost/cosette");
        ResultSet rs = dataSource.getConnection().createStatement().executeQuery("SELECT CONCAT('DROP TABLE IF EXISTS `', table_name, '`')\n" +
                "FROM information_schema.tables\n" +
                "WHERE table_schema = 'cosette'");
        while (rs.next()) {
            dataSource.getConnection().createStatement().executeUpdate(rs.getString(1));
        }
    }

    /**
     * Execute a DDL statement.
     *
     * @param ddl The given DDL statement.
     */
    public void applyDDL(String ddl) throws SQLException {
        Statement statement = dataSource.getConnection().createStatement();
        statement.executeUpdate(ddl);
        statement.close();
    }

    /**
     * @return The current schema.
     */
    public SchemaPlus extractSchema() {
        JdbcConvention convention = new JdbcConvention(MysqlSqlDialect.DEFAULT, Expressions.call(DataContext.ROOT, BuiltInMethod.DATA_CONTEXT_GET_ROOT_SCHEMA.method), "cosette-convection");
        Schema schema = new JdbcSchema(dataSource, MysqlSqlDialect.DEFAULT, convention, null, null);
        CalciteSchema calciteSchema = CalciteSchema.createRootSchema(false, true, "calcite-schema", schema);
        return calciteSchema.plus();
    }

    /**
     * @return A RawPlanner instance based on the extracted schema.
     */
    public RawPlanner createPlanner() {
        return new RawPlanner(extractSchema());
    }

}

// TODO: Resolve constraints.
