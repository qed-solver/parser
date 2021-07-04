package org.cosette;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.calcite.linq4j.Ord;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.prepare.PlannerImpl;
import org.apache.calcite.rel.*;
import org.apache.calcite.rel.core.TableFunctionScan;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.logical.*;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.sql.*;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.tools.ValidationException;
import java.sql.*;
import java.util.*;

public class SQLParse {

    public static void main( String[] args ) throws SQLException, SqlParseException, ValidationException, JsonProcessingException {
        SQLParse sqlParse = new SQLParse();
        sqlParse.applyDDL("""
                                  CREATE TABLE EMP (
                                    EMP_ID INTEGER NOT NULL,
                                    EMP_NAME VARCHAR,
                                    DEPT_ID INTEGER
                                 )
                             """);
        sqlParse.applyDDL("""
                                CREATE TABLE DEPT (
                                      DEPT_ID INTEGER,
                                      DEPT_NAME VARCHAR NOT NULL
                                  )
                                """);

        String sql1 = """
                SELECT * FROM
                (SELECT * FROM EMP WHERE DEPT_ID = 10) AS T
                WHERE T.DEPT_ID + 5 > T.EMP_ID
                    """;

        String sql2 = """
                SELECT * FROM
                (SELECT * FROM EMP WHERE DEPT_ID = 10) AS T
                WHERE 15 > T.EMP_ID
                """;

        sqlParse.parseDML(sql1);
        sqlParse.parseDML(sql2);

        sqlParse.dumpToJSON();

        sqlParse.done();

    }

    private final SchemaGenerator schemaGenerator;
    private final List<RelRoot> rootList;
    private final Set<RelOptTable> tableSet;

    public SQLParse() throws SQLException {
        schemaGenerator = new SchemaGenerator();
        rootList = new ArrayList<>();
        tableSet = new HashSet<>();
    }

    public void applyDDL(String ddl) throws SQLException {
        schemaGenerator.applyDDL(ddl);
    }

    public void parseDML(String dml) throws SqlParseException, ValidationException {
        PlannerImpl planner = schemaGenerator.createPlanner();
        SqlNode sqlNode = planner.parse(dml);
        planner.validate(sqlNode);
        RelRoot relRoot = planner.rel(sqlNode);
        rootList.add(relRoot);
        tableSet.addAll(RelOptUtil.findTables(relRoot.project()));
    }

    public void dumpToJSON() throws JsonProcessingException {
        ObjectMapper mapper = new ObjectMapper();

        ObjectNode mainObject = mapper.createObjectNode();

        ArrayNode schemaArray = mainObject.putArray("schemas");
        List<RelOptTable> tableList = new ArrayList<>(tableSet);
        for (RelOptTable table: tableList) {
            ObjectNode tableObject = mapper.createObjectNode();
            ArrayNode typeArray = tableObject.putArray("types");
            for (RelDataTypeField field: table.getRowType().getFieldList()) {
                typeArray.add(field.getType().toString());
            }
            // TODO: Support referential constraints (e.g. PRIMARY, UNIQUE)

            table.getReferentialConstraints();
            schemaArray.add(tableObject);
        }

        for (RelRoot root: rootList) {
            RelNode relNode = root.project();
            System.out.println(relNode.explain());
        }

        String json = mapper.writerWithDefaultPrettyPrinter().writeValueAsString(mainObject);
        System.out.println(json);

    }

    public void done() throws SQLException {
        schemaGenerator.close();
    }

}

class RelJsonShuttle implements RelShuttle {

    private final ObjectMapper relMapper;

    public RelJsonShuttle(ObjectMapper mapper) {
        relMapper = mapper;
    }

    private RelNode visitChild(RelNode parent, int i, RelNode child) {
        RelNode child2 = child.accept(this);
        if (child2 != child) {
            final List<RelNode> newInputs = new ArrayList<>(parent.getInputs());
            newInputs.set(i, child2);
            return parent.copy(parent.getTraitSet(), newInputs);
        }
        return parent;
    }

    private RelNode visitChildren(RelNode rel) {
        for (Ord<RelNode> input : Ord.zip(rel.getInputs())) {
            rel = visitChild(rel, input.i, input.e);
        }
        return rel;
    }

    @Override public RelNode visit(LogicalAggregate aggregate) {
        return visitChild(aggregate, 0, aggregate.getInput());
    }

    @Override public RelNode visit(LogicalMatch match) {
        return visitChild(match, 0, match.getInput());
    }

    @Override public RelNode visit(TableScan scan) {
        return scan;
    }

    @Override public RelNode visit(TableFunctionScan scan) {
        return visitChildren(scan);
    }

    @Override public RelNode visit(LogicalValues values) {
        return values;
    }

    @Override public RelNode visit(LogicalFilter filter) {
        return visitChild(filter, 0, filter.getInput());
    }

    @Override public RelNode visit(LogicalCalc calc) {
        return visitChildren(calc);
    }

    @Override public RelNode visit(LogicalProject project) {
        return visitChild(project, 0, project.getInput());
    }

    @Override public RelNode visit(LogicalJoin join) {
        return visitChildren(join);
    }

    @Override public RelNode visit(LogicalCorrelate correlate) {
        return visitChildren(correlate);
    }

    @Override public RelNode visit(LogicalUnion union) {
        return visitChildren(union);
    }

    @Override public RelNode visit(LogicalIntersect intersect) {
        return visitChildren(intersect);
    }

    @Override public RelNode visit(LogicalMinus minus) {
        return visitChildren(minus);
    }

    @Override public RelNode visit(LogicalSort sort) {
        return visitChildren(sort);
    }

    @Override public RelNode visit(LogicalExchange exchange) {
        return visitChildren(exchange);
    }

    @Override public RelNode visit(LogicalTableModify modify) {
        return visitChildren(modify);
    }

    @Override public RelNode visit(RelNode other) {
        return visitChildren(other);
    }

}