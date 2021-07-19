package org.cosette;

import com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.tools.ValidationException;

import java.sql.SQLException;

public class Main {

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

        String sql3 = """
                SELECT * FROM EMP AS T WHERE EXISTS (SELECT * FROM EMP
                WHERE EXISTS (SELECT * FROM DEPT WHERE DEPT_ID = EMP.DEPT_ID AND DEPT_ID = T.DEPT_ID))
                """;

//        sqlParse.parseDML(sql1);
//        sqlParse.parseDML(sql2);
        sqlParse.parseDML(sql3);

        sqlParse.dumpToJSON();

        sqlParse.done();

    }

}
