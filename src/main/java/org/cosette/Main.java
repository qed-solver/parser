package org.cosette;

import com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.tools.ValidationException;
import org.apache.commons.io.FilenameUtils;

import java.io.*;
import java.sql.SQLException;
import java.util.Scanner;
import java.util.regex.Pattern;

public class Main {

    public static void main( String[] args ) {
        args = new String[1];
        args[0] = "../examples/example.sql";
        for (String filename: args) {
            parseFile(filename);
        }
    }

    public static void parseFile(String filename) {
        try {
            String type = FilenameUtils.getExtension(filename);
            Scanner scanner = new Scanner(new File(filename));
            if (type.equals("sql")) {
                parseSQLFile(scanner);
            } else if (type.equals("cos")) {
                parseCOSFile(scanner);
            } else {
                System.err.println("Not supported file type: " + type);
            }
        } catch (Exception e) {
            System.err.println("In file: " + filename);
            e.printStackTrace();
            System.exit(-1);
        }
    }

    private static void parseSQLFile(Scanner content) throws ValidationException, JsonProcessingException, SQLException {
        SQLParse sqlParse = new SQLParse();
        content.useDelimiter(Pattern.compile("\n\n"));
        while (content.hasNext()) {
            String statement = content.next();
            try {
                sqlParse.parseDML(statement);
            } catch (SqlParseException e) {
                sqlParse.applyDDL(statement);
            }
        }
        System.out.println(sqlParse.dumpToJSON());
    }

    private static void parseCOSFile(Scanner content) {
    }

}