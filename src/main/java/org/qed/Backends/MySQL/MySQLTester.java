package org.qed.Backends.MySQL;

import org.qed.*;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;

import kala.collection.Seq;

public class MySQLTester {

    public static String genPath = "src/main/java/org/qed/Backends/MySQL/Generated";
    public static String testPath = "src/main/java/org/qed/Backends/MySQL/Tests";
    
    // MySQL connection settings
    private static final String DB_URL = "jdbc:mysql://localhost:3306/test_db";
    private static final String USER = "root";
    private static final String PASSWORD = "xys6279462"; 
    
    private Connection connection;
    private MySQLGenerator generator;
    
    public MySQLTester() {
        this.generator = new MySQLGenerator();
    }

    public void connect() throws SQLException {
        connection = DriverManager.getConnection(DB_URL, USER, PASSWORD);
        System.out.println("+ Connected to MySQL database");
    }
    
    public void disconnect() throws SQLException {
        if (connection != null && !connection.isClosed()) {
            connection.close();
            System.out.println("- Disconnected from MySQL");
        }
    }

    public static Seq<RRule> ruleList() {
        java.io.File ruleDir = new java.io.File("src/main/java/org/qed/RRuleInstances");
        java.io.File[] files = ruleDir.listFiles((dir, name) -> name.endsWith(".java"));
        java.util.List<RRule> rules = new java.util.ArrayList<>();
        
        if (files != null) {
            for (java.io.File file : files) {
                String className = file.getName().replace(".java", "");
                if (!className.equals("FilterMerge")) {
                    continue;
                }
                try {
                    Class<?> clazz = Class.forName("org.qed.RRuleInstances." + className);
                    RRule rule = (RRule) clazz.getConstructor().newInstance();
                    rules.add(rule);
                } catch (Exception e) {
                    throw new RuntimeException("Failed to load rule: " + className, e);
                }
            }
        }
        return Seq.from(rules);
    }

    public void generate() {
        var rules = ruleList();
        
        try {
            Files.createDirectories(Path.of(genPath));
            for (var rule : rules) {
                serialize(rule, genPath);
            }
        } catch (IOException e) {
            System.err.println("Error creating directories: " + e.getMessage());
        }
    }
    
    public void serialize(RRule rule, String path) {
        var sql = generator.generate(rule);
        try {
            Path filePath = Path.of(path, rule.name() + ".sql");
            Files.write(filePath, sql.getBytes());
        } catch (IOException ioe) {
            System.err.println(ioe.getMessage());
        }
    }
    
    public void loadRule(String ruleName) throws SQLException {
        Path rulePath = Path.of(genPath, ruleName + ".sql");
        try {
            String sql = Files.readString(rulePath);
            Statement stmt = connection.createStatement();
            
            String[] statements = sql.split(";");
            for (String statement : statements) {
                String trimmed = statement.trim();
                if (!trimmed.isEmpty()) {
                    stmt.execute(trimmed);
                }
            }
        } catch (IOException ioe) {
            System.err.println(ioe.getMessage());
        }
    }
    
    public void testRule(String ruleName, String source, String target) throws SQLException {
        System.out.println("Testing rule: " + ruleName);
        cleanupRules();
        loadRule(ruleName);
        String answer = executeSQL(source);
        if (answer.trim().equals(target.trim())) 
        {
            System.out.println("succeeded");
        } 
        else 
        {
            System.out.println("failed");
            System.out.println("> Source sql:\n" + source);
            System.out.println("> Actual sql:\n" + answer);
            System.out.println("> Expected sql:\n" + target);
        }
    }

    private String executeSQL(String source) throws SQLException {
        Statement stmt = connection.createStatement();

        // Use a timestamp to filter queries instead of deleting
        stmt.execute("SET GLOBAL general_log = 1");
        stmt.execute("SET GLOBAL log_output = 'TABLE'");
        
        // Get current timestamp as a baseline
        ResultSet timeResult = stmt.executeQuery("SELECT NOW(6)");
        timeResult.next();
        String startTime = timeResult.getString(1);
        timeResult.close();
        
        // Get connection ID
        ResultSet connId = stmt.executeQuery("SELECT CONNECTION_ID()");
        connId.next();
        long connectionId = connId.getLong(1);
        connId.close();
        
        try {
            // Execute the source query
            ResultSet rs = stmt.executeQuery(source);
            rs.close();
        } catch (SQLException e) {
            // Query might fail, but we still want to see what was attempted
        }
        
        // Get queries executed after our start time by our connection
        ResultSet result = stmt.executeQuery(
            "SELECT argument FROM mysql.general_log " +
            "WHERE thread_id = " + connectionId + " " +
            "AND event_time > '" + startTime + "' " +
            "AND command_type = 'Query' " +
            "AND argument LIKE 'SELECT%' " +
            "AND argument NOT LIKE '%mysql.general_log%' " +
            "AND argument NOT LIKE '%NOW(%' " +
            "ORDER BY event_time DESC LIMIT 1"
        );
        
        String executedQuery = source;
        if (result.next()) {
            executedQuery = result.getString("argument");
        }
        result.close();
        
        return executedQuery;
    }
    public void runAllTests() throws SQLException {
        connect();
        Statement stmt = connection.createStatement();
        stmt.execute("SET GLOBAL rewriter_enabled = ON");
        java.io.File testDir = new java.io.File("src/main/java/org/qed/Backends/MySQL/Tests");
        java.io.File[] testFiles = testDir.listFiles((dir, name) -> name.endsWith("Test.java"));
        for (java.io.File testFile : testFiles) {
            String className = "org.qed.Backends.MySQL.Tests." + testFile.getName().replace(".java", "");
            try {
                Class<?> testClass = Class.forName(className);
                testClass.getMethod("runTest").invoke(null);
            } catch (Exception e) {
                System.err.println(className + " failed: " + e.getMessage());
            }
        }
        // System.out.println("haha1");
        cleanupRules();
        // System.out.println("haha2");
        disconnect();
    }

    private void cleanupRules() throws SQLException {
        // System.out.println("test1");
        Statement stmt = connection.createStatement();
        // System.out.println("test2");
        stmt.execute("DELETE FROM query_rewrite.rewrite_rules");
        // System.out.println("test3");
        stmt.execute("CALL query_rewrite.flush_rewrite_rules()");
    }

    public void cleanup() throws SQLException {
        connect();
        cleanupRules();
        disconnect();
    }
    
    public static void main(String[] args) {
        try {
            Class.forName("com.mysql.cj.jdbc.Driver");
        } catch (ClassNotFoundException e) {
            throw new RuntimeException("MySQL Driver not found!", e);
        }
        MySQLTester tester = new MySQLTester();
        // tester.generate();
        try {
            tester.runAllTests();
        } catch (SQLException e) {
            System.err.println(e.getMessage());
        }
        try {
            tester.cleanup();
        } catch (SQLException e) {
            System.err.println("Cleanup failed: " + e.getMessage());
        }
    }
}