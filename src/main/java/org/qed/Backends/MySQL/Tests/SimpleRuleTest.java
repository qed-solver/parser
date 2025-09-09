package org.qed.Backends.MySQL.Tests;

import org.qed.Backends.MySQL.MySQLTester;
import java.sql.SQLException;

public class SimpleRuleTest {
    
    public static void runTest() throws SQLException {
        MySQLTester tester = new MySQLTester();
        tester.connect();
        
        String before = "SELECT 5";
        String after = "SELECT 5 + 1";
        
        tester.testRule("SimpleRule", before, after);
        tester.disconnect();
    }
    
    public static void main(String[] args) throws SQLException {
        runTest();
    }
}